//! A key-value map based on skip table. It's designed to be used in concurrent mode and performance
//! takes high priority. It has narrower APIs comapred to [super::ordinary].
//!
//! EBR (epoch based reclamation) approach is used to safely free objects.
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::skiptable::map::lockfree::SkipTableMap;
//! use rand::Rng;
//! use rand::rngs::SmallRng;
//!
//! fn main() {
//!     let order = 12usize;
//!     let table = SkipTableMap::<u64, u64>::new(order, None);
//!     let mut rng: SmallRng = rand::make_rng();
//!
//!     let max = 2usize.pow(order as u32);
//!     let mut vec = Vec::<u64>::with_capacity(max);
//!     while vec.len() < max {
//!         vec.push(rng.next_u64());
//!     }
//!
//!     let thr_num = 4usize;
//!     let op_cnt = max;
//!     let mask = max - 1;
//!     let vec_ref = &vec;
//!     let table_ref = &table;
//!
//!     std::thread::scope(|s| {
//!         for _ in 0..thr_num {
//!             s.spawn(move || {
//!                 let mut rng: SmallRng = rand::make_rng();
//!                 for op_idx in 0..op_cnt {
//!                     let rnd = rng.next_u32();
//!                     let mut idx = rnd as usize & mask;
//!                     let op = rnd & 0x7;
//!                     match op {
//!                         x if x >= 4 => {
//!                             // inserting
//!                             if x == 4 {
//!                                 idx = op_idx as usize & mask;
//!                             }
//!                             let key = vec_ref[idx];
//!                             let _ = table_ref.insert(key, idx as u64);
//!                         }
//!                         0 => {
//!                             // get. key may not in map
//!                             let key = vec_ref[idx];
//!                             if let Ok(Some(entry)) = table_ref.get(&key) {
//!                                 let v = entry.value();
//!                                 let idx_u64 = idx as u64;
//!                                 assert!(*v == idx_u64 || *v == idx_u64 >> 1)
//!                             }
//!                         }
//!                         1 => {
//!                             // get_mut. key may not in map
//!                             let key = vec_ref[idx];
//!                             if let Ok(Some(mut entry)) = table_ref.get_mut(&key) {
//!                                 let v = entry.value_mut().unwrap();
//!                                 let idx_u64 = idx as u64;
//!                                 assert!(*v == idx_u64 || *v == idx_u64 >> 1);
//!                                 *v = idx_u64 >> 1;
//!                             }
//!                         }
//!                         2 => {
//!                             // remove. key may not in map
//!                             let key = vec_ref[idx];
//!                             let idx_u64 = idx as u64;
//!                             if let Ok(Some(res)) = table_ref.remove(&key) {
//!                                 assert!(res.0 == key);
//!                                 assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
//!                             }
//!                         }
//!                         3 => {
//!                             // remove_wo_kv. key may not in map
//!                             let key = vec_ref[idx];
//!                             table_ref.remove_wo_kv(&key);
//!                         }
//!                         _ => (),
//!                     }
//!                 }
//!             });
//!         }
//!     });
//! }
//! ```

use super::MAX_HEIGHT;
use crate::sync::spinlock::SpinLock;
use crate::{CacheAligned, Error::*, Result};
use rand::Rng;
use rand::rngs::SmallRng;
use std::alloc::{Layout, alloc, handle_alloc_error};
use std::borrow::Borrow;
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence};

static ID_GEN: AtomicU64 = AtomicU64::new(0);

const FALSE: usize = 0;
const TRUE: usize = 1;
const FLAG_DELETED: u32 = 0x1;
const FLAG_UPDATING_VALUE: u32 = 0x2;
const FLAG_IN_USE: u32 = 0x4;
const FLAG_IN_MUT_USE: u32 = 0x8;
const FLAG_KV_TAKEN: u32 = 0x10;
const FLAG_LINKING: u32 = 0x20;
const EPOCH_ADV_PENDING: usize = 0x10000;
const PTR_DELETE_MARK: usize = 0x01;
const MAX_EPOCH: usize = 3;

#[inline]
fn epoch_by_off(curr: usize, off: usize) -> usize {
    let mut ret = curr + off;
    if ret >= MAX_EPOCH {
        ret -= MAX_EPOCH;
    }
    ret
}

struct TableEpoch {
    lock: SpinLock<()>,
    table_id: u64,
    epoch_set: HashSet<usize>,
}

impl TableEpoch {
    fn register_epoch(&mut self, epoch: usize) {
        let _l = self.lock.lock();
        self.epoch_set.insert(epoch);
    }

    fn unregister_epoch(&mut self, epoch: usize) {
        let _l = self.lock.lock();
        self.epoch_set.remove(&epoch);
    }

    fn usize_to_local_epoch(&self, epoch: usize) -> &LocalEpoch {
        unsafe { &*(epoch as *const LocalEpoch) }
    }
}

struct EpochRegistry {
    lock: SpinLock<()>,
    vec: Vec<TableEpoch>,
}

impl EpochRegistry {
    fn get_table_epoch(&mut self, table_id: u64) -> Option<&mut TableEpoch> {
        let _l = self.lock.lock();
        self.vec.iter_mut().find(|te| te.table_id == table_id)
    }

    fn reg_table_epoch(&mut self, table_id: u64) {
        let te = TableEpoch {
            lock: SpinLock::<()>::new(()),
            table_id,
            epoch_set: HashSet::new(),
        };
        let _l = self.lock.lock();
        self.vec.push(te);
    }

    fn remove_table_epoch(&mut self, table_id: u64) {
        let _l = self.lock.lock();
        if let Some(pos) = self.vec.iter().position(|te| te.table_id == table_id) {
            self.vec.remove(pos);
        }
    }
}

struct EpochRegistryWrapper(UnsafeCell<EpochRegistry>);

unsafe impl Sync for EpochRegistryWrapper {}

impl EpochRegistryWrapper {
    fn get(&self) -> EpochRegistryGuard<'_> {
        EpochRegistryGuard(self)
    }
}

struct EpochRegistryGuard<'a>(&'a EpochRegistryWrapper);

impl<'a> Deref for EpochRegistryGuard<'a> {
    type Target = EpochRegistry;
    fn deref(&self) -> &EpochRegistry {
        unsafe { &*self.0.0.get() }
    }
}

impl<'a> DerefMut for EpochRegistryGuard<'a> {
    fn deref_mut(&mut self) -> &mut EpochRegistry {
        unsafe { &mut *self.0.0.get() }
    }
}

static EPOCH_REGISTRY: LazyLock<EpochRegistryWrapper> = LazyLock::new(|| {
    EpochRegistryWrapper(UnsafeCell::new(EpochRegistry {
        lock: SpinLock::new(()),
        vec: Vec::new(),
    }))
});

fn get_registry() -> EpochRegistryGuard<'static> {
    EPOCH_REGISTRY.get()
}

struct LocalEpoch {
    table_id: u64,
    gepoch: usize,
    active: CacheAligned<AtomicUsize>,
    epoch: CacheAligned<AtomicUsize>,
}

struct LocalEpochPinner<'a>(&'a mut LocalEpoch);

impl<'a> LocalEpochPinner<'a> {
    fn new(le: &'a mut LocalEpoch, gepoch: &AtomicUsize) -> Self {
        le.gepoch = gepoch.load(Ordering::Acquire) & MAX_EPOCH;
        le.epoch.0.store(le.gepoch, Ordering::Release);
        le.active.0.store(TRUE, Ordering::Release);
        le.gepoch = gepoch.load(Ordering::Acquire) & MAX_EPOCH;
        le.epoch.0.store(le.gepoch, Ordering::Release);
        Self(le)
    }

    fn repin(&mut self, gepoch: &AtomicUsize) {
        self.0.gepoch = gepoch.load(Ordering::Acquire) & MAX_EPOCH;
        self.0.epoch.0.store(self.0.gepoch, Ordering::Release);
    }
}

impl<'a> Drop for LocalEpochPinner<'a> {
    fn drop(&mut self) {
        self.0.active.0.store(FALSE, Ordering::Release);
    }
}

struct LocalEpochVec {
    vec: Vec<LocalEpoch>,
}

impl LocalEpochVec {
    fn get_epoch(&mut self, table_id: u64) -> &mut LocalEpoch {
        if let Some(pos) = self.vec.iter().position(|e| e.table_id == table_id) {
            &mut self.vec[pos]
        } else {
            let epoch = LocalEpoch {
                table_id,
                gepoch: 0,
                active: CacheAligned(AtomicUsize::new(FALSE)),
                epoch: CacheAligned(AtomicUsize::new(0)),
            };
            self.vec.push(epoch);
            let ret = self.vec.last_mut().unwrap();
            let mut reg = get_registry();
            let t_epoch = reg.get_table_epoch(table_id).unwrap();
            t_epoch.register_epoch(ret as *const LocalEpoch as usize);
            ret
        }
    }

    #[cfg(test)]
    fn remove_epoch(&mut self, table_id: u64) {
        let res = self.vec.iter().position(|e| e.table_id == table_id);
        if let Some(pos) = res {
            self.vec.remove(pos);
        }
    }
}

struct LocalEpochGuard {
    epoch_vec: LocalEpochVec,
}

impl Deref for LocalEpochGuard {
    type Target = LocalEpochVec;
    fn deref(&self) -> &LocalEpochVec {
        &self.epoch_vec
    }
}

impl DerefMut for LocalEpochGuard {
    fn deref_mut(&mut self) -> &mut LocalEpochVec {
        &mut self.epoch_vec
    }
}

impl LocalEpochGuard {
    fn new() -> Self {
        let epoch_vec = LocalEpochVec { vec: Vec::new() };
        Self { epoch_vec }
    }
}

impl Drop for LocalEpochGuard {
    fn drop(&mut self) {
        for lepoch in &self.epoch_vec.vec {
            if let Some(gepoch) = get_registry().get_table_epoch(lepoch.table_id) {
                let ptr = lepoch as *const LocalEpoch as usize;
                gepoch.unregister_epoch(ptr);
            }
        }
    }
}

thread_local! {
    static LOCAL_EPOCH: RefCell<LocalEpochGuard> = RefCell::new(LocalEpochGuard::new());
    static SMALL_RNG: RefCell<SmallRng> = RefCell::new(rand::make_rng());
}

type NodePtr = *mut u8;
type NodeAtomicPtr = AtomicPtr<u8>;

#[derive(Debug)]
struct PrecEnt {
    prec: NodePtr,
    next: NodePtr,
    op_ok: bool,
}
struct PrecVec {
    filled: bool,
    ary: [MaybeUninit<PrecEnt>; MAX_HEIGHT],
}
impl PrecVec {
    fn new() -> Self {
        Self {
            filled: false,
            ary: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }

    fn set_ent(&mut self, idx: usize, prec: NodePtr, next: NodePtr) {
        if idx < MAX_HEIGHT {
            let ent = unsafe { self.ary[idx].assume_init_mut() };
            if !self.filled {
                ent.op_ok = false;
            }
            if !self.filled || !ent.op_ok {
                ent.prec = prec;
                ent.next = next;
            }
        }
    }

    fn get_ent(&mut self, idx: usize) -> &mut PrecEnt {
        unsafe { self.ary[idx].assume_init_mut() }
    }

    #[inline]
    fn set_filled(&mut self, filled: bool) {
        self.filled = filled;
    }
}

#[inline]
fn normalize_ptr(ptr: NodePtr) -> NodePtr {
    (ptr as usize & !PTR_DELETE_MARK) as NodePtr
}

/// The option for [SkipTableMap::len].
pub enum LenOption {
    /// Not provided, [SkipTableMap::len] always returns `0`.
    NotProvided,
    /// Maintained by atomic variable. It costs some overheads in inserting and removing entries.
    AtomicVariable,
    // /// Count from the lowest link list
    // CountOnDemand,
}

// pub enum ClearOption {
//     NotProvided,
//     AtomicCount,
// }

/// The optional configuration for the [SkipTableMap].
pub struct TunableConfig {
    /// The threshold to advance the epoch to reclaim the garbage in EBR.
    /// The allowed range is `[256,)`. The default value is `256`.
    pub epoch_adv_thres: usize,
    /// The option for [SkipTableMap::len]. The default value is [LenOption::NotProvided].
    pub len_opt: LenOption,
    // /// The option for [SkipTableMap::clear].
    // pub clear_opt: ClearOption,
}

impl Default for TunableConfig {
    fn default() -> Self {
        Self {
            epoch_adv_thres: 256,
            len_opt: LenOption::NotProvided,
        }
    }
}

impl TunableConfig {
    fn sanitize(&mut self) {
        self.epoch_adv_thres = self.epoch_adv_thres.max(256);
    }
}

/// A key-value map based on skip table.
pub struct SkipTableMap<K, V> {
    id: u64,
    max_height: usize,
    config: TunableConfig,
    epoch: CacheAligned<AtomicUsize>,
    gb_cnt: CacheAligned<AtomicUsize>,
    ent_cnt: CacheAligned<AtomicUsize>,
    next: Vec<CacheAligned<NodeAtomicPtr>>,
    garbage: [CacheAligned<NodeAtomicPtr>; MAX_EPOCH],
    _m: PhantomData<(K, V)>,
}

#[derive(Debug)]
#[repr(C)]
struct Node<K, V> {
    height: usize,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
    flags: AtomicU32,
    refcnt: AtomicU32,
    next: [NodeAtomicPtr],
}

#[derive(Debug)]
#[repr(C)]
struct FixedNode<K, V> {
    height: usize,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
    flags: AtomicU32,
    refcnt: AtomicU32,
}

impl<K, V> FixedNode<K, V> {
    #[inline]
    fn ptr_to_mut<'a>(node_ptr: NodePtr) -> &'a mut Self {
        unsafe { &mut *(node_ptr as *mut Self) }
    }

    #[inline]
    fn fn_get_next_addr(node_ptr: NodePtr, level: usize) -> *mut NodeAtomicPtr {
        let next_ptr = unsafe {
            node_ptr.add(std::mem::size_of::<Self>() + std::mem::size_of::<usize>() * level)
        };
        next_ptr as *mut NodeAtomicPtr
    }

    #[inline]
    fn fn_get_next_atomic<'a>(node_ptr: NodePtr, level: usize) -> &'a NodeAtomicPtr {
        unsafe { &(*(Self::fn_get_next_addr(node_ptr, level) as *const NodeAtomicPtr)) }
    }

    #[inline]
    fn get_next_atomic(&self, node_ptr: NodePtr, level: usize) -> &NodeAtomicPtr {
        Self::fn_get_next_atomic(node_ptr, level)
    }

    #[inline]
    fn get_next_ptr(&self, node_ptr: NodePtr, level: usize) -> NodePtr {
        Self::fn_get_next_ptr(node_ptr, level)
    }

    #[inline]
    fn fn_get_next_ptr(node_ptr: NodePtr, level: usize) -> NodePtr {
        let next_atomic = Self::fn_get_next_atomic(node_ptr, level);
        normalize_ptr(next_atomic.load(Ordering::Acquire))
    }

    #[inline]
    fn set_next_ptr(node_ptr: NodePtr, level: usize, next: NodePtr) {
        let next_atomic = Self::fn_get_next_atomic(node_ptr, level);
        next_atomic.store(next, Ordering::Release);
    }

    #[inline]
    fn take_kv(&mut self) -> (K, V) {
        unsafe {
            (
                ManuallyDrop::<K>::take(&mut self.key),
                ManuallyDrop::<V>::take(&mut self.value),
            )
        }
    }

    #[inline]
    fn unxor_flags(&self, mask: u32) {
        let mut flags = self.flags.load(Ordering::Acquire);
        loop {
            match self.flags.compare_exchange_weak(
                flags,
                flags & !mask,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(f) => flags = f,
            }
        }
    }

    #[inline]
    fn mark_next_ptr_delete(&self, ptr: NodePtr) {
        for level in 0..self.height {
            let next_atomic = self.get_next_atomic(ptr, level);
            let mut next_ptr = next_atomic.load(Ordering::Acquire);
            while (next_ptr as usize) & PTR_DELETE_MARK == 0 {
                let mark_ptr = (next_ptr as usize | PTR_DELETE_MARK) as NodePtr;
                match next_atomic.compare_exchange_weak(
                    next_ptr,
                    mark_ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(ptr) => next_ptr = ptr,
                }
            }
        }
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: usize) -> NodePtr {
        // one plus for next pointer in garbage list
        let slice_layout = Layout::array::<NodeAtomicPtr>(height + 1).unwrap();
        let (node_layout, _) = Layout::new::<FixedNode<K, V>>()
            .extend(slice_layout)
            .unwrap();
        unsafe {
            let raw_ptr = alloc(node_layout);
            if raw_ptr.is_null() {
                handle_alloc_error(node_layout);
            };
            let node_ptr = raw_ptr as *mut FixedNode<K, V>;
            std::ptr::addr_of_mut!((*node_ptr).height).write(height);
            std::ptr::addr_of_mut!((*node_ptr).key).write(ManuallyDrop::new(key));
            std::ptr::addr_of_mut!((*node_ptr).value).write(ManuallyDrop::new(value));
            std::ptr::addr_of_mut!((*node_ptr).flags).write(AtomicU32::new(FLAG_LINKING));
            std::ptr::addr_of_mut!((*node_ptr).refcnt).write(AtomicU32::new(1));
            raw_ptr
        }
    }

    fn from_node_ptr(ptr: NodePtr) -> Box<Self> {
        // one plus for next pointer in garbage list
        let height = unsafe { *(ptr as *const usize) } + 1;
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ptr, height) as *mut Node<K, V>;
        unsafe { Box::from_raw(fat_ptr) }
    }
}

enum EntryUsage {
    Gc,
    UserUse,
    UserMutUse,
}

/// A reference to an entry in the key-value map.
pub struct EntryRef<K, V> {
    ptr: NodePtr,
    usage: EntryUsage,
    _m: PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for EntryRef<K, V> {}

impl<K, V> EntryRef<K, V> {
    fn from_ptr(ptr: NodePtr, bmut: bool) -> Self {
        let node = unsafe { &*(ptr as *const FixedNode<K, V>) };
        node.refcnt.fetch_add(1, Ordering::Release);
        Self {
            ptr,
            usage: if bmut {
                EntryUsage::UserMutUse
            } else {
                EntryUsage::UserUse
            },
            _m: PhantomData,
        }
    }

    fn take_from_ptr(ptr: NodePtr) -> Self {
        Self {
            ptr,
            usage: EntryUsage::Gc,
            _m: PhantomData,
        }
    }

    #[inline]
    fn as_ref(&self) -> &FixedNode<K, V> {
        unsafe { &*(self.ptr as *const FixedNode<K, V>) }
    }

    #[inline]
    fn as_mut(&mut self) -> &mut FixedNode<K, V> {
        unsafe { &mut *(self.ptr as *mut FixedNode<K, V>) }
    }

    /// Gets a reference to the key in the entry.
    pub fn key(&self) -> &K {
        &self.as_ref().key
    }

    /// Gets a reference to the value in the entry.
    pub fn value(&self) -> &V {
        &self.as_ref().value
    }

    /// Gets a mutable reference to the value in the entry. If this [EntryRef] is retrieved by
    /// [SkipTableMap::get] rather than [SkipTableMap::get_mut], an [ElementRefNotMutable] error
    /// is returned.
    pub fn value_mut(&mut self) -> Result<&mut V> {
        let node = self.as_mut();
        let flags = node.flags.load(Ordering::Acquire);
        if flags & FLAG_IN_MUT_USE == 0 {
            return Err(ElementRefNotMutable);
        }
        Ok(&mut node.value)
    }
}

impl<K, V> Drop for EntryRef<K, V> {
    fn drop(&mut self) {
        let node = unsafe { &*(self.ptr as *const FixedNode<K, V>) };
        let mut flags = node.flags.load(Ordering::Acquire);
        // If the entry is deleted, the IN_USE/IN_MUT_USE flags are no needed to be maintained
        while flags & FLAG_DELETED == 0 {
            let mut nflags = flags;
            match self.usage {
                EntryUsage::UserMutUse => nflags = flags & !FLAG_IN_MUT_USE,
                EntryUsage::UserUse => {
                    if node.refcnt.load(Ordering::Acquire) <= 2 {
                        nflags = flags & !FLAG_IN_USE;
                    }
                }
                _ => (),
            }
            if nflags != flags {
                match node.flags.compare_exchange_weak(
                    flags,
                    nflags,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(f) => flags = f,
                }
            } else {
                break;
            }
        }

        if node.refcnt.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            let mut node_box = Node::<K, V>::from_node_ptr(self.ptr);
            if flags & FLAG_KV_TAKEN == 0 {
                unsafe {
                    ManuallyDrop::drop(&mut node_box.key);
                    ManuallyDrop::drop(&mut node_box.value);
                }
            }
            drop(node_box);
        }
    }
}

impl<K, V> Drop for SkipTableMap<K, V> {
    fn drop(&mut self) {
        get_registry().remove_table_epoch(self.id);
        for epoch in 0..self.garbage.len() {
            self.clear_garbage(epoch);
        }
        let mut next = self.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let _entry = EntryRef::<K, V>::take_from_ptr(next);
            next = FixedNode::<K, V>::fn_get_next_ptr(next, 0);
        }
    }
}

impl<K, V> SkipTableMap<K, V> {
    fn put_garbage(&self, epoch: usize, ptr: NodePtr, height: usize) {
        let garb = &self.garbage[epoch];
        let mut head = self.garbage[epoch].load(Ordering::Acquire);
        let next_atomic = FixedNode::<K, V>::fn_get_next_atomic(ptr, height);
        next_atomic.store(head, Ordering::Release);
        while let Err(h) =
            garb.compare_exchange_weak(head, ptr, Ordering::AcqRel, Ordering::Acquire)
        {
            head = h;
            next_atomic.store(head, Ordering::Release);
        }
        self.gb_cnt.fetch_add(1, Ordering::Release);
    }

    fn clear_garbage(&self, epoch: usize) {
        let null: NodePtr = std::ptr::null_mut();
        let mut next = self.garbage[epoch].swap(null, Ordering::AcqRel);
        let mut cnt = 0;
        while !next.is_null() {
            cnt += 1;
            let ptr = next;
            let _entry = EntryRef::<K, V>::take_from_ptr(ptr);
            let node = FixedNode::<K, V>::ptr_to_mut(ptr);
            next = node.get_next_ptr(ptr, node.height);
        }
        self.gb_cnt.fetch_sub(cnt, Ordering::Release);
    }

    fn adv_epoch(&self, epoch: usize) {
        if self.gb_cnt.load(Ordering::Acquire) > self.config.epoch_adv_thres {
            // 1. Advance to the new epoch
            let nepoch = epoch_by_off(epoch, 1);
            if self
                .epoch
                .compare_exchange_weak(
                    epoch,
                    nepoch | EPOCH_ADV_PENDING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // 2. Wait current(old) epoch exit
                let mut reg = get_registry();
                let gepoch = reg.get_table_epoch(self.id).unwrap();
                loop {
                    let mut ok = true;
                    let _ge_lock = gepoch.lock.lock();
                    for &ptr in &gepoch.epoch_set {
                        let lepoch = gepoch.usize_to_local_epoch(ptr);
                        if lepoch.active.0.load(Ordering::Acquire) == TRUE
                            && lepoch.epoch.0.load(Ordering::Acquire) == epoch
                        {
                            ok = false;
                            std::hint::spin_loop();
                            break;
                        }
                    }
                    if ok {
                        break;
                    }
                }

                // 3. GC for the previous epoch
                let pepoch = epoch_by_off(epoch, 2);
                self.clear_garbage(pepoch);

                // 4. Unmark pending of epoch advancinig.
                self.epoch.store(nepoch, Ordering::Release);
            }
        }
    }

    /// Makes a new, empty [SkipTableMap].
    ///
    /// Parameters:
    /// - `max_height`: The maximum height allowed. The allowed range is [1, 32].
    /// - `config`: The optional configuration for the map.
    pub fn new(max_height: usize, config: Option<TunableConfig>) -> Self {
        let mut config = config.unwrap_or_default();
        config.sanitize();
        let null: NodePtr = std::ptr::null_mut();
        let max_height = max_height.clamp(1, MAX_HEIGHT);
        let mut next = Vec::new();
        for _ in 0..max_height {
            next.push(CacheAligned(NodeAtomicPtr::new(null)));
        }
        let table_id = ID_GEN.fetch_add(1, Ordering::AcqRel);
        get_registry().reg_table_epoch(table_id);
        Self {
            id: table_id,
            max_height,
            config,
            epoch: CacheAligned(AtomicUsize::new(0)),
            gb_cnt: CacheAligned(AtomicUsize::new(0)),
            ent_cnt: CacheAligned(AtomicUsize::new(0)),
            next,
            garbage: [
                CacheAligned(NodeAtomicPtr::new(null)),
                CacheAligned(NodeAtomicPtr::new(null)),
                CacheAligned(NodeAtomicPtr::new(null)),
            ],
            _m: PhantomData,
        }
    }

    fn gen_height(&self) -> usize {
        let r = SMALL_RNG.with(|rng| rng.borrow_mut().next_u32());
        ((r.trailing_zeros() + 1) as usize).min(self.max_height)
    }

    // Need to be called under epoch protection
    fn search<Q>(&self, key: &Q) -> NodePtr
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: NodePtr = std::ptr::null_mut();
        let mut level = self.max_height - 1;
        let mut next: *mut u8 = null;
        'level_loop: loop {
            let mut prev: NodePtr = null;
            if next.is_null() {
                next = self.next[level].load(Ordering::Acquire);
            }

            'same_level: loop {
                if next.is_null() {
                    if level == 0 {
                        break 'level_loop;
                    }
                    if !prev.is_null() {
                        next = prev;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    let node = FixedNode::<K, V>::ptr_to_mut(next);
                    let k = node.key.deref().borrow();
                    match k.cmp(key) {
                        CmpOrdering::Equal => {
                            return next;
                        }
                        CmpOrdering::Less => {
                            prev = next;
                            next = node.get_next_ptr(next, level);
                            continue 'same_level;
                        }
                        CmpOrdering::Greater => {
                            if level == 0 {
                                break 'level_loop;
                            }
                            next = prev;
                            level -= 1;
                            continue 'level_loop;
                        }
                    }
                }
            }
        }
        null
    }

    // Need to be called under epoch protection
    fn search_ext<Q>(
        &self,
        key: &Q,
        prec_height: usize,
        min_level: usize,
        vec: &mut PrecVec,
    ) -> NodePtr
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: NodePtr = std::ptr::null_mut();
        let mut res: NodePtr = null;
        let mut level = self.max_height - 1;
        let mut next: *mut u8 = null;

        'level_loop: loop {
            let mut prev: NodePtr = null;
            if next.is_null() {
                next = self.next[level].load(Ordering::Acquire);
            }

            'same_level: loop {
                if level < min_level {
                    break 'level_loop;
                }
                if next.is_null() {
                    if level < prec_height {
                        vec.set_ent(level, prev, next);
                    }
                    if level == 0 {
                        break 'level_loop;
                    }
                    if !prev.is_null() {
                        next = prev;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    let node = FixedNode::<K, V>::ptr_to_mut(next);
                    let k = node.key.deref().borrow();
                    match k.cmp(key) {
                        CmpOrdering::Equal => {
                            res = next;
                            if level < prec_height {
                                vec.set_ent(level, prev, next);
                            }
                            if level == 0 {
                                break 'level_loop;
                            } else {
                                next = prev;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                        CmpOrdering::Less => {
                            prev = next;
                            next = node.get_next_ptr(next, level);
                            continue 'same_level;
                        }
                        CmpOrdering::Greater => {
                            if level < prec_height {
                                vec.set_ent(level, prev, next);
                            }
                            if level == 0 {
                                break 'level_loop;
                            }
                            next = prev;
                            level -= 1;
                            continue 'level_loop;
                        }
                    }
                }
            }
        }
        vec.set_filled(true);
        res
    }

    // 1. Need to be called under epoch protection.
    // 2. Need to link level 0 first. Linking other levels rely on its success.
    // 3. If race found (nodes with equal key inserted at same time), the later one should fail.
    //    This is detected on linking its level 0.
    fn link_nodes(
        &self,
        vec: &mut PrecVec,
        ins_node: NodePtr,
        height: usize,
        epoch: &mut LocalEpochPinner,
    ) -> bool
    where
        K: Ord,
    {
        let node = FixedNode::<K, V>::ptr_to_mut(ins_node);
        loop {
            let mut ok = true;
            let mut level = 0;
            let mut min_ng_level = node.height;
            while level < height {
                let ent = vec.get_ent(level);
                if ent.op_ok {
                    level += 1;
                    continue;
                }

                // Use store() instead CAS has no problem since this level of node being insterted
                // is not linked so no others can change its next at this level.
                FixedNode::<K, V>::set_next_ptr(ins_node, level, ent.next);
                let next_atomic = if ent.prec.is_null() {
                    &self.next[level]
                } else {
                    FixedNode::<K, V>::fn_get_next_atomic(ent.prec, level)
                };
                match next_atomic.compare_exchange_weak(
                    ent.next,
                    ins_node,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => ent.op_ok = true,
                    Err(ptr) => {
                        let ptr = normalize_ptr(ptr);
                        let race = FixedNode::<K, V>::ptr_to_mut(ptr);
                        if race.key.eq(&node.key) {
                            return false;
                        } else if race.key.gt(&node.key) {
                            FixedNode::<K, V>::set_next_ptr(ins_node, level, ptr);
                        }
                        ok = false;
                        if min_ng_level > level {
                            min_ng_level = level;
                        }
                        if level == 0 {
                            break;
                        }
                    }
                }
                level += 1;
            }

            if !ok {
                // Found other entry with same key inserted, this linking should fail,
                // since prec_vec filled here is based on the the newly inserted node (who won the race),
                // so there is no chance to do the key equalness check in the above code.
                epoch.repin(&self.epoch);
                let sch_ptr = self.search_ext(&node.key, height, min_ng_level, vec);
                if !sch_ptr.is_null() && sch_ptr != ins_node {
                    return false;
                }
            } else {
                break;
            }
        }
        true
    }

    /// Inserts a key-value pair into the map.
    ///
    /// On success, if the map did not have this key present, None is returned; And if the map did
    /// have this key present, the value is updated, and the old value is returned.
    ///
    /// On failure (i.g. the entry by the key exist in the map and is being used by others via
    /// [Self::get] or [Self::get_mut]), the key and value provided in arguments are returned.
    pub fn insert(&self, key: K, value: V) -> std::result::Result<Option<V>, (K, V)>
    where
        K: Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.insert_inner(key, value, guard.borrow_mut().get_epoch(id)))
    }

    fn insert_inner(
        &self,
        mut key: K,
        mut value: V,
        epoch: &mut LocalEpoch,
    ) -> std::result::Result<Option<V>, (K, V)>
    where
        K: Ord,
    {
        'main_loop: loop {
            let height = self.gen_height();
            let mut vec = PrecVec::new();
            let mut pin = LocalEpochPinner::new(epoch, &self.epoch);
            let res_ptr = self.search_ext(&key, height, 0, &mut vec);
            if !res_ptr.is_null() {
                let node = FixedNode::<K, V>::ptr_to_mut(res_ptr);
                let mut flags = node.flags.load(Ordering::Acquire);
                loop {
                    if flags & FLAG_DELETED != 0 {
                        std::hint::spin_loop();
                        continue 'main_loop;
                    } else if flags & (FLAG_IN_USE | FLAG_IN_MUT_USE) != 0 {
                        return Err((key, value));
                    } else if flags & FLAG_UPDATING_VALUE != 0 {
                        std::hint::spin_loop();
                        flags = node.flags.load(Ordering::Acquire);
                        continue;
                    } else {
                        let nflags = flags | FLAG_UPDATING_VALUE;
                        match node.flags.compare_exchange_weak(
                            flags,
                            nflags,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                std::mem::swap(&mut value, &mut node.value);
                                node.unxor_flags(FLAG_UPDATING_VALUE);
                                return Ok(Some(value));
                            }
                            Err(f) => {
                                flags = f;
                                continue;
                            }
                        }
                    }
                }
            } else {
                let ptr = Node::<K, V>::new(key, value, height);
                let node = FixedNode::<K, V>::ptr_to_mut(ptr);
                if !self.link_nodes(&mut vec, ptr, height, &mut pin) {
                    node.flags.store(FLAG_KV_TAKEN, Ordering::Relaxed);
                    let kv = node.take_kv();
                    key = kv.0;
                    value = kv.1;
                    let _node = Node::<K, V>::from_node_ptr(ptr);
                    continue 'main_loop;
                }
                node.unxor_flags(FLAG_LINKING);
                if let LenOption::AtomicVariable = self.config.len_opt {
                    self.ent_cnt.fetch_add(1, Ordering::Release);
                }
                return Ok(None);
            }
        }
    }

    // 1. Need to be called under epoch protection
    // 2. Need to unlink level 0 after unlinking other levels succeeded.
    fn unlink_nodes(&self, vec: &mut PrecVec, del_node: NodePtr, height: usize)
    where
        K: Ord,
    {
        loop {
            let mut ok = true;
            let mut level = height;
            while level > 0 {
                level -= 1;
                let ent = vec.get_ent(level);
                if !ok && level == 0 {
                    break;
                }
                if ent.next != del_node {
                    ok = false;
                    break;
                }
                if ent.op_ok {
                    continue;
                }
                let new_next = FixedNode::<K, V>::fn_get_next_ptr(del_node, level);
                let next_atomic = if ent.prec.is_null() {
                    &self.next[level]
                } else {
                    if !new_next.is_null() {
                        let pnode = FixedNode::<K, V>::ptr_to_mut(normalize_ptr(ent.prec));
                        let nnode = FixedNode::<K, V>::ptr_to_mut(normalize_ptr(new_next));
                        assert!(pnode.key.le(&nnode.key));
                    }
                    FixedNode::<K, V>::fn_get_next_atomic(ent.prec, level)
                };
                match next_atomic.compare_exchange_weak(
                    ent.next,
                    new_next,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => ent.op_ok = true,
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }

            if !ok {
                let node = FixedNode::<K, V>::ptr_to_mut(del_node);
                self.search_ext(&node.key, height, 0, vec);
            } else {
                break;
            }
        }
    }

    /// Removes a key from the map, returning the key and value at the key if the key was previously
    /// in the map.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others via [SkipTableMap::get] or
    /// [SkipTableMap::get_mut], since ownership of key and value can't be taken in that case.
    /// Use [SkipTableMap::remove_wo_kv] instead to avoid that if you don't need key and value.
    pub fn remove<Q>(&self, key: &Q) -> Result<Option<(K, V)>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.remove_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn remove_inner<Q>(&self, key: &Q, epoch: &mut LocalEpoch) -> Result<Option<(K, V)>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let mut vec = PrecVec::new();
        let pin = LocalEpochPinner::new(epoch, &self.epoch);
        let res_ptr = self.search_ext(key, self.max_height, 0, &mut vec);
        if res_ptr.is_null() {
            Ok(None)
        } else {
            let node = FixedNode::<K, V>::ptr_to_mut(res_ptr);
            let mut flags = node.flags.load(Ordering::Acquire);
            loop {
                if flags & FLAG_DELETED != 0 {
                    return Ok(None);
                } else if flags & (FLAG_IN_USE | FLAG_IN_MUT_USE) != 0 {
                    return Err(ElementInUse);
                } else if flags & (FLAG_UPDATING_VALUE | FLAG_LINKING) != 0 {
                    std::hint::spin_loop();
                    flags = node.flags.load(Ordering::Acquire);
                } else {
                    let nflags = flags | FLAG_DELETED | FLAG_KV_TAKEN;
                    match node.flags.compare_exchange_weak(
                        flags,
                        nflags,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            node.mark_next_ptr_delete(res_ptr);
                            self.unlink_nodes(&mut vec, res_ptr, node.height);
                            let (k, v) = node.take_kv();
                            let gepoch = pin.0.gepoch;
                            self.put_garbage(gepoch, res_ptr, node.height);
                            if let LenOption::AtomicVariable = self.config.len_opt {
                                self.ent_cnt.fetch_sub(1, Ordering::Release);
                            }
                            drop(pin);
                            self.adv_epoch(gepoch);
                            return Ok(Some((k, v)));
                        }
                        Err(f) => flags = f,
                    }
                }
            }
        }
    }

    /// Removes a key from the map if the key was previously in the map, without returning the key
    /// and value at the key. The returned value indicates whether the key was previously in the map.
    pub fn remove_wo_kv<Q>(&self, key: &Q) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.remove_wo_kv_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn remove_wo_kv_inner<Q>(&self, key: &Q, epoch: &mut LocalEpoch) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let mut vec = PrecVec::new();
        let pin = LocalEpochPinner::new(epoch, &self.epoch);
        let res_ptr = self.search_ext(key, self.max_height, 0, &mut vec);
        if res_ptr.is_null() {
            false
        } else {
            let node = FixedNode::<K, V>::ptr_to_mut(res_ptr);
            let mut flags = node.flags.load(Ordering::Acquire);
            loop {
                if flags & FLAG_DELETED != 0 {
                    return false;
                } else if flags & (FLAG_UPDATING_VALUE | FLAG_LINKING) != 0 {
                    std::hint::spin_loop();
                    flags = node.flags.load(Ordering::Acquire);
                } else {
                    let nflags = flags | FLAG_DELETED;
                    match node.flags.compare_exchange_weak(
                        flags,
                        nflags,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            node.mark_next_ptr_delete(res_ptr);
                            self.unlink_nodes(&mut vec, res_ptr, node.height);
                            let gepoch = pin.0.gepoch;
                            self.put_garbage(gepoch, res_ptr, node.height);
                            if let LenOption::AtomicVariable = self.config.len_opt {
                                self.ent_cnt.fetch_sub(1, Ordering::Release);
                            }
                            drop(pin);
                            self.adv_epoch(gepoch);
                            return true;
                        }
                        Err(f) => flags = f,
                    }
                }
            }
        }
    }

    /// Returns a reference to the entry corresponding to the key.
    ///
    /// [ElementInMutUse] is returned if the the entry is being used by others via [SkipTableMap::get_mut].
    pub fn get<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.get_inner(key, false, guard.borrow_mut().get_epoch(id)))
    }

    /// Returns a mutable reference to the entry corresponding to the key.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others via [SkipTableMap::get]
    /// or [SkipTableMap::get_mut].
    pub fn get_mut<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.get_inner(key, true, guard.borrow_mut().get_epoch(id)))
    }

    fn get_inner<Q>(
        &self,
        key: &Q,
        mutable: bool,
        epoch: &mut LocalEpoch,
    ) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        let sch_ptr = self.search(key);
        if sch_ptr.is_null() {
            Ok(None)
        } else {
            let node = FixedNode::<K, V>::ptr_to_mut(sch_ptr);
            let mut flags = node.flags.load(Ordering::Acquire);
            loop {
                if flags & FLAG_DELETED != 0 {
                    return Ok(None);
                } else if flags & FLAG_UPDATING_VALUE != 0 {
                    std::hint::spin_loop();
                    flags = node.flags.load(Ordering::Acquire);
                } else {
                    if flags & (FLAG_IN_MUT_USE | FLAG_IN_USE) == 0 {
                        let nflags = flags
                            | if mutable {
                                FLAG_IN_MUT_USE
                            } else {
                                FLAG_IN_USE
                            };
                        match node.flags.compare_exchange_weak(
                            flags,
                            nflags,
                            Ordering::SeqCst,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                return Ok(Some(EntryRef::<K, V>::from_ptr(sch_ptr, mutable)));
                            }
                            Err(f) => {
                                flags = f;
                                continue;
                            }
                        }
                    } else if mutable {
                        return Err(ElementInUse);
                    } else if !mutable && flags & FLAG_IN_MUT_USE != 0 {
                        return Err(ElementInMutUse);
                    } else {
                        return Ok(Some(EntryRef::<K, V>::from_ptr(sch_ptr, false)));
                    }
                }
            }
        }
    }

    /// Returns the number of elements in the map. NOTE that by default it doesn't work (always returns 0)
    /// unless you configured the table with a [LenOption] rather than [LenOption::NotProvided].
    pub fn len(&self) -> usize {
        match self.config.len_opt {
            LenOption::NotProvided => 0,
            LenOption::AtomicVariable => self.ent_cnt.load(Ordering::Acquire),
        }
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.next[0].load(Ordering::Acquire).is_null()
    }

    // /// Clears the map, removing all elements.
    // pub fn clear(&mut self) {
    //     let id = self.id;
    //     LOCAL_EPOCH.with(|guard| self.clear_inner(guard.borrow_mut().get_epoch(id)))
    // }

    // fn clear_inner(&mut self, epoch: &mut LocalEpoch) {
    //     let null: NodePtr = std::ptr::null_mut();
    //     let mut idx = self.max_height - 1;
    //     while idx > 0 {
    //         self.next[idx].store(null, Ordering::Release);
    //         idx -= 1;
    //     }
    //     let mut next = self.next[0].swap(null, Ordering::AcqRel);
    //     let _pin = LocalEpochPinner::new(epoch, &self.epoch);
    //     while !next.is_null() {
    //         let entry = EntryRef::<K, V>::take_from_ptr(next);
    //         let node = entry.as_ref();
    //         let mut flags = node.flags.load(Ordering::Acquire);
    //         loop {
    //             match node.flags.compare_exchange_weak(
    //                 flags,
    //                 flags | FLAG_DELETED,
    //                 Ordering::AcqRel,
    //                 Ordering::Acquire,
    //             ) {
    //                 Ok(_) => break,
    //                 Err(f) => flags = f,
    //             }
    //         }
    //         next = node.get_next_ptr(next, 0);
    //     }
    // }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.contains_key_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn contains_key_inner<Q>(&self, key: &Q, epoch: &mut LocalEpoch) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        !self.search(key).is_null()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    #[test]
    fn check_nodes1() {
        let order = 4usize;
        let table = SkipTableMap::<u64, u64>::new(order, None);
        let mut rng: SmallRng = rand::make_rng();

        let max = 2usize.pow(order as u32);
        let mut vec = Vec::<u64>::with_capacity(max);
        {
            let mut set = BTreeSet::new();
            while vec.len() < max {
                let num = rng.next_u64();
                if !set.contains(&num) {
                    vec.push(num);
                    set.insert(num);
                }
            }
        }
        println!("test start");

        let start_time = Instant::now();
        for idx in 0..max {
            let num = vec[idx];
            table.insert(num, num + 10).unwrap();
        }
        println!("wduration={:?}", start_time.elapsed());
        //println!("table={:?}", table);
        let mut next = table.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = Node::<u64, u64>::from_node_ptr(next);
            println!(
                "node, ptr={:?}, node=(height={}, key={}, val={}, flags={:?}, refcnt={:?}, next={:?})",
                next, node.height, *node.key, *node.value, node.flags, node.refcnt, &node.next
            );
            next = node.next[0].load(Ordering::Acquire);
            let _ = Box::into_raw(node);
        }
        let start_time = Instant::now();
        for num in &vec {
            let v = table.get(num).unwrap().unwrap();
            assert_eq!(*v.value(), *num + 10);
        }
        println!("rduration={:?}", start_time.elapsed());
    }

    #[test]
    fn check_nodes2() {
        let order = 12usize;
        let table = SkipTableMap::<u64, u64>::new(order, None);
        let mut rng: SmallRng = rand::make_rng();

        let max = 2usize.pow(order as u32);
        let mut vec = Vec::<u64>::with_capacity(max);
        {
            let mut set = BTreeSet::new();
            while vec.len() < max {
                let num = rng.next_u64();
                if !set.contains(&num) {
                    vec.push(num);
                    set.insert(num);
                }
            }
        }
        println!("test start, len={}", vec.len());

        let thr_num = 4usize;
        let count_per_thr = max / thr_num;
        let table_ref = &table;
        let start_time = Instant::now();
        std::thread::scope(|s| {
            for idx in 0..thr_num {
                let start = idx * count_per_thr;
                let end = start + count_per_thr;
                let slice = &vec[start..end];
                s.spawn(move || {
                    for &num in slice {
                        table_ref.insert(num, num + 10).unwrap();
                    }
                });
            }
        });
        println!("wduration={:?}", start_time.elapsed());
        let start_time = Instant::now();
        std::thread::scope(|s| {
            for idx in 0..thr_num {
                let start = idx * count_per_thr;
                let end = start + count_per_thr;
                let slice = &vec[start..end];
                s.spawn(move || {
                    for num in slice {
                        let v = table_ref.get(num).unwrap().unwrap();
                        assert_eq!(*v.value(), *num + 10);
                    }
                });
            }
        });
        println!("rduration={:?}", start_time.elapsed());

        let mut prev = u64::MAX;
        println!("table.next={:?}", &table.next);
        let mut next = table.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = Node::<u64, u64>::from_node_ptr(next);
            let key = *node.key;
            if prev != u64::MAX {
                assert!(prev < key);
            }
            println!(
                "node, ptr={:?}, node=(height={}, key={}, val={}, flags={:?}, refcnt={:?}, next={:?})",
                next, node.height, key, *node.value, node.flags, node.refcnt, &node.next
            );
            prev = key;
            next = node.next[0].load(Ordering::Acquire);
            let _ = Box::into_raw(node);
        }

        let start_time = Instant::now();
        std::thread::scope(|s| {
            for idx in 0..thr_num {
                let start = idx * count_per_thr;
                let end = start + count_per_thr;
                let slice = &vec[start..end];
                s.spawn(move || {
                    for num in slice {
                        let (k, v) = table_ref.remove(num).unwrap().unwrap();
                        assert_eq!(k, *num);
                        assert_eq!(v, *num + 10);
                    }
                });
            }
        });
        println!("dduration={:?}", start_time.elapsed());
        assert!(table.len() == 0);
        println!("gb_cnt={}", table.gb_cnt.load(Ordering::Acquire));
    }

    #[test]
    #[ignore]
    fn test_perf1() {
        let order = 24usize;
        let mut rng: SmallRng = rand::make_rng();

        let max = 2usize.pow(order as u32);
        let thr_num = 32usize;
        let count_per_thr = max / thr_num;
        let mut vec = Vec::<u64>::with_capacity(max);
        {
            let mut set = BTreeSet::new();
            while vec.len() < max {
                let num = rng.next_u64();
                if !set.contains(&num) {
                    vec.push(num);
                    set.insert(num);
                }
            }
        }
        let vec_ref = &vec;
        println!("test start, len={}", vec.len());

        std::thread::scope(|scope| {
            for map_id in 0..3u32 {
                scope.spawn(move || {
                    let mut config = None;
                    if map_id == 1 {
                        let mut cfg = TunableConfig::default();
                        cfg.epoch_adv_thres = 512;
                        cfg.len_opt = LenOption::AtomicVariable;
                        config = Some(cfg);
                    }
                    let table = SkipTableMap::<u64, u64>::new(order, config);

                    let table_ref = &table;
                    for _ in 0..5 {
                        let start_time = Instant::now();
                        std::thread::scope(|s| {
                            for idx in 0..thr_num {
                                let start = idx * count_per_thr;
                                let end = start + count_per_thr;
                                let slice = &vec_ref[start..end];
                                s.spawn(move || {
                                    for &num in slice {
                                        table_ref.insert(num, num + 10).unwrap();
                                    }
                                });
                            }
                        });
                        println!(
                            "map{}: wduration={:?}, len={}",
                            map_id,
                            start_time.elapsed(),
                            table.len()
                        );
                        let start_time = Instant::now();
                        std::thread::scope(|s| {
                            for idx in 0..thr_num {
                                let start = idx * count_per_thr;
                                let end = start + count_per_thr;
                                let slice = &vec_ref[start..end];
                                s.spawn(move || {
                                    for num in slice {
                                        let v = table_ref.get(num).unwrap().unwrap();
                                        assert_eq!(*v.value(), *num + 10);
                                    }
                                });
                            }
                        });
                        println!(
                            "map{}: rduration={:?}, len={}",
                            map_id,
                            start_time.elapsed(),
                            table.len()
                        );

                        let mut prev = u64::MAX;
                        let mut next = table.next[0].load(Ordering::Acquire);
                        while !next.is_null() {
                            let node = FixedNode::<u64, u64>::ptr_to_mut(next);
                            let key = *node.key;
                            if prev != u64::MAX {
                                assert!(prev < key);
                            }
                            prev = key;
                            next = node.get_next_ptr(next, 0);
                        }

                        let start_time = Instant::now();
                        std::thread::scope(|s| {
                            for idx in 0..thr_num {
                                let start = idx * count_per_thr;
                                let end = start + count_per_thr;
                                let slice = &vec_ref[start..end];
                                s.spawn(move || {
                                    for num in slice {
                                        let (k, v) = table_ref.remove(num).unwrap().unwrap();
                                        assert_eq!(k, *num);
                                        assert_eq!(v, *num + 10);
                                    }
                                });
                            }
                        });
                        println!(
                            "map{}: dduration={:?}, len={}",
                            map_id,
                            start_time.elapsed(),
                            table.len()
                        );
                        assert!(table.len() == 0);
                        println!(
                            "map{}: gb_cnt={}",
                            map_id,
                            table.gb_cnt.load(Ordering::Acquire)
                        );
                    }
                });
            }
        });
    }

    #[test]
    #[ignore]
    fn test_perf2() {
        // {
        //     let dummy_table = SkipTableMap::<u64, u64>::new(1, None);
        //     let _ = dummy_table.insert(0, 0);
        //     let _ = dummy_table.get(&0);
        // }
        // use crate::alloc_tests::ALLOC;
        // use std::sync::atomic::Ordering;
        // let obytes = ALLOC.bytes.load(Ordering::Acquire);
        // let ocount = ALLOC.count.load(Ordering::Acquire);
        {
            let order = 24usize;
            let table = SkipTableMap::<u64, u64>::new(order, None);
            {
                let mut rng: SmallRng = rand::make_rng();

                let max = 2usize.pow(order as u32);
                let mut vec = Vec::<u64>::with_capacity(max);
                {
                    let mut set = BTreeSet::new();
                    while vec.len() < max {
                        let num = rng.next_u64();
                        if !set.contains(&num) {
                            vec.push(num);
                            set.insert(num);
                        }
                    }
                }

                let thr_num = 32usize;
                let vec_ref = &vec;
                let table_ref = &table;
                println!("test start, len={}", vec.len());

                let start_time = Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..thr_num {
                        s.spawn(move || {
                            for idx in 0..max {
                                table_ref.insert(vec_ref[idx], idx as u64).unwrap();
                            }
                        });
                    }
                });
                println!("wduration={:?}", start_time.elapsed());
                println!("len={}", table.len());

                let mut prev = u64::MAX;
                let mut next = table.next[0].load(Ordering::Acquire);
                while !next.is_null() {
                    let node = FixedNode::<u64, u64>::ptr_to_mut(next);
                    let key = *node.key;
                    if prev != u64::MAX {
                        assert!(prev < key);
                    }
                    prev = key;
                    next = node.get_next_ptr(next, 0);
                }

                let start_time = Instant::now();
                std::thread::scope(|s| {
                    for thr_idx in 0..thr_num {
                        s.spawn(move || {
                            let th = std::thread::current().id();
                            let imut = thr_idx & 0x1 == 0;
                            if imut {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    if let Ok(Some(entry)) = table_ref.get(&key) {
                                        let v = entry.value();
                                        let idx_u64 = idx as u64;
                                        assert!(*v == idx_u64 || *v == idx_u64 >> 1)
                                    }
                                }
                            } else {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    if let Ok(Some(mut entry)) = table_ref.get_mut(&key) {
                                        let idx_u64 = idx as u64;
                                        if let Ok(v) = entry.value_mut() {
                                            assert!(*v == idx_u64 || *v == idx_u64 >> 1);
                                            *v = idx_u64 >> 1;
                                        } else {
                                            println!(
                                                "{:?}: unexpected mut error: ptr={:?}",
                                                th, entry.ptr
                                            );
                                        }
                                    }
                                }
                            }
                        });
                    }
                });
                println!("rduration={:?}", start_time.elapsed());
                println!("len={}", table.len());

                let start_time = Instant::now();
                std::thread::scope(|s| {
                    for thr_idx in 0..thr_num {
                        s.spawn(move || {
                            let take_kv = thr_idx & 0x1 == 0;
                            if take_kv {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    let idx_u64 = idx as u64;
                                    if let Ok(Some(res)) = table_ref.remove(&key) {
                                        assert!(res.0 == key);
                                        assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
                                    }
                                }
                            } else {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    table_ref.remove_wo_kv(&key);
                                }
                            }
                        });
                    }
                });
                println!("dduration={:?}", start_time.elapsed());
                println!("len={}", table.len());
                println!("gb_cnt={}", table.gb_cnt.load(Ordering::Acquire));
            }

            LOCAL_EPOCH.with(|guard| guard.borrow_mut().remove_epoch(table.id));
        }

        // let nbytes = ALLOC.bytes.load(Ordering::Acquire);
        // let ncount = ALLOC.count.load(Ordering::Acquire);
        // println!(
        //     "obytes={}, ocount={}, nbytes={}, ncount={}",
        //     obytes, ocount, nbytes, ncount
        // );
        // assert!(obytes == nbytes);
        // assert!(ocount == ncount);
    }

    #[test]
    #[ignore]
    fn test_mixed_ops() {
        {
            let dummy_table = SkipTableMap::<u64, u64>::new(1, None);
            let _ = dummy_table.insert(0, 0);
            let _ = dummy_table.get(&0);
        }
        use crate::alloc_tests::ALLOC;
        use std::sync::atomic::Ordering;
        let obytes = ALLOC.bytes.load(Ordering::Acquire);
        let ocount = ALLOC.count.load(Ordering::Acquire);
        {
            let order = 24usize;
            let table = SkipTableMap::<u64, u64>::new(order, None);
            let mut rng: SmallRng = rand::make_rng();

            let max = 2usize.pow(order as u32);
            let mut vec = Vec::<u64>::with_capacity(max);
            {
                let mut set = BTreeSet::new();
                while vec.len() < max {
                    let num = rng.next_u64();
                    if !set.contains(&num) {
                        vec.push(num);
                        set.insert(num);
                    }
                }
            }

            let thr_num = 32usize;
            let op_cnt = 2u64.pow((order) as u32);
            let mask = max - 1;
            let vec_ref = &vec;
            let table_ref = &table;
            println!("test start, len={}", vec.len());

            let start_time = Instant::now();
            std::thread::scope(|s| {
                for _ in 0..thr_num {
                    s.spawn(move || {
                        let mut rng: SmallRng = rand::make_rng();
                        for op_idx in 0..op_cnt {
                            let rnd = rng.next_u32();
                            let mut idx = rnd as usize & mask;
                            let op = rnd & 0x7;
                            match op {
                                x if x >= 4 => {
                                    // inserting
                                    if x == 4 {
                                        idx = op_idx as usize & mask;
                                    }
                                    let key = vec_ref[idx];
                                    let _ = table_ref.insert(key, idx as u64);
                                }
                                0 => {
                                    // get. key may not in map
                                    let key = vec_ref[idx];
                                    if let Ok(Some(entry)) = table_ref.get(&key) {
                                        let v = entry.value();
                                        let idx_u64 = idx as u64;
                                        assert!(*v == idx_u64 || *v == idx_u64 >> 1)
                                    }
                                }
                                1 => {
                                    // get_mut. key may not in map
                                    let key = vec_ref[idx];
                                    if let Ok(Some(mut entry)) = table_ref.get_mut(&key) {
                                        let v = entry.value_mut().unwrap();
                                        let idx_u64 = idx as u64;
                                        assert!(*v == idx_u64 || *v == idx_u64 >> 1);
                                        *v = idx_u64 >> 1;
                                    }
                                }
                                2 => {
                                    // remove. key may not in map
                                    let key = vec_ref[idx];
                                    let idx_u64 = idx as u64;
                                    if let Ok(Some(res)) = table_ref.remove(&key) {
                                        assert!(res.0 == key);
                                        assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
                                    }
                                }
                                3 => {
                                    // remove_wo_kv. key may not in map
                                    let key = vec_ref[idx];
                                    table_ref.remove_wo_kv(&key);
                                }
                                _ => (),
                            }
                        }
                    });
                }
            });
            println!("duration={:?}", start_time.elapsed());
            println!("gb_cnt={}", table.gb_cnt.load(Ordering::Acquire));
            LOCAL_EPOCH.with(|guard| guard.borrow_mut().remove_epoch(table.id));
        }
        let nbytes = ALLOC.bytes.load(Ordering::Acquire);
        let ncount = ALLOC.count.load(Ordering::Acquire);
        println!(
            "obytes={}, ocount={}, nbytes={}, ncount={}",
            obytes, ocount, nbytes, ncount
        );
        assert!(obytes == nbytes);
        assert!(ocount == ncount);
    }

    #[test]
    fn test_basic_api_correctness() {
        let config = TunableConfig {
            epoch_adv_thres: 256,
            len_opt: LenOption::AtomicVariable,
        };
        let map = SkipTableMap::<u64, String>::new(8, Some(config));

        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        assert!(!map.contains_key(&100));

        let res = map.insert(100, "hundred".to_string());
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), None);
        assert!(!map.is_empty());
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&100));

        {
            let entry = map.get(&100).unwrap().expect("Key 100 should exist");
            assert_eq!(*entry.key(), 100);
            assert_eq!(entry.value(), "hundred");
        }

        let res_update = map.insert(100, "ONE_HUNDRED".to_string());
        assert!(res_update.is_ok());
        assert_eq!(res_update.unwrap(), Some("hundred".to_string()));
        assert_eq!(map.len(), 1);

        {
            let mut entry = map.get_mut(&100).unwrap().expect("Key 100 should exist");
            let val_ref = entry.value_mut().unwrap();
            assert_eq!(val_ref, "ONE_HUNDRED");
            *val_ref = "HUNDRED_MODIFIED".to_string();
        }

        {
            let entry = map.get(&100).unwrap().unwrap();
            assert_eq!(entry.value(), "HUNDRED_MODIFIED");
        }

        assert!(!map.remove_wo_kv(&200));
        assert!(map.remove_wo_kv(&100));
        assert_eq!(map.len(), 0);
        assert!(!map.contains_key(&100));
        assert!(map.get(&100).unwrap().is_none());

        map.insert(200, "two hundred".to_string()).unwrap();
        assert_eq!(map.len(), 1);

        let rem_res = map.remove(&200).unwrap();
        assert_eq!(rem_res, Some((200, "two hundred".to_string())));
        assert_eq!(map.len(), 0);
        assert!(!map.contains_key(&200));
        assert_eq!(map.remove(&200).unwrap(), None);
    }

    #[test]
    fn test_concurrency_and_exclusion() {
        let map = Arc::new(SkipTableMap::<u64, u64>::new(8, None));
        map.insert(1, 100).unwrap();

        {
            let entry_shared = map.get(&1).unwrap().unwrap();
            let mut_res = map.get_mut(&1);
            assert!(matches!(mut_res, Err(ElementInUse)));
            drop(entry_shared);

            let entry_mut = map.get_mut(&1).unwrap().unwrap();
            let get_res = map.get(&1);
            assert!(matches!(get_res, Err(ElementInMutUse)));
            drop(entry_mut);
        }

        {
            let entry_ref = map.get(&1).unwrap().unwrap();

            let rem_res = map.remove(&1);
            assert!(matches!(rem_res, Err(ElementInUse)));

            let ins_res = map.insert(1, 200);
            assert_eq!(ins_res, Err((1, 200)));

            drop(entry_ref);
        }

        {
            let entry_mut = map.get_mut(&1).unwrap().unwrap();

            let rem_res = map.remove(&1);
            assert!(matches!(rem_res, Err(ElementInUse)));

            let ins_res = map.insert(1, 300);
            assert_eq!(ins_res, Err((1, 300)));

            drop(entry_mut);
        }

        assert!(map.insert(1, 400).is_ok());
        assert_eq!(map.get(&1).unwrap().unwrap().value(), &400);
        assert!(map.remove(&1).unwrap().is_some());
    }

    #[test]
    fn test_multithreaded_exclusion() {
        let map = Arc::new(SkipTableMap::<u64, u64>::new(8, None));
        map.insert(42, 1000).unwrap();

        let barrier = Arc::new(Barrier::new(2));

        let map_clone = map.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let _mut_guard = map_clone.get_mut(&42).unwrap().unwrap();
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(50));
        });

        barrier.wait();
        assert!(matches!(map.get(&42), Err(ElementInMutUse)));
        assert!(matches!(map.get_mut(&42), Err(ElementInUse)));
        assert!(matches!(map.remove(&42), Err(ElementInUse)));
        assert_eq!(map.insert(42, 2000), Err((42, 2000)));

        handle.join().unwrap();

        assert!(map.get(&42).unwrap().is_some());
    }

    #[derive(Debug)]
    struct DropDetector {
        id: u64,
        drop_count: Arc<AtomicUsize>,
    }

    impl Drop for DropDetector {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_gc_destructor_behavior_on_remove_and_remove_wo_kv() {
        let key_drops = Arc::new(AtomicUsize::new(0));
        let val_drops = Arc::new(AtomicUsize::new(0));

        let config = TunableConfig {
            epoch_adv_thres: 1,
            len_opt: LenOption::NotProvided,
        };
        let map = SkipTableMap::<DropDetector, DropDetector>::new(4, Some(config));

        {
            let k1 = DropDetector {
                id: 1,
                drop_count: key_drops.clone(),
            };
            let v1 = DropDetector {
                id: 10,
                drop_count: val_drops.clone(),
            };

            map.insert(k1, v1).unwrap();
            assert_eq!(key_drops.load(Ordering::SeqCst), 0);
            assert_eq!(val_drops.load(Ordering::SeqCst), 0);

            let removed = map.remove_wo_kv(&DropDetector {
                id: 1,
                drop_count: Arc::new(AtomicUsize::new(0)),
            });
            assert!(removed);

            assert_eq!(key_drops.load(Ordering::SeqCst), 0);
            assert_eq!(val_drops.load(Ordering::SeqCst), 0);
        }

        drop(map);

        assert_eq!(key_drops.load(Ordering::SeqCst), 1,);
        assert_eq!(val_drops.load(Ordering::SeqCst), 1,);

        let key_drops_2 = Arc::new(AtomicUsize::new(0));
        let val_drops_2 = Arc::new(AtomicUsize::new(0));

        {
            let config2 = TunableConfig {
                epoch_adv_thres: 1,
                len_opt: LenOption::NotProvided,
            };
            let map2 = SkipTableMap::<DropDetector, DropDetector>::new(4, Some(config2));

            let k2 = DropDetector {
                id: 2,
                drop_count: key_drops_2.clone(),
            };
            let v2 = DropDetector {
                id: 20,
                drop_count: val_drops_2.clone(),
            };

            map2.insert(k2, v2).unwrap();

            let kv_option = map2
                .remove(&DropDetector {
                    id: 2,
                    drop_count: Arc::new(AtomicUsize::new(0)),
                })
                .unwrap();
            assert!(kv_option.is_some());
            let (k_taken, v_taken) = kv_option.unwrap();

            assert_eq!(key_drops_2.load(Ordering::SeqCst), 0);
            assert_eq!(val_drops_2.load(Ordering::SeqCst), 0);

            drop(k_taken);
            drop(v_taken);

            assert_eq!(key_drops_2.load(Ordering::SeqCst), 1);
            assert_eq!(val_drops_2.load(Ordering::SeqCst), 1);

            drop(map2);
        }

        assert_eq!(key_drops_2.load(Ordering::SeqCst), 1,);
        assert_eq!(val_drops_2.load(Ordering::SeqCst), 1,);
    }

    impl PartialEq for DropDetector {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
    impl Eq for DropDetector {}

    impl PartialOrd for DropDetector {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl std::cmp::Ord for DropDetector {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.id.cmp(&other.id)
        }
    }
}

#[cfg(test)]
mod bytes_tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_bytes_basic_operations() {
        let config = TunableConfig {
            epoch_adv_thres: 256,
            len_opt: LenOption::AtomicVariable,
        };
        let map = SkipTableMap::<Bytes, Bytes>::new(8, Some(config));

        let k1 = Bytes::from("key1");
        let v1 = Bytes::from("value1");
        let v1_updated = Bytes::from("value1_updated");

        assert_eq!(map.insert(k1.clone(), v1.clone()).unwrap(), None);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&k1));

        {
            let entry = map.get(&k1).unwrap().unwrap();
            assert_eq!(entry.key(), &k1);
            assert_eq!(entry.value(), &v1);
        }

        let old_val = map.insert(k1.clone(), v1_updated.clone()).unwrap();
        assert_eq!(old_val, Some(v1.clone()));
        assert_eq!(map.len(), 1);

        {
            let mut entry = map.get_mut(&k1).unwrap().unwrap();
            let val_mut = entry.value_mut().unwrap();
            *val_mut = Bytes::from("value1_mutated");
        }

        {
            let entry = map.get(&k1).unwrap().unwrap();
            assert_eq!(entry.value(), &Bytes::from("value1_mutated"));
        }

        let k2 = Bytes::from("key2");
        let v2 = Bytes::from("value2");
        map.insert(k2.clone(), v2.clone()).unwrap();
        assert_eq!(map.len(), 2);

        assert!(map.remove_wo_kv(&k2));
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key(&k2));

        let removed_kv = map.remove(&k1).unwrap();
        assert_eq!(
            removed_kv,
            Some((k1.clone(), Bytes::from("value1_mutated")))
        );
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_bytes_exclusion() {
        let map = SkipTableMap::<Bytes, Bytes>::new(8, None);
        let key = Bytes::from("exclusive_key");
        let val = Bytes::from("exclusive_val");

        map.insert(key.clone(), val.clone()).unwrap();

        {
            let entry_guard = map.get(&key).unwrap().unwrap();
            assert_eq!(entry_guard.value(), &val);
            assert!(matches!(map.get_mut(&key), Err(ElementInUse)));
            assert!(matches!(map.remove(&key), Err(ElementInUse)));

            let new_val = Bytes::from("new_val");
            assert_eq!(
                map.insert(key.clone(), new_val.clone()),
                Err((key.clone(), new_val))
            );

            drop(entry_guard);
        }

        {
            let entry_mut_guard = map.get_mut(&key).unwrap().unwrap();
            assert!(matches!(map.get(&key), Err(ElementInMutUse)));
            assert!(matches!(map.remove(&key), Err(ElementInUse)));
            drop(entry_mut_guard);
        }

        assert!(map.remove(&key).unwrap().is_some());
    }
}
