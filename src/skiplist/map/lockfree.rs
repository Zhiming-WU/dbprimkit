//! A key-value map based on skip list. It's designed to be used in concurrent mode and performance
//! takes high priority. It has narrower APIs comapred to [super::ordinary].
//!
//! EBR (epoch based reclamation) approach is used to safely free objects (map entries). Objects deleted
//! in current epoch are not immediately freed in current epoch but in later epoch, so that other users
//! who concurrently (in parallel) traverse the map don't encounter use-after-free problem.
//!
//! <div class="warning" id="caution_epoch_pinner">
//!
//! **CAUTION**
//!
//! The postponing of freeing objects is done by holding a `EpochPinner` which prevents the epoch
//! from advancing. If you do some operation (something like [SkipListMap::iter_with],
//! [SkipListMap::iter_mut_with] or [SkipListMap::clear])
//! which holds a `EpochPinner` too long, it prevents the epoch from advancing, which means deleted objects
//! may not get freed in time, and it also causes the thread which is trying to advance the epoch (potential
//! performed in [SkipListMap::remove] or [SkipListMap::remove_wo_kv], etc.) stuck at waiting the finish of
//! the `EpochPinner`.
//!
//! </div>
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::skiplist::map::lockfree::SkipListMap;
//! use rand::Rng;
//! use rand::rngs::SmallRng;
//!
//! fn main() {
//!     let order = 12usize;
//!     let map = SkipListMap::<u64, u64>::new(order, None);
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
//!     let map_ref = &map;
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
//!                             let _ = map_ref.insert(key, idx as u64);
//!                         }
//!                         0 => {
//!                             // get. key may not in map
//!                             let key = vec_ref[idx];
//!                             if let Ok(Some(entry)) = map_ref.get(&key) {
//!                                 let v = entry.value();
//!                                 let idx_u64 = idx as u64;
//!                                 assert!(*v == idx_u64 || *v == idx_u64 >> 1)
//!                             }
//!                         }
//!                         1 => {
//!                             // get_mut. key may not in map
//!                             let key = vec_ref[idx];
//!                             if let Ok(Some(mut entry)) = map_ref.get_mut(&key) {
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
//!                             if let Ok(Some(res)) = map_ref.remove(&key) {
//!                                 assert!(res.0 == key);
//!                                 assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
//!                             }
//!                         }
//!                         3 => {
//!                             // remove_wo_kv. key may not in map
//!                             let key = vec_ref[idx];
//!                             map_ref.remove_wo_kv(&key);
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

struct MapEpoch {
    lock: SpinLock<()>,
    map_id: u64,
    epoch_set: HashSet<usize>,
}

impl MapEpoch {
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
    vec: Vec<MapEpoch>,
}

impl EpochRegistry {
    fn get_map_epoch(&mut self, map_id: u64) -> Option<&mut MapEpoch> {
        let _l = self.lock.lock();
        self.vec.iter_mut().find(|te| te.map_id == map_id)
    }

    fn reg_map_epoch(&mut self, map_id: u64) {
        let me = MapEpoch {
            lock: SpinLock::<()>::new(()),
            map_id,
            epoch_set: HashSet::new(),
        };
        let _l = self.lock.lock();
        self.vec.push(me);
    }

    fn remove_map_epoch(&mut self, map_id: u64) {
        let _l = self.lock.lock();
        if let Some(pos) = self.vec.iter().position(|te| te.map_id == map_id) {
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
    map_id: u64,
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
    fn get_epoch(&mut self, map_id: u64) -> &mut LocalEpoch {
        if let Some(pos) = self.vec.iter().position(|e| e.map_id == map_id) {
            &mut self.vec[pos]
        } else {
            let epoch = LocalEpoch {
                map_id,
                gepoch: 0,
                active: CacheAligned(AtomicUsize::new(FALSE)),
                epoch: CacheAligned(AtomicUsize::new(0)),
            };
            self.vec.push(epoch);
            let ret = self.vec.last_mut().unwrap();
            let mut reg = get_registry();
            let t_epoch = reg.get_map_epoch(map_id).unwrap();
            t_epoch.register_epoch(ret as *const LocalEpoch as usize);
            ret
        }
    }

    #[cfg(test)]
    fn remove_epoch(&mut self, map_id: u64) {
        let res = self.vec.iter().position(|e| e.map_id == map_id);
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
            if let Some(gepoch) = get_registry().get_map_epoch(lepoch.map_id) {
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

/// The option for [SkipListMap::len].
pub enum LenOption {
    /// Not provided, [SkipListMap::len] always returns `0`.
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

/// The optional configuration for the [SkipListMap].
pub struct TunableConfig {
    /// The threshold to advance the epoch to reclaim the garbage in EBR.
    /// The allowed range is `[256,)`. The default value is `256`.
    pub epoch_adv_thres: usize,
    /// The option for [SkipListMap::len]. The default value is [LenOption::NotProvided].
    pub len_opt: LenOption,
    // /// The option for [SkipListMap::clear].
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

/// A key-value map based on skip list.
pub struct SkipListMap<K, V> {
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

struct Node<K, V> {
    _height: usize,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
    _flags: AtomicU32,
    _refcnt: AtomicU32,
    _next: [NodeAtomicPtr],
}

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
    fn set_unset_flags(&self, set: bool, mask: u32) {
        let mut flags = self.flags.load(Ordering::Acquire);
        let mut nflags = if set { flags | mask } else { flags & !mask };
        while nflags != flags {
            match self.flags.compare_exchange_weak(
                flags,
                nflags,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(f) => {
                    flags = f;
                    nflags = if set { flags | mask } else { flags & !mask };
                }
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
    /// [SkipListMap::get] rather than [SkipListMap::get_mut], an [ElementRefNotMutable] error
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

impl<K, V> Drop for SkipListMap<K, V> {
    fn drop(&mut self) {
        get_registry().remove_map_epoch(self.id);
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

impl<K, V> SkipListMap<K, V> {
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
                let gepoch = reg.get_map_epoch(self.id).unwrap();
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

    /// Makes a new, empty [SkipListMap].
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
        let map_id = ID_GEN.fetch_add(1, Ordering::AcqRel);
        get_registry().reg_map_epoch(map_id);
        Self {
            id: map_id,
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
        let res = LOCAL_EPOCH
            .with(|guard| self.insert_inner(key, value, false, guard.borrow_mut().get_epoch(id)));
        res.map(|opt_kv| opt_kv.map(|kv| kv.1))
    }

    pub(crate) fn replace(&self, key: K, value: V) -> std::result::Result<Option<(K, V)>, (K, V)>
    where
        K: Ord,
    {
        let id = self.id;
        LOCAL_EPOCH
            .with(|guard| self.insert_inner(key, value, true, guard.borrow_mut().get_epoch(id)))
    }

    fn insert_inner(
        &self,
        mut key: K,
        mut value: V,
        repalce_key: bool,
        epoch: &mut LocalEpoch,
    ) -> std::result::Result<Option<(K, V)>, (K, V)>
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
                                if repalce_key {
                                    std::mem::swap(&mut key, &mut node.key);
                                }
                                std::mem::swap(&mut value, &mut node.value);
                                node.set_unset_flags(false, FLAG_UPDATING_VALUE);
                                return Ok(Some((key, value)));
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
                node.set_unset_flags(false, FLAG_LINKING);
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
    /// [ElementInUse] is returned if the the entry is being used by others via [SkipListMap::get] or
    /// [SkipListMap::get_mut], since ownership of key and value can't be taken in that case.
    /// Use [SkipListMap::remove_wo_kv] instead to avoid that if you don't need key and value.
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
    /// [ElementInMutUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get_mut].
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
    /// [ElementInUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get] or [SkipListMap::get_mut].
    pub fn get_mut<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.get_inner(key, true, guard.borrow_mut().get_epoch(id)))
    }

    fn get_entry_ref(&self, ptr: NodePtr, mutable: bool) -> Result<Option<EntryRef<K, V>>> {
        if ptr.is_null() {
            Ok(None)
        } else {
            let node = FixedNode::<K, V>::ptr_to_mut(ptr);
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
                                return Ok(Some(EntryRef::<K, V>::from_ptr(ptr, mutable)));
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
                        return Ok(Some(EntryRef::<K, V>::from_ptr(ptr, false)));
                    }
                }
            }
        }
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
        self.get_entry_ref(sch_ptr, mutable)
    }

    /// Returns the number of elements in the map. NOTE that by default it doesn't work (always returns 0)
    /// unless you configured the map with a [LenOption] rather than [LenOption::NotProvided].
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

    /// Returns a reference to the first entry, if any.
    ///
    /// [ElementInMutUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get_mut] or [SkipListMap::first_entry_mut].
    pub fn first_entry(&self) -> Result<Option<EntryRef<K, V>>> {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.first_entry_inner(false, guard.borrow_mut().get_epoch(id)))
    }

    /// Returns a mutable reference to the first entry, if any.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get], [SkipListMap::get_mut], [SkipListMap::first_entry] or [SkipListMap::first_entry_mut].
    pub fn first_entry_mut(&self) -> Result<Option<EntryRef<K, V>>> {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.first_entry_inner(true, guard.borrow_mut().get_epoch(id)))
    }

    fn first_entry_inner(
        &self,
        mutable: bool,
        epoch: &mut LocalEpoch,
    ) -> Result<Option<EntryRef<K, V>>> {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        let ptr = self.next[0].load(Ordering::Acquire);
        self.get_entry_ref(ptr, mutable)
    }

    fn search_last(&self) -> NodePtr {
        let null: *mut u8 = std::ptr::null_mut();
        let mut level = self.max_height - 1;
        let mut next: NodePtr = null;
        let mut res: NodePtr = null;

        'level_loop: loop {
            if next.is_null() {
                next = self.next[level].load(Ordering::Acquire);
            }

            'same_level: loop {
                if next.is_null() {
                    if level == 0 {
                        break 'level_loop;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    res = next;
                    let tmp_next = FixedNode::<K, V>::fn_get_next_ptr(next, level);
                    if !tmp_next.is_null() {
                        next = tmp_next;
                        continue 'same_level;
                    } else {
                        if level == 0 {
                            break 'level_loop;
                        }
                        level -= 1;
                        continue 'level_loop;
                    }
                }
            }
        }
        res
    }

    /// Returns a reference to the last entry, if any.
    ///
    /// [ElementInMutUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get_mut] or [SkipListMap::last_entry_mut].
    pub fn last_entry(&self) -> Result<Option<EntryRef<K, V>>> {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.last_entry_inner(false, guard.borrow_mut().get_epoch(id)))
    }

    /// Returns a mutable reference to the last entry, if any.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others via something like
    /// [SkipListMap::get], [SkipListMap::get_mut], [SkipListMap::last_entry] or [SkipListMap::last_entry_mut].
    pub fn last_entry_mut(&self) -> Result<Option<EntryRef<K, V>>> {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.last_entry_inner(true, guard.borrow_mut().get_epoch(id)))
    }

    fn last_entry_inner(
        &self,
        mutable: bool,
        epoch: &mut LocalEpoch,
    ) -> Result<Option<EntryRef<K, V>>> {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        let ptr = self.search_last();
        self.get_entry_ref(ptr, mutable)
    }

    /// Returns a reference to the entry that immediately after the `key`, if any.
    ///
    /// [ElementInMutUse] is returned if the the entry is being mutably used by others.
    pub fn next_entry<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| {
            self.next_or_prev_entry_inner(key, true, false, guard.borrow_mut().get_epoch(id))
        })
    }

    /// Returns a mutable reference to the entry that immediately after the `key`, if any.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others.
    pub fn next_entry_mut<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| {
            self.next_or_prev_entry_inner(key, true, true, guard.borrow_mut().get_epoch(id))
        })
    }

    /// Returns a reference to the entry that immediately before the `key`, if any.
    ///
    /// [ElementInMutUse] is returned if the the entry is being mutably used by others.
    pub fn prev_entry<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| {
            self.next_or_prev_entry_inner(key, false, false, guard.borrow_mut().get_epoch(id))
        })
    }

    /// Returns a mutable reference to the entry that immediately before the `key`, if any.
    ///
    /// [ElementInUse] is returned if the the entry is being used by others.
    pub fn prev_entry_mut<Q>(&self, key: &Q) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| {
            self.next_or_prev_entry_inner(key, false, true, guard.borrow_mut().get_epoch(id))
        })
    }

    fn next_or_prev_entry_inner<Q>(
        &self,
        key: &Q,
        is_next: bool,
        mutable: bool,
        epoch: &mut LocalEpoch,
    ) -> Result<Option<EntryRef<K, V>>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let mut vec = PrecVec::new();
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        loop {
            let res_ptr = self.search_ext(key, 1, 0, &mut vec);
            let ptr = {
                let ent = vec.get_ent(0);
                if is_next {
                    if !res_ptr.is_null() {
                        FixedNode::<K, V>::fn_get_next_ptr(res_ptr, 0)
                    } else {
                        ent.next
                    }
                } else {
                    ent.prec
                }
            };
            if ptr.is_null() {
                return Ok(None);
            }
            match self.get_entry_ref(ptr, mutable) {
                Ok(None) => {
                    // entry is deleted, retry
                    std::hint::spin_loop();
                    continue;
                }
                res @ _ => return res,
            }
        }
    }

    /// Clears the map, removing all elements.
    ///
    /// <div class="warning">
    ///
    /// **CAUTION**
    ///
    /// This operation holds a `EpochPinner` for long if the number of entries in the map is large.
    /// See [the descreption here](super::lockfree#caution_epoch_pinner).
    ///
    /// Also notice that if concurrent inserting occurs, whether the inserted entry is cleared
    /// from the mapis not certain, depending on its position and timing inserted.
    ///
    /// It may be an option to create a new map instead of calling this method.
    ///
    /// </div>
    pub fn clear(&self) {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.clear_inner(guard.borrow_mut().get_epoch(id)))
    }

    fn clear_inner(&self, epoch: &mut LocalEpoch) {
        let null: NodePtr = std::ptr::null_mut();
        for level in (1..self.max_height).rev() {
            self.next[level].store(null, Ordering::Release);
        }
        let pin = LocalEpochPinner::new(epoch, &self.epoch);
        let epoch = pin.0.gepoch;
        let mut next = self.next[0].swap(null, Ordering::AcqRel);
        while !next.is_null() {
            let node = FixedNode::<K, V>::ptr_to_mut(next);
            node.set_unset_flags(true, FLAG_DELETED);
            self.put_garbage(epoch, next, node.height);
            next = node.get_next_ptr(next, 0);
        }
        drop(pin);
        self.adv_epoch(epoch);
    }

    /// Iterate entries with a function.
    ///
    /// <div class="warning">
    ///
    /// **CAUTION**
    ///
    /// This operation holds a `EpochPinner` for long if the number of entries in the map is large.
    /// See [the descreption here](super::lockfree#caution_epoch_pinner).
    ///
    /// Also notice that the entries being mutably used by others are skipped to call the provided
    /// function.
    ///
    /// It may be an option to use [SkipListMap::next_entry] with with [SkipListMap::first_entry]
    /// to achieve iteration.
    ///
    /// </div>
    pub fn iter_with<F>(&self, func: F)
    where
        F: FnMut(EntryRef<K, V>),
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.iter_with_inner(func, guard.borrow_mut().get_epoch(id)))
    }

    fn iter_with_inner<F>(&self, mut func: F, epoch: &mut LocalEpoch)
    where
        F: FnMut(EntryRef<K, V>),
    {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        let mut ptr = self.next[0].load(Ordering::Acquire);
        while !ptr.is_null() {
            if let Ok(Some(ent)) = self.get_entry_ref(ptr, false) {
                func(ent);
            }
            ptr = FixedNode::<K, V>::fn_get_next_ptr(ptr, 0);
        }
    }

    /// Mutably iterate entries with a function.
    ///
    /// <div class="warning">
    ///
    /// **CAUTION**
    ///
    /// This operation holds a `EpochPinner` for long if the number of entries in the map is large.
    /// See [the descreption here](super::lockfree#caution_epoch_pinner).
    ///
    /// Also notice that the entries being used by others are skipped to call the provided
    /// function.
    ///
    /// It may be an option to use [SkipListMap::next_entry_mut] with [SkipListMap::first_entry_mut]
    /// to achieve iteration.
    ///
    /// </div>
    pub fn iter_mut_with<F>(&self, func: F)
    where
        F: FnMut(EntryRef<K, V>),
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.iter_mut_with_inner(func, guard.borrow_mut().get_epoch(id)))
    }

    fn iter_mut_with_inner<F>(&self, mut func: F, epoch: &mut LocalEpoch)
    where
        F: FnMut(EntryRef<K, V>),
    {
        let _pin = LocalEpochPinner::new(epoch, &self.epoch);
        let mut ptr = self.next[0].load(Ordering::Acquire);
        while !ptr.is_null() {
            if let Ok(Some(ent)) = self.get_entry_ref(ptr, true) {
                func(ent);
            }
            ptr = FixedNode::<K, V>::fn_get_next_ptr(ptr, 0);
        }
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
        let map = SkipListMap::<u64, u64>::new(order, None);
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
            map.insert(num, num + 10).unwrap();
        }
        println!("wduration={:?}", start_time.elapsed());
        //println!("map={:?}", map);
        let mut next = map.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = Node::<u64, u64>::from_node_ptr(next);
            println!(
                "node, ptr={:?}, node=(height={}, key={}, val={}, flags={:?}, refcnt={:?}, next={:?})",
                next, node._height, *node.key, *node.value, node._flags, node._refcnt, &node._next
            );
            next = node._next[0].load(Ordering::Acquire);
            let _ = Box::into_raw(node);
        }
        let start_time = Instant::now();
        for num in &vec {
            let v = map.get(num).unwrap().unwrap();
            assert_eq!(*v.value(), *num + 10);
        }
        println!("rduration={:?}", start_time.elapsed());
    }

    #[test]
    fn check_nodes2() {
        let order = 12usize;
        let map = SkipListMap::<u64, u64>::new(order, None);
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
        let map_ref = &map;
        let start_time = Instant::now();
        std::thread::scope(|s| {
            for idx in 0..thr_num {
                let start = idx * count_per_thr;
                let end = start + count_per_thr;
                let slice = &vec[start..end];
                s.spawn(move || {
                    for &num in slice {
                        map_ref.insert(num, num + 10).unwrap();
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
                        let v = map_ref.get(num).unwrap().unwrap();
                        assert_eq!(*v.value(), *num + 10);
                    }
                });
            }
        });
        println!("rduration={:?}", start_time.elapsed());

        let mut prev = u64::MAX;
        println!("map.next={:?}", &map.next);
        let mut next = map.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = Node::<u64, u64>::from_node_ptr(next);
            let key = *node.key;
            if prev != u64::MAX {
                assert!(prev < key);
            }
            println!(
                "node, ptr={:?}, node=(height={}, key={}, val={}, flags={:?}, refcnt={:?}, next={:?})",
                next, node._height, key, *node.value, node._flags, node._refcnt, &node._next
            );
            prev = key;
            next = node._next[0].load(Ordering::Acquire);
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
                        let (k, v) = map_ref.remove(num).unwrap().unwrap();
                        assert_eq!(k, *num);
                        assert_eq!(v, *num + 10);
                    }
                });
            }
        });
        println!("dduration={:?}", start_time.elapsed());
        assert!(map.len() == 0);
        println!("gb_cnt={}", map.gb_cnt.load(Ordering::Acquire));
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
                    let map = SkipListMap::<u64, u64>::new(order, config);

                    let map_ref = &map;
                    for _ in 0..5 {
                        let start_time = Instant::now();
                        std::thread::scope(|s| {
                            for idx in 0..thr_num {
                                let start = idx * count_per_thr;
                                let end = start + count_per_thr;
                                let slice = &vec_ref[start..end];
                                s.spawn(move || {
                                    for &num in slice {
                                        map_ref.insert(num, num + 10).unwrap();
                                    }
                                });
                            }
                        });
                        println!(
                            "map{}: wduration={:?}, len={}",
                            map_id,
                            start_time.elapsed(),
                            map.len()
                        );
                        let start_time = Instant::now();
                        std::thread::scope(|s| {
                            for idx in 0..thr_num {
                                let start = idx * count_per_thr;
                                let end = start + count_per_thr;
                                let slice = &vec_ref[start..end];
                                s.spawn(move || {
                                    for num in slice {
                                        let v = map_ref.get(num).unwrap().unwrap();
                                        assert_eq!(*v.value(), *num + 10);
                                    }
                                });
                            }
                        });
                        println!(
                            "map{}: rduration={:?}, len={}",
                            map_id,
                            start_time.elapsed(),
                            map.len()
                        );

                        let mut prev = u64::MAX;
                        let mut next = map.next[0].load(Ordering::Acquire);
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
                                        let (k, v) = map_ref.remove(num).unwrap().unwrap();
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
                            map.len()
                        );
                        assert!(map.len() == 0);
                        println!(
                            "map{}: gb_cnt={}",
                            map_id,
                            map.gb_cnt.load(Ordering::Acquire)
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
        //     let dummy_map = SkipListMap::<u64, u64>::new(1, None);
        //     let _ = dummy_map.insert(0, 0);
        //     let _ = dummy_map.get(&0);
        // }
        // use crate::alloc_tests::ALLOC;
        // use std::sync::atomic::Ordering;
        // let obytes = ALLOC.bytes.load(Ordering::Acquire);
        // let ocount = ALLOC.count.load(Ordering::Acquire);
        {
            let order = 24usize;
            let map = SkipListMap::<u64, u64>::new(order, None);
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
                let map_ref = &map;
                println!("test start, len={}", vec.len());

                let start_time = Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..thr_num {
                        s.spawn(move || {
                            for idx in 0..max {
                                map_ref.insert(vec_ref[idx], idx as u64).unwrap();
                            }
                        });
                    }
                });
                println!("wduration={:?}", start_time.elapsed());
                println!("len={}", map.len());

                let mut prev = u64::MAX;
                let mut next = map.next[0].load(Ordering::Acquire);
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
                                    if let Ok(Some(entry)) = map_ref.get(&key) {
                                        let v = entry.value();
                                        let idx_u64 = idx as u64;
                                        assert!(*v == idx_u64 || *v == idx_u64 >> 1)
                                    }
                                }
                            } else {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    if let Ok(Some(mut entry)) = map_ref.get_mut(&key) {
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
                println!("len={}", map.len());

                let start_time = Instant::now();
                std::thread::scope(|s| {
                    for thr_idx in 0..thr_num {
                        s.spawn(move || {
                            let take_kv = thr_idx & 0x1 == 0;
                            if take_kv {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    let idx_u64 = idx as u64;
                                    if let Ok(Some(res)) = map_ref.remove(&key) {
                                        assert!(res.0 == key);
                                        assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
                                    }
                                }
                            } else {
                                for idx in 0..max {
                                    let key = vec_ref[idx];
                                    map_ref.remove_wo_kv(&key);
                                }
                            }
                        });
                    }
                });
                println!("dduration={:?}", start_time.elapsed());
                println!("len={}", map.len());
                println!("gb_cnt={}", map.gb_cnt.load(Ordering::Acquire));
            }

            LOCAL_EPOCH.with(|guard| guard.borrow_mut().remove_epoch(map.id));
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
        // {
        //     let dummy_map = SkipListMap::<u64, u64>::new(1, None);
        //     let _ = dummy_map.insert(0, 0);
        //     let _ = dummy_map.get(&0);
        // }
        // use crate::alloc_tests::ALLOC;
        // use std::sync::atomic::Ordering;
        // let obytes = ALLOC.bytes.load(Ordering::Acquire);
        // let ocount = ALLOC.count.load(Ordering::Acquire);
        {
            let order = 24usize;
            let map = SkipListMap::<u64, u64>::new(order, None);
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
            let map_ref = &map;
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
                                    let _ = map_ref.insert(key, idx as u64);
                                }
                                0 => {
                                    // get. key may not in map
                                    let key = vec_ref[idx];
                                    if let Ok(Some(entry)) = map_ref.get(&key) {
                                        let v = entry.value();
                                        let idx_u64 = idx as u64;
                                        assert!(*v == idx_u64 || *v == idx_u64 >> 1)
                                    }
                                }
                                1 => {
                                    // get_mut. key may not in map
                                    let key = vec_ref[idx];
                                    if let Ok(Some(mut entry)) = map_ref.get_mut(&key) {
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
                                    if let Ok(Some(res)) = map_ref.remove(&key) {
                                        assert!(res.0 == key);
                                        assert!(res.1 == idx_u64 || res.1 == idx_u64 >> 1)
                                    }
                                }
                                3 => {
                                    // remove_wo_kv. key may not in map
                                    let key = vec_ref[idx];
                                    map_ref.remove_wo_kv(&key);
                                }
                                _ => (),
                            }
                        }
                    });
                }
            });
            println!("duration={:?}", start_time.elapsed());
            println!("gb_cnt={}", map.gb_cnt.load(Ordering::Acquire));
            LOCAL_EPOCH.with(|guard| guard.borrow_mut().remove_epoch(map.id));
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
    fn test_basic_api_correctness() {
        let config = TunableConfig {
            epoch_adv_thres: 256,
            len_opt: LenOption::AtomicVariable,
        };
        let map = SkipListMap::<u64, String>::new(8, Some(config));

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
        let map = Arc::new(SkipListMap::<u64, u64>::new(8, None));
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
        let map = Arc::new(SkipListMap::<u64, u64>::new(8, None));
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
        let map = SkipListMap::<DropDetector, DropDetector>::new(4, Some(config));

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
            let map2 = SkipListMap::<DropDetector, DropDetector>::new(4, Some(config2));

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
        let map = SkipListMap::<Bytes, Bytes>::new(8, Some(config));

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
        let map = SkipListMap::<Bytes, Bytes>::new(8, None);
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

#[cfg(test)]
mod iter_and_clear_tests {
    use super::*;

    fn setup_test_map() -> SkipListMap<u64, String> {
        let config = TunableConfig {
            epoch_adv_thres: 256,
            len_opt: LenOption::AtomicVariable,
        };
        let map = SkipListMap::<u64, String>::new(8, Some(config));
        map.insert(20, "twenty".to_string()).unwrap();
        map.insert(10, "ten".to_string()).unwrap();
        map.insert(30, "thirty".to_string()).unwrap();
        map
    }

    #[test]
    fn test_first_and_last_entry() {
        let map = setup_test_map();

        {
            let first = map.first_entry().unwrap().expect("Should have first entry");
            assert_eq!(*first.key(), 10);
            assert_eq!(first.value(), "ten");
        }

        {
            let last = map.last_entry().unwrap().expect("Should have last entry");
            assert_eq!(*last.key(), 30);
            assert_eq!(last.value(), "thirty");
        }

        let empty_map = SkipListMap::<u64, u64>::new(4, None);
        assert!(empty_map.first_entry().unwrap().is_none());
        assert!(empty_map.last_entry().unwrap().is_none());
    }

    #[test]
    fn test_first_and_last_entry_mut() {
        let map = setup_test_map();

        {
            let mut first_mut = map
                .first_entry_mut()
                .unwrap()
                .expect("Should have first entry");
            assert_eq!(*first_mut.key(), 10);
            let val = first_mut.value_mut().unwrap();
            *val = "TEN_MUTATED".to_string();
        }

        {
            let first = map.first_entry().unwrap().unwrap();
            assert_eq!(first.value(), "TEN_MUTATED");
        }

        {
            let mut last_mut = map
                .last_entry_mut()
                .unwrap()
                .expect("Should have last entry");
            assert_eq!(*last_mut.key(), 30);
            let val = last_mut.value_mut().unwrap();
            *val = "THIRTY_MUTATED".to_string();
        }

        {
            let last = map.last_entry().unwrap().unwrap();
            assert_eq!(last.value(), "THIRTY_MUTATED");
        }

        let empty_map = SkipListMap::<u64, u64>::new(4, None);
        assert!(empty_map.first_entry_mut().unwrap().is_none());
        assert!(empty_map.last_entry_mut().unwrap().is_none());
    }

    #[test]
    fn test_next_and_prev_entry() {
        let map = setup_test_map();

        {
            let prev = map.prev_entry(&20).unwrap().unwrap();
            assert_eq!(*prev.key(), 10);

            let next = map.next_entry(&20).unwrap().unwrap();
            assert_eq!(*next.key(), 30);

            assert!(map.prev_entry(&10).unwrap().is_none());
            let next_of_10 = map.next_entry(&10).unwrap().unwrap();
            assert_eq!(*next_of_10.key(), 20);

            assert!(map.next_entry(&30).unwrap().is_none());
            let prev_of_30 = map.prev_entry(&30).unwrap().unwrap();
            assert_eq!(*prev_of_30.key(), 20);

            let prev_of_15 = map.prev_entry(&15).unwrap().unwrap();
            assert_eq!(*prev_of_15.key(), 10);

            let next_of_15 = map.next_entry(&15).unwrap().unwrap();
            assert_eq!(*next_of_15.key(), 20);
        }
    }

    #[test]
    fn test_next_and_prev_entry_mut() {
        let map = setup_test_map();

        {
            let mut next_mut = map.next_entry_mut(&10).unwrap().unwrap(); // key = 20
            assert_eq!(*next_mut.key(), 20);
            *next_mut.value_mut().unwrap() = "TWENTY_MUTATED".to_string();
        }

        {
            let mut prev_mut = map.prev_entry_mut(&30).unwrap().unwrap(); // key = 20
            assert_eq!(*prev_mut.key(), 20);
            assert_eq!(prev_mut.value(), "TWENTY_MUTATED");
            *prev_mut.value_mut().unwrap() = "TWENTY_MUTATED_AGAIN".to_string();
        }

        let entry_20 = map.get(&20).unwrap().unwrap();
        assert_eq!(entry_20.value(), "TWENTY_MUTATED_AGAIN");
    }

    #[test]
    fn test_clear() {
        let map = setup_test_map();

        assert!(!map.is_empty());
        assert_eq!(map.len(), 3);

        map.clear();

        assert!(map.is_empty());
        assert!(map.first_entry().unwrap().is_none());
        assert!(map.last_entry().unwrap().is_none());
        assert!(!map.contains_key(&10));
        assert!(!map.contains_key(&20));
        assert!(!map.contains_key(&30));
    }

    #[test]
    fn test_iter_with() {
        let map = setup_test_map();

        let mut visited = Vec::new();
        map.iter_with(|entry| {
            visited.push((*entry.key(), entry.value().clone()));
        });

        assert_eq!(
            visited,
            vec![
                (10, "ten".to_string()),
                (20, "twenty".to_string()),
                (30, "thirty".to_string())
            ]
        );
    }

    #[test]
    fn test_iter_mut_with() {
        let map = setup_test_map();

        map.iter_mut_with(|mut entry| {
            let key = *entry.key();
            let val_mut = entry.value_mut().unwrap();
            *val_mut = format!("{}_{}", val_mut, key);
        });

        let mut visited = Vec::new();
        map.iter_with(|entry| {
            visited.push((*entry.key(), entry.value().clone()));
        });

        assert_eq!(
            visited,
            vec![
                (10, "ten_10".to_string()),
                (20, "twenty_20".to_string()),
                (30, "thirty_30".to_string())
            ]
        );
    }

    #[test]
    fn test_entry_exclusion_conflict_on_new_apis() {
        let map = setup_test_map();

        {
            let first_guard = map.first_entry().unwrap().unwrap(); // key 10

            assert!(matches!(map.first_entry_mut(), Err(ElementInUse)));
            assert!(matches!(map.get_mut(&10), Err(ElementInUse)));

            drop(first_guard);
        }

        {
            let first_mut_guard = map.first_entry_mut().unwrap().unwrap(); // key 10

            assert!(matches!(map.first_entry(), Err(ElementInMutUse)));
            assert!(matches!(map.get(&10), Err(ElementInMutUse)));
            assert!(matches!(map.prev_entry(&20), Err(ElementInMutUse)));

            drop(first_mut_guard);
        }
    }
}
