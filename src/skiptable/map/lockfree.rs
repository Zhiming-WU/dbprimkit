//! A key-value map based on skip table. It's designed to be used in concurrent mode and performance
//! takes high priority. It has narrower APIs comapred to [super::ordinary].
//!
//! EBR (epoch based reclamation) approach is used to safely free objects.
//!

use crate::ringbuffer::RingBuffer;
use crate::sync::spinlock::SpinLock;
use crate::{CacheAligned, Error, Result};
use rand::Rng;
use rand::rngs::SmallRng;
use std::alloc::{Layout, alloc, handle_alloc_error};
use std::borrow::Borrow;
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence};

static ID_GEN: AtomicU64 = AtomicU64::new(0);

const FALSE: usize = 0;
const TRUE: usize = 1;
const FLAG_DELETED: u32 = 0x1;
const FLAG_UPDATING: u32 = 0x2;
const FLAG_IN_USE: u32 = 0x4;
const FLAG_IN_MUT_USE: u32 = 0x8;

/*#[derive(Clone, Copy)]
struct EpochPtr(*const LocalEpoch);
unsafe impl Send for EpochPtr {}
unsafe impl Sync for EpochPtr {}*/

const EPOCH_CNT: usize = 3;

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
}

struct EpochRegistry {
    lock: SpinLock<()>,
    vec: Vec<TableEpoch>,
}

impl EpochRegistry {
    fn get_table_epoch(&mut self, table_id: u64) -> &mut TableEpoch {
        let _l = self.lock.lock();
        self.vec
            .iter_mut()
            .find(|te| te.table_id == table_id)
            .unwrap()
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
    active: CacheAligned<AtomicUsize>,
    epoch: CacheAligned<AtomicUsize>,
}

struct LocalEpochPinner<'a>(&'a LocalEpoch);

impl<'a> LocalEpochPinner<'a> {
    fn new(le: &'a LocalEpoch, curr_epoch: usize) -> Self {
        le.epoch.0.store(curr_epoch, Ordering::Release);
        le.active.0.store(TRUE, Ordering::Release);
        Self(le)
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
    fn get_epoch(&mut self, table_id: u64) -> &LocalEpoch {
        if let Some(pos) = self.vec.iter().position(|e| e.table_id == table_id) {
            &mut self.vec[pos]
        } else {
            let epoch = LocalEpoch {
                table_id,
                active: CacheAligned(AtomicUsize::new(FALSE)),
                epoch: CacheAligned(AtomicUsize::new(0)),
            };
            let mut reg = get_registry();
            let t_epoch = reg.get_table_epoch(table_id);
            t_epoch.register_epoch(&epoch as *const LocalEpoch as usize);
            self.vec.push(epoch);
            self.vec.last().unwrap()
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
        for epoch in &self.epoch_vec.vec {
            let registry = get_registry();
            let _l = registry.lock.lock();
            let mut reg = get_registry();
            for t_epoch in &mut reg.vec {
                if t_epoch.table_id == epoch.table_id {
                    let ptr = epoch as *const LocalEpoch as usize;
                    let _ll = t_epoch.lock.lock();
                    t_epoch.epoch_set.remove(&ptr);
                }
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
type PrecVec = Vec<(*mut NodeAtomicPtr, NodePtr)>;

#[inline]
fn next_addr_to_ptr(addr: *mut NodeAtomicPtr) -> NodePtr {
    unsafe { (*addr).load(Ordering::Acquire) }
}

#[inline]
fn set_next(next_addr: *mut NodeAtomicPtr, next: NodePtr) {
    unsafe { (*next_addr).store(next, Ordering::Release) };
}

#[inline]
fn ptr_to_fix_node_ref<K, V>(ptr: *mut u8) -> &'static mut FixedNode<K, V> {
    unsafe { &mut *(ptr as *mut FixedNode<K, V>) }
}

/// A key-value map based on skip table.
#[derive(Debug)]
pub struct SkipTableMap<K, V> {
    epoch: AtomicUsize,
    id: u64,
    max_height: usize,
    next: Vec<NodeAtomicPtr>,
    garbage: [RingBuffer<EntryRef<K, V>>; EPOCH_CNT],
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
    fn ptr_to_mut<'a>(node_ptr: *mut u8) -> &'a mut Self {
        unsafe { &mut *(node_ptr as *mut Self) }
    }

    #[inline]
    fn get_next_addr(node_ptr: *mut u8, level: usize) -> *mut NodeAtomicPtr {
        let next_ptr = unsafe {
            node_ptr.add(std::mem::size_of::<Self>() + std::mem::size_of::<usize>() * level)
        };
        next_ptr as *mut NodeAtomicPtr
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: usize) -> *mut u8 {
        let slice_layout = Layout::array::<NodeAtomicPtr>(height).unwrap();
        let (node_layout, slice_offset) = Layout::new::<FixedNode<K, V>>()
            .extend(slice_layout)
            .unwrap();
        let node_ptr = unsafe {
            let raw_ptr = alloc(node_layout);
            if raw_ptr.is_null() {
                handle_alloc_error(node_layout);
            };
            let slice_fat_ptr: *mut [NodeAtomicPtr] =
                std::ptr::slice_from_raw_parts_mut(raw_ptr as *mut NodeAtomicPtr, height);
            let node_fat_ptr = slice_fat_ptr as *mut Self;

            std::ptr::addr_of_mut!((*node_fat_ptr).height).write(height);
            std::ptr::addr_of_mut!((*node_fat_ptr).key).write(ManuallyDrop::new(key));
            std::ptr::addr_of_mut!((*node_fat_ptr).value).write(ManuallyDrop::new(value));
            std::ptr::addr_of_mut!((*node_fat_ptr).flags).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*node_fat_ptr).refcnt).write(AtomicU32::new(1));
            let slice_ptr = raw_ptr.add(slice_offset) as *mut usize;
            std::ptr::write_bytes(slice_ptr, 0, height);
            raw_ptr
        };
        node_ptr
    }

    fn from_node_ptr(ptr: NodePtr) -> Box<Self> {
        let height = unsafe { *(ptr as *const usize) };
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ptr, height) as *mut Node<K, V>;
        unsafe { Box::from_raw(fat_ptr) }
    }
}

/// A reference to an entry in the key-value map.
#[derive(Debug)]
pub struct EntryRef<K, V> {
    ptr: NodePtr,
    _m: PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for EntryRef<K, V> {}

impl<K, V> EntryRef<K, V> {
    fn from_ptr(ptr: NodePtr) -> Self {
        let node = unsafe { &*(ptr as *const FixedNode<K, V>) };
        node.refcnt.fetch_add(1, Ordering::Relaxed);
        Self {
            ptr,
            _m: PhantomData,
        }
    }

    fn take_from_ptr(ptr: NodePtr) -> Self {
        Self {
            ptr,
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

    /// Gets a mutable reference to the value in the entry.
    pub fn value_mut(&mut self) -> &mut V {
        &mut self.as_mut().value
    }
}

impl<K, V> Drop for EntryRef<K, V> {
    fn drop(&mut self) {
        let node = unsafe { &*(self.ptr as *const FixedNode<K, V>) };
        if node.refcnt.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            drop(Node::<K, V>::from_node_ptr(self.ptr));
        }
    }
}

impl<K, V> Drop for SkipTableMap<K, V> {
    fn drop(&mut self) {
        get_registry().remove_table_epoch(self.id);
        self.clear();
    }
}

impl<K, V> SkipTableMap<K, V> {
    fn get_next_addr(&self, level: usize) -> *mut NodeAtomicPtr {
        &self.next[level] as *const NodeAtomicPtr as *mut NodeAtomicPtr
    }

    /// Makes a new, empty [SkipTableMap].
    ///
    /// Parameters:
    /// - `max_height`: The maximum height allowed. The allowed range is [0, 32].
    pub fn new(max_height: usize) -> Self {
        let max_height = max_height.min(32).max(1);
        let mut next = Vec::new();
        for _ in 0..max_height {
            next.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        let table_id = ID_GEN.fetch_add(1, Ordering::AcqRel);
        get_registry().reg_table_epoch(table_id);
        let g0 = RingBuffer::<EntryRef<K, V>>::new(6).unwrap();
        let g1 = RingBuffer::<EntryRef<K, V>>::new(6).unwrap();
        let g2 = RingBuffer::<EntryRef<K, V>>::new(6).unwrap();
        Self {
            epoch: AtomicUsize::new(0),
            id: table_id,
            max_height,
            next,
            garbage: [g0, g1, g2],
        }
    }

    fn gen_height(&self) -> usize {
        let r = SMALL_RNG.with(|rng| rng.borrow_mut().next_u32());
        ((r.trailing_zeros() + 1) as usize).min(self.max_height)
    }

    fn search<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Option<EntryRef<K, V>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: NodePtr = std::ptr::null_mut();
        let mut level = self.max_height - 1;
        let mut next: *mut u8 = null;
        let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
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
                            if node.flags.load(Ordering::Acquire) & FLAG_DELETED != 0 {
                                break 'level_loop;
                            }
                            return Some(EntryRef::<K, V>::from_ptr(next));
                        }
                        CmpOrdering::Less => {
                            prev = next;
                            next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(next, level));
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
        None
    }

    fn search_ext<Q>(
        &self,
        key: &Q,
        prec_height: usize,
        epoch: &LocalEpoch,
    ) -> (Option<EntryRef<K, V>>, PrecVec)
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: NodePtr = std::ptr::null_mut();
        let mut level = self.max_height - 1;
        let mut next: *mut u8 = null;
        let mut vec = PrecVec::new();
        let mut res: NodePtr = null;
        let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));

        'level_loop: loop {
            let mut next_addr = self.get_next_addr(level);
            let mut prev: NodePtr = null;
            if next.is_null() {
                next = next_addr_to_ptr(next_addr);
            }

            'same_level: loop {
                if next.is_null() {
                    if level < prec_height {
                        vec.push((next_addr, next));
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
                            if level < prec_height {
                                vec.push((next_addr, next));
                            }
                            if level == 0 {
                                if node.flags.load(Ordering::Acquire) & FLAG_DELETED == 0 {
                                    res = next;
                                }
                                break 'level_loop;
                            } else {
                                next = prev;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                        CmpOrdering::Less => {
                            prev = next;
                            next_addr = FixedNode::<K, V>::get_next_addr(next, level);
                            next = next_addr_to_ptr(next_addr);
                            continue 'same_level;
                        }
                        CmpOrdering::Greater => {
                            if level < prec_height {
                                vec.push((next_addr, next));
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
        if !res.is_null() {
            return (Some(EntryRef::from_ptr(res)), vec);
        }
        (None, vec)
    }

    fn link_node(prev: *mut u8, next: *mut u8, ins_node: *mut u8, level: usize) -> bool {
        let height = unsafe { *(ins_node as *const usize) };
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ins_node, height) as *mut Node<K, V>;
        unsafe {
            (*fat_ptr).next[level].store(next, Ordering::Release);
        }
        let res = ptr2nodeptr(prev).compare_exchange_weak(
            next,
            ins_node,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        res.is_ok()
    }

    fn like_upper_nodes(&self, ins_node: *mut u8, height: usize, key: &K) {}

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, None is returned.
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.insert_inner(key, value, guard.borrow_mut().get_epoch(id)))
    }

    fn insert_inner(&self, key: K, value: V, epoch: &LocalEpoch) -> Option<V>
    where
        K: Ord,
    {
        loop {
            let height = self.gen_height();
            let sch_res = self.search_ext(&key, height, epoch);
            match sch_res.0 {
                Some(entry) => {
                    let node = FixedNode::<K, V>::ptr_to_mut(entry.ptr);
                    let flags = node.flags.load(Ordering::Acquire);
                }
                None => {}
            }
        }
    }

    /// Removes a key from the map, returning the key and value at the key if the key was previously
    /// in the map.
    pub fn remove<Q>(&self, key: &Q) -> Result<Option<(K, V)>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.remove_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn remove_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Result<Option<(K, V)>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let sch_res = self.search_ext(key, self.max_height, epoch);
        match sch_res.0 {
            None => Ok(None),
            Some(entry) => {
                let node = FixedNode::<K, V>::ptr_to_mut(entry.ptr);
                let mut flags = node.flags.load(Ordering::Acquire);
                loop {
                    if flags & FLAG_DELETED != 0 {
                        return Ok(None);
                    } else if flags & (FLAG_IN_USE | FLAG_IN_MUT_USE) != 0 {
                        return Err(Error::ElementInUse);
                    } else if flags & FLAG_UPDATING != 0 {
                        std::hint::spin_loop();
                    } else {
                        let nflags = flags | FLAG_DELETED;
                        match node.flags.compare_exchange_weak(
                            flags,
                            nflags,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                // TODO: unlink nodes and put into garbage and recycle
                                //let key = node.key.t
                            }
                            Err(f) => flags = f,
                        }
                    }
                }
            }
        }
    }

    /// Removes a key from the map, without returning the key and value at the key if the key was
    /// previously in the map.
    pub fn remove_no_kv<Q>(&self, key: &Q) -> Option<()>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.remove_no_kv_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn remove_no_kv_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Option<()>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        None
    }

    /// Returns a reference to the entry corresponding to the key.
    pub fn get<Q>(&self, key: &Q) -> Option<EntryRef<K, V>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.get_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn get_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Option<EntryRef<K, V>>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        match self.search(key, epoch) {
            None => None,
            Some(entry) => {
                let node = FixedNode::<K, V>::ptr_to_mut(entry.ptr);
                loop {
                    let flags = node.flags.load(Ordering::Acquire);
                    if flags & FLAG_UPDATING != 0 {
                        std::hint::spin_loop();
                    } else if flags & FLAG_DELETED != 0 {
                        return None;
                    } else {
                        return Some(entry);
                    }
                }
            }
        }
    }

    /// Returns a clone of the value in the entry corresponding to the key.
    pub fn get_by_clone<Q>(&self, key: &Q) -> Option<V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
        V: Clone,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.get_by_clone_inner(key, guard.borrow_mut().get_epoch(id)))
    }

    fn get_by_clone_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Option<V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
        V: Clone,
    {
        match self.search(key, epoch) {
            None => None,
            Some(entry) => {
                let node = FixedNode::<K, V>::ptr_to_mut(entry.ptr);
                let mut flags = node.flags.load(Ordering::Acquire);
                loop {
                    if flags & FLAG_UPDATING != 0 {
                        std::hint::spin_loop();
                    } else if flags & FLAG_DELETED != 0 {
                        return None;
                    } else {
                        let nflags = flags | FLAG_IN_USE;
                        match node.flags.compare_exchange_weak(
                            flags,
                            nflags,
                            Ordering::Release,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                let value = node.value.deref().clone();
                                node.flags.store(flags, Ordering::Release);
                                return Some(value);
                            }
                            Err(f) => {
                                flags = f;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.len_inner(guard.borrow_mut().get_epoch(id)))
    }

    fn len_inner(&self, epoch: &LocalEpoch) -> usize {
        let mut len = 0usize;
        let _pin = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
        let mut next = self.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            len += 1;
            next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(next, 0));
        }
        len
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.next[0].load(Ordering::Acquire).is_null()
    }

    /// Clears the map, removing all elements.
    pub fn clear(&mut self) {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.clear_inner(guard.borrow_mut().get_epoch(id)))
    }

    fn clear_inner(&mut self, epoch: &LocalEpoch) {
        // let mut next = self.next[0].load(Ordering::Acquire);
        // let _pin = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
    }

    /// Returns `true if the map contains a value for the specified key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let id = self.id;
        LOCAL_EPOCH.with(|guard| self.search(key, guard.borrow_mut().get_epoch(id)).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::{collections::BTreeSet, time::Instant};

    #[test]
    fn check_nodes1() {
        let table = SkipTableMap::<u64, u64>::new(24);
        let mut rng: SmallRng = rand::make_rng();

        let max = 2usize.pow(24);
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
            table.insert(num, num + 10);
        }
        /*println!("table={:?}", table);
        let mut next = table.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = NodeIter::<u64, u64>::from_ptr(next);
            println!("node, ptr={:?}, node={:?}", next, node.deref());
            next = node.next[0].load(Ordering::Relaxed);
        }*/
        println!("wduration={:?}", start_time.elapsed());
        let start_time = Instant::now();
        for num in &vec {
            let v = table.get(num).unwrap();
            assert_eq!(*v.deref(), *num + 10);
        }
        println!("rduration={:?}", start_time.elapsed());
    }

    #[test]
    fn check_nodes2() {
        let table = SkipTableMap::<u64, u64>::new(26);
        let mut rng: SmallRng = rand::make_rng();

        let max = 2usize.pow(26);
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

        let thr_num = 32usize;
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
                        table_ref.insert(num, num + 10);
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
                        let v = table_ref.get(num).unwrap();
                        assert_eq!(*v.deref(), *num + 10);
                    }
                });
            }
        });
        println!("rduration={:?}", start_time.elapsed());

        let mut prev = u64::MAX;
        let mut next = table.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            let node = NodeIter::<u64, u64>::from_ptr(next);
            if prev != u64::MAX {
                assert!(prev < node.key);
            }
            //println!("node={:?}", node.deref());
            prev = node.key;
            next = node.next[0].load(Ordering::Relaxed);
        }
    }

    #[test]
    fn test_try_log_random() {}
}
