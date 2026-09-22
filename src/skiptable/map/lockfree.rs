//! Not implemented.

use crate::CacheAligned;
use crate::ringbuffer::RingBuffer;
use crate::sync::spinlock::SpinLock;
use rand::Rng;
use rand::rngs::SmallRng;
use std::alloc::{Layout, alloc, handle_alloc_error};
use std::borrow::Borrow;
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence};

static ID_GEN: AtomicU64 = AtomicU64::new(0);

const FALSE: usize = 0;
const TRUE: usize = 1;
const FLAG_DELETED: u32 = 0x1;
const FLAG_UPDATING: u32 = 0x2;

type NodeAtomicPtr = AtomicPtr<u8>;

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
    fn get_epoch(&mut self, table_id: u64, curr_epoch: usize) -> &LocalEpoch {
        if let Some(pos) = self.vec.iter().position(|e| e.table_id == table_id) {
            &mut self.vec[pos]
        } else {
            let epoch = LocalEpoch {
                table_id,
                active: CacheAligned(AtomicUsize::new(FALSE)),
                epoch: CacheAligned(AtomicUsize::new(curr_epoch)),
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

#[inline]
fn ptr2nodeptr(ptr: *mut u8) -> &'static NodeAtomicPtr {
    unsafe { &*(ptr as *mut NodeAtomicPtr) }
}

#[inline]
fn nodeptr2ptr(rf: &NodeAtomicPtr) -> *mut u8 {
    rf as *const NodeAtomicPtr as *mut u8
}

#[inline]
fn ptr_to_fix_node_ref<K, V>(ptr: *mut u8) -> &'static mut FixedNode<K, V> {
    unsafe { &mut *(ptr as *mut FixedNode<K, V>) }
}

#[derive(Debug)]
pub struct SkipTableMap<K, V> {
    epoch: AtomicUsize,
    id: u64,
    max_height: u8,
    hgen: HeightGen,
    next: Vec<NodeAtomicPtr>,
    garbage: [RingBuffer<NodeRef<K, V>>; EPOCH_CNT],
}

//#[repr(C)]
#[derive(Debug)]
#[repr(C)]
struct Node<K, V> {
    height: usize,
    key: K,
    value: V,
    flags: AtomicU32,
    refcnt: AtomicU32,
    next: [NodeAtomicPtr],
}

#[repr(C)]
struct FixedNode<K, V> {
    _height: usize,
    key: K,
    value: V,
    flags: AtomicU32,
    _refcnt: AtomicU32,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: u8) -> *mut u8 {
        let height = height as usize;
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

            std::ptr::addr_of_mut!((*node_fat_ptr).key).write(key);
            std::ptr::addr_of_mut!((*node_fat_ptr).value).write(value);
            std::ptr::addr_of_mut!((*node_fat_ptr).height).write(height);
            std::ptr::addr_of_mut!((*node_fat_ptr).flags).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*node_fat_ptr).refcnt).write(AtomicU32::new(1));
            let slice_ptr = raw_ptr.add(slice_offset) as *mut usize;
            std::ptr::write_bytes(slice_ptr, 0, height);
            raw_ptr
        };
        node_ptr
    }

    fn from_u8_ptr(ptr: *mut u8) -> Box<Self> {
        let height = unsafe { *(ptr as *const usize) };
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ptr, height) as *mut Node<K, V>;
        unsafe { Box::from_raw(fat_ptr) }
    }

    fn get_key_ref(&self) -> &K {
        &self.key
    }

    fn into_value(self: Box<Self>) -> V {
        self.value
    }
}

struct NodeIter<K, V> {
    ptr: NonNull<Node<K, V>>,
}

impl<K, V> NodeIter<K, V> {
    fn from_ptr(ptr: *mut u8) -> Self {
        let height = unsafe { *(ptr as *const usize) };
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ptr, height) as *mut Node<K, V>;
        Self {
            ptr: NonNull::new(fat_ptr).unwrap(),
        }
    }
}

impl<K, V> Deref for NodeIter<K, V> {
    type Target = Node<K, V>;
    fn deref(&self) -> &Node<K, V> {
        unsafe { self.ptr.as_ref() }
    }
}

impl<K, V> DerefMut for NodeIter<K, V> {
    fn deref_mut(&mut self) -> &mut Node<K, V> {
        unsafe { self.ptr.as_mut() }
    }
}

#[derive(Debug)]
struct HeightGen {
    max_height: u8,
}

impl HeightGen {
    fn new(max_height: u8) -> Self {
        Self {
            max_height: max_height.min(32),
        }
    }
    fn gen_height(&self) -> u8 {
        let r = SMALL_RNG.with(|rng| rng.borrow_mut().next_u32());
        ((r.trailing_zeros() + 1) as u8).min(self.max_height)
    }
}

#[derive(Debug)]
pub struct NodeRef<K, V> {
    ptr: NonNull<Node<K, V>>,
}

unsafe impl<K, V> Send for NodeRef<K, V> {}

impl<K, V> NodeRef<K, V> {
    fn from_ptr(ptr: *mut u8) -> Self {
        let height = unsafe { *(ptr as *const usize) };
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(ptr, height) as *mut Node<K, V>;
        unsafe { (*fat_ptr).refcnt.fetch_add(1, Ordering::Relaxed) };
        Self {
            ptr: NonNull::new(fat_ptr).unwrap(),
        }
    }
}

impl<K, V> Deref for NodeRef<K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        unsafe { &self.ptr.as_ref().value }
    }
}

impl<K, V> Drop for NodeRef<K, V> {
    fn drop(&mut self) {
        unsafe {
            let node = self.ptr.as_mut();
            if node.refcnt.fetch_sub(1, Ordering::Release) == 1 {
                fence(Ordering::Acquire);
                drop(Box::from_raw(self.ptr.as_ptr()));
            }
        }
    }
}

impl<K, V> Drop for SkipTableMap<K, V> {
    fn drop(&mut self) {
        get_registry().remove_table_epoch(self.id);
    }
}

impl<K, V> SkipTableMap<K, V>
where
    K: PartialEq + Ord + std::fmt::Debug + 'static,
    V: std::fmt::Debug + 'static,
{
    pub fn new(mut max_height: u8) -> Self {
        if max_height == 0 {
            max_height = 1;
        }
        let mut next = Vec::new();
        for _ in 0..max_height as usize {
            next.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        let table_id = ID_GEN.fetch_add(1, Ordering::AcqRel);
        get_registry().reg_table_epoch(table_id);
        let g0 = RingBuffer::<NodeRef<K, V>>::new(6).unwrap();
        let g1 = RingBuffer::<NodeRef<K, V>>::new(6).unwrap();
        let g2 = RingBuffer::<NodeRef<K, V>>::new(6).unwrap();
        Self {
            epoch: AtomicUsize::new(0),
            id: table_id,
            max_height,
            hgen: HeightGen::new(max_height),
            next,
            garbage: [g0, g1, g2],
        }
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

    fn like_upper_nodes(&self, ins_node: *mut u8, height: usize, key: &K) {
        let mut upper_level = height - 1;
        let max_height = self.max_height as usize;
        let null: *mut u8 = std::ptr::null_mut();
        'whole_loop: loop {
            let mut level = max_height - 1;
            let mut next: *mut u8 = null;

            'level_loop: loop {
                if upper_level == 0 {
                    break 'whole_loop;
                }
                let mut curr_ptr = nodeptr2ptr(&self.next[level]);
                let mut prev_val: *mut u8 = null;
                if next.is_null() {
                    next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                }

                'same_level: loop {
                    if next.is_null() {
                        if level == upper_level {
                            let ok = Self::link_node(curr_ptr, next, ins_node, level);
                            if ok {
                                upper_level -= 1;
                            } else {
                                continue 'whole_loop;
                            }
                        }
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        let node = NodeIter::<K, V>::from_ptr(next);
                        match node.key.cmp(key) {
                            CmpOrdering::Equal => {
                                if level == upper_level {
                                    upper_level -= 1;
                                }
                                level -= 1;
                                continue 'level_loop;
                            }
                            CmpOrdering::Less => {
                                prev_val = next;
                                curr_ptr = nodeptr2ptr(&node.next[level]);
                                next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                                continue 'same_level;
                            }
                            CmpOrdering::Greater => {
                                if level == upper_level {
                                    let ok = Self::link_node(curr_ptr, next, ins_node, level);
                                    if ok {
                                        upper_level -= 1;
                                    } else {
                                        continue 'whole_loop;
                                    }
                                }
                                next = prev_val;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let table_id = self.id;
        let curr_epoch = self.epoch.load(Ordering::Acquire);
        LOCAL_EPOCH.with(|guard| {
            self.insert_inner(
                key,
                value,
                guard.borrow_mut().get_epoch(table_id, curr_epoch),
            )
        })
    }

    fn insert_inner(&self, key: K, value: V, epoch: &LocalEpoch) -> Option<V> {
        let next_offset: usize = std::mem::size_of::<FixedNode<K, V>>();
        let usize_size: usize = std::mem::size_of::<usize>();
        let ins_node = Node::<K, V>::new(key, value, self.hgen.gen_height());
        let ins_box = Node::<K, V>::from_u8_ptr(ins_node);
        let max_height = self.max_height as usize;
        let null: *mut u8 = std::ptr::null_mut();
        'whole_loop: loop {
            let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
            let mut level = max_height - 1;
            let mut next: *mut u8 = null;

            'level_loop: loop {
                let mut curr_ptr = nodeptr2ptr(&self.next[level]);
                let mut prev_val: *mut u8 = null;
                if next.is_null() {
                    next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                }

                'same_level: loop {
                    if next.is_null() {
                        if level == 0 {
                            let ok = Self::link_node(curr_ptr, next, ins_node, level);
                            if ok {
                                let key = ins_box.get_key_ref();
                                self.like_upper_nodes(ins_node, ins_box.next.len(), key);
                                let _ = Box::into_raw(ins_box);
                                break 'whole_loop;
                            }
                            continue 'whole_loop;
                        }
                        if !prev_val.is_null() {
                            next = prev_val;
                        }
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        //let mut node = NodeIter::<K, V>::from_ptr(next);
                        let node = ptr_to_fix_node_ref::<K, V>(next);
                        /*let mut flags = node.flags.load(Ordering::Acquire);
                        if flags & FLAG_DELETED != 0 {
                            continue 'whole_loop;
                        }*/
                        match node.key.cmp(ins_box.get_key_ref()) {
                            CmpOrdering::Equal => loop {
                                let mut flags = node.flags.load(Ordering::Acquire);
                                if flags == 0 {
                                    match node.flags.compare_exchange_weak(
                                        0,
                                        FLAG_UPDATING,
                                        Ordering::Acquire,
                                        Ordering::Relaxed,
                                    ) {
                                        Ok(_) => {
                                            let mut value = ins_box.into_value();
                                            std::mem::swap(&mut node.value, &mut value);
                                            node.flags.store(0, Ordering::Release);
                                            return Some(value);
                                        }
                                        Err(v) => {
                                            flags = v;
                                        }
                                    }
                                }
                                if flags & FLAG_DELETED != 0 {
                                    continue 'whole_loop;
                                } else if flags & FLAG_UPDATING != 0 {
                                    std::hint::spin_loop();
                                }
                            },
                            CmpOrdering::Less => {
                                prev_val = next;
                                curr_ptr = unsafe { next.add(next_offset + usize_size * level) }; //nodeptr2ptr(&node.next[level]);
                                next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                                continue 'same_level;
                            }
                            CmpOrdering::Greater => {
                                if level == 0 {
                                    let ok = Self::link_node(curr_ptr, next, ins_node, level);
                                    if ok {
                                        let key = ins_box.get_key_ref();
                                        self.like_upper_nodes(ins_node, ins_box.next.len(), key);
                                        let _ = Box::into_raw(ins_box);
                                        break 'whole_loop;
                                    }
                                    continue 'whole_loop;
                                }
                                next = prev_val;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        let table_id = self.id;
        let curr_epoch = self.epoch.load(Ordering::Acquire);
        LOCAL_EPOCH.with(|guard| {
            self.remove_inner(key, guard.borrow_mut().get_epoch(table_id, curr_epoch))
        })
    }

    fn remove_inner<Q>(&mut self, key: &Q, epoch: &LocalEpoch) -> Option<V>
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        let max_height = self.max_height as usize;
        let null: *mut u8 = std::ptr::null_mut();
        'whole_loop: loop {
            let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
            let mut level = max_height - 1;
            let mut next: *mut u8 = null;

            'level_loop: loop {
                let mut curr_ptr = nodeptr2ptr(&self.next[level]);
                let mut prev_val: *mut u8 = null;
                if next.is_null() {
                    next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                }

                'same_level: loop {
                    if next.is_null() {
                        if level == 0 {
                            break 'whole_loop;
                        }
                        if !prev_val.is_null() {
                            next = prev_val;
                        }
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        let node = NodeIter::<K, V>::from_ptr(next);
                        let k = node.key.borrow();
                        match k.cmp(key) {
                            CmpOrdering::Equal => loop {
                                let mut flags = node.flags.load(Ordering::Acquire);
                                if flags == 0 {
                                    match node.flags.compare_exchange_weak(
                                        0,
                                        FLAG_DELETED,
                                        Ordering::Acquire,
                                        Ordering::Relaxed,
                                    ) {
                                        Ok(_) => {
                                            return None;
                                        }
                                        Err(v) => {
                                            flags = v;
                                        }
                                    }
                                }
                                if flags & FLAG_DELETED != 0 {
                                    break 'whole_loop;
                                } else if flags & FLAG_UPDATING != 0 {
                                    std::hint::spin_loop();
                                }
                            },
                            CmpOrdering::Less => {
                                prev_val = next;
                                curr_ptr = nodeptr2ptr(&node.next[level]);
                                next = ptr2nodeptr(curr_ptr).load(Ordering::Acquire);
                                continue 'same_level;
                            }
                            CmpOrdering::Greater => {
                                if level == 0 {
                                    break 'whole_loop;
                                }
                                next = prev_val;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn get<Q>(&self, key: &Q) -> Option<NodeRef<K, V>>
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        let table_id = self.id;
        let curr_epoch = self.epoch.load(Ordering::Acquire);
        LOCAL_EPOCH
            .with(|guard| self.get_inner(key, guard.borrow_mut().get_epoch(table_id, curr_epoch)))
    }

    fn get_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> Option<NodeRef<K, V>>
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        'whole_loop: loop {
            let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
            let mut level = self.max_height as usize - 1;
            let mut next: *mut u8 = std::ptr::null_mut();
            'level_loop: loop {
                let mut prev: *mut u8 = std::ptr::null_mut();
                if next.is_null() {
                    next = self.next[level].load(Ordering::Acquire);
                }

                'same_level: loop {
                    if next.is_null() {
                        if level == 0 {
                            break 'whole_loop;
                        }
                        if !prev.is_null() {
                            next = prev;
                        }
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        let node = NodeIter::<K, V>::from_ptr(next);
                        /*let mut flags = node.flags.load(Ordering::Acquire);
                        if flags & FLAG_DELETED != 0 {
                            continue 'whole_loop;
                        }*/
                        let k = node.key.borrow();
                        match k.cmp(key) {
                            CmpOrdering::Equal => loop {
                                let flags = node.flags.load(Ordering::Acquire);
                                if flags == 0 {
                                    return Some(NodeRef::from_ptr(next));
                                } else if flags & FLAG_DELETED != 0 {
                                    break 'whole_loop;
                                } else if flags & FLAG_UPDATING != 0 {
                                    std::hint::spin_loop();
                                }
                            },
                            CmpOrdering::Less => {
                                prev = next;
                                next = node.next[level].load(Ordering::Acquire);
                                continue 'same_level;
                            }
                            CmpOrdering::Greater => {
                                if level == 0 {
                                    break 'whole_loop;
                                }
                                next = prev;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        let mut len = 0usize;
        let mut next = self.next[0].load(Ordering::Acquire);
        while !next.is_null() {
            len += 1;
            next = NodeIter::<K, V>::from_ptr(next).next[0].load(Ordering::Acquire);
        }
        len
    }

    pub fn is_empty(&self) -> bool {
        let next = self.next[0].load(Ordering::Acquire);
        next.is_null()
    }

    pub fn clear(&mut self) {}

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        let table_id = self.id;
        let curr_epoch = self.epoch.load(Ordering::Acquire);
        LOCAL_EPOCH.with(|guard| {
            self.contains_key_inner(key, guard.borrow_mut().get_epoch(table_id, curr_epoch))
        })
    }

    fn contains_key_inner<Q>(&self, key: &Q, epoch: &LocalEpoch) -> bool
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        'whole_loop: loop {
            let _pinner = LocalEpochPinner::new(epoch, self.epoch.load(Ordering::Acquire));
            let mut level = self.max_height as usize - 1;
            let mut next: *mut u8 = std::ptr::null_mut();
            'level_loop: loop {
                let mut prev: *mut u8 = std::ptr::null_mut();
                if next.is_null() {
                    next = self.next[level].load(Ordering::Acquire);
                }

                'same_level: loop {
                    if next.is_null() {
                        if level == 0 {
                            break 'whole_loop;
                        }
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        let node = NodeIter::<K, V>::from_ptr(next);
                        let k = node.key.borrow();
                        match k.cmp(key) {
                            CmpOrdering::Equal => loop {
                                let flags = node.flags.load(Ordering::Acquire);
                                if flags & FLAG_DELETED != 0 {
                                    break 'whole_loop;
                                } else {
                                    return true;
                                }
                            },
                            CmpOrdering::Less => {
                                prev = next;
                                next = node.next[level].load(Ordering::Acquire);
                                continue 'same_level;
                            }
                            CmpOrdering::Greater => {
                                if level == 0 {
                                    break 'whole_loop;
                                }
                                next = prev;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                    }
                }
            }
        }
        false
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
