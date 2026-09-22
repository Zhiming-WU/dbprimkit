//! A key-value map based on skip table, with APIs similar to [std::collections::BTreeMap].
//! It's designed to used in non-concurrent mode and the user needs to use synchronization
//! utilities (i.g. locks) to use it in concurrent mode.
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::skiptable::map::ordinary::SkipTableMap;
//!
//! fn main() {
//!     let mut map = SkipTableMap::<i32, String>::new(16);
//!
//!     assert!(map.is_empty());
//!     assert_eq!(map.len(), 0);
//!
//!     assert_eq!(map.insert(1, "one".to_string()), None);
//!     assert_eq!(map.insert(2, "two".to_string()), None);
//!     assert_eq!(map.insert(3, "three".to_string()), None);
//!
//!     assert_eq!(map.len(), 3);
//!     assert!(!map.is_empty());
//!
//!     assert_eq!(map.insert(2, "TWO".to_string()), Some("two".to_string()));
//!
//!     assert_eq!(map.get(&1), Some(&"one".to_string()));
//!     assert_eq!(map.get(&2), Some(&"TWO".to_string()));
//!     assert_eq!(map.get(&4), None);
//!     assert!(map.contains_key(&3));
//!     assert!(!map.contains_key(&4));
//!
//!     if let Some(v) = map.get_mut(&3) {
//!         *v = "THREE".to_string();
//!     }
//!     assert_eq!(map.get(&3), Some(&"THREE".to_string()));
//!
//!     assert_eq!(map.remove(&2), Some("TWO".to_string()));
//!     assert_eq!(map.remove(&2), None);
//!     assert_eq!(map.len(), 2);
//!
//!     assert_eq!(map.remove_entry(&1), Some((1, "one".to_string())));
//!     assert_eq!(map.len(), 1);
//!
//!     map.clear();
//!     assert_eq!(map.len(), 0);
//!     assert!(map.is_empty());
//!     assert_eq!(map.get(&3), None);
//! }
//! ```
use rand::Rng;
use rand::rngs::SmallRng;
use std::alloc::{Layout, alloc, handle_alloc_error};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

type NodePtr = *mut u8;
type PrecVec = Vec<(*mut NodePtr, NodePtr)>;

#[inline]
fn next_addr_to_ptr(addr: *mut NodePtr) -> NodePtr {
    unsafe { *addr }
}

#[inline]
fn set_next(next_addr: *mut NodePtr, next: NodePtr) {
    unsafe { *next_addr = next }
}

/// A key-value map based on skip table.
#[derive(Debug)]
#[repr(C)]
pub struct SkipTableMap<K, V> {
    max_height: usize,
    len: usize,
    hgen: HeightGen,
    next: Vec<NodePtr>,
    _m: PhantomData<(K, V)>,
}

unsafe impl<K: Send, V: Send> Send for SkipTableMap<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for SkipTableMap<K, V> {}

#[derive(Debug)]
#[repr(C)]
struct Node<K, V> {
    height: usize,
    key: K,
    value: V,
    next: [NodePtr],
}

#[derive(Debug)]
#[repr(C)]
struct FixedNode<K, V> {
    height: usize,
    key: K,
    value: V,
}

impl<K, V> FixedNode<K, V> {
    #[inline]
    fn ptr_to_mut<'a>(node_ptr: *mut u8) -> &'a mut Self {
        unsafe { &mut *(node_ptr as *mut Self) }
    }

    #[inline]
    fn get_next_addr(node_ptr: *mut u8, level: usize) -> *mut NodePtr {
        let next_ptr = unsafe {
            node_ptr.add(std::mem::size_of::<Self>() + std::mem::size_of::<usize>() * level)
        };
        next_ptr as *mut NodePtr
    }

    #[inline]
    // return (whether_meets_bound, whether_meets_by_key_equal)
    fn meets_start_bound<Q>(&self, bound: &Bound<&Q>) -> (bool, bool)
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let k = self.key.borrow();
        match bound {
            Bound::Unbounded => (true, false),
            Bound::Excluded(start) => match (*start).cmp(k) {
                Ordering::Greater => (false, false),
                Ordering::Equal => (false, false),
                Ordering::Less => (true, false),
            },
            Bound::Included(start) => match (*start).cmp(k) {
                Ordering::Greater => (false, false),
                Ordering::Equal => (true, true),
                Ordering::Less => (true, false),
            },
        }
    }

    #[inline]
    // return (whether_meets_bound, whether_meets_by_key_equal)
    fn meets_end_bound<Q>(&self, bound: &Bound<&Q>) -> (bool, bool)
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let k = self.key.borrow();
        match bound {
            Bound::Unbounded => (true, false),
            Bound::Excluded(end) => match k.cmp(*end) {
                Ordering::Greater => (false, false),
                Ordering::Equal => (false, false),
                Ordering::Less => (true, false),
            },
            Bound::Included(end) => match k.cmp(*end) {
                Ordering::Greater => (false, false),
                Ordering::Equal => (true, true),
                Ordering::Less => (true, false),
            },
        }
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: usize) -> NodePtr {
        let slice_layout = Layout::array::<NodePtr>(height).unwrap();
        let (node_layout, slice_offset) = Layout::new::<FixedNode<K, V>>()
            .extend(slice_layout)
            .unwrap();
        let node_ptr = unsafe {
            let raw_ptr = alloc(node_layout);
            if raw_ptr.is_null() {
                handle_alloc_error(node_layout);
            };
            let slice_fat_ptr: *mut [NodePtr] =
                std::ptr::slice_from_raw_parts_mut(raw_ptr as *mut NodePtr, height);
            let node_fat_ptr = slice_fat_ptr as *mut Self;

            std::ptr::addr_of_mut!((*node_fat_ptr).height).write(height);
            std::ptr::addr_of_mut!((*node_fat_ptr).key).write(key);
            std::ptr::addr_of_mut!((*node_fat_ptr).value).write(value);
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

#[derive(Debug)]
struct HeightGen {
    max_height: usize,
    rng: SmallRng,
}

impl HeightGen {
    fn new(max_height: usize) -> Self {
        Self {
            max_height,
            rng: rand::make_rng(),
        }
    }
    fn gen_height(&mut self) -> usize {
        let r = self.rng.next_u32();
        ((r.trailing_zeros() + 1) as usize).min(self.max_height)
    }
}

impl<K, V> Drop for SkipTableMap<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<K, V> SkipTableMap<K, V> {
    fn get_next_addr(&self, level: usize) -> *mut NodePtr {
        &self.next[level] as *const NodePtr as *mut NodePtr
    }

    /// Makes a new, empty [SkipTableMap].
    ///
    /// Parameters:
    /// - `max_height`: The maximum height allowed. The allowed range is [0, 32].
    pub fn new(max_height: usize) -> Self {
        let max_height = max_height.min(32).max(1);
        let mut next = Vec::with_capacity(max_height);
        for _ in 0..max_height as usize {
            next.push(std::ptr::null_mut());
        }
        Self {
            max_height,
            len: 0,
            hgen: HeightGen::new(max_height),
            next,
            _m: PhantomData,
        }
    }

    fn like_nodes(vec: &PrecVec, ins_node: NodePtr, height: usize) {
        let mut level = height;
        let start = vec.len() - height;
        for &(prec, next) in &vec[start..] {
            level -= 1;
            set_next(FixedNode::<K, V>::get_next_addr(ins_node, level), next);
            set_next(prec, ins_node);
        }
    }

    fn search<Q>(&self, key: &Q) -> NodePtr
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: NodePtr = std::ptr::null_mut();
        let mut level = self.max_height as usize - 1;
        let mut next: NodePtr = null;

        'level_loop: loop {
            let mut prev_val: NodePtr = null;
            if next.is_null() {
                next = next_addr_to_ptr(self.get_next_addr(level));
            }

            'same_level: loop {
                if next.is_null() {
                    if level == 0 {
                        break 'level_loop;
                    }
                    if !prev_val.is_null() {
                        next = prev_val;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    let node = FixedNode::<K, V>::ptr_to_mut(next);
                    let k = node.key.borrow();
                    match k.cmp(key) {
                        Ordering::Equal => {
                            return next;
                        }
                        Ordering::Less => {
                            prev_val = next;
                            next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(next, level));
                            continue 'same_level;
                        }
                        Ordering::Greater => {
                            if level == 0 {
                                break 'level_loop;
                            }
                            next = prev_val;
                            level -= 1;
                            continue 'level_loop;
                        }
                    }
                }
            }
        }
        null
    }

    fn search_ext<Q>(&self, key: &Q, prec_height: usize) -> (NodePtr, PrecVec)
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: *mut u8 = std::ptr::null_mut();
        let mut level = self.max_height as usize - 1;
        let mut vec = PrecVec::new();
        let mut next: NodePtr = null;
        let mut res: NodePtr = null;

        'level_loop: loop {
            let mut next_addr = self.get_next_addr(level);
            let mut prev_val: *mut u8 = null;
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
                    if !prev_val.is_null() {
                        next = prev_val;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    let node = FixedNode::<K, V>::ptr_to_mut(next);
                    let k = node.key.borrow();
                    match k.cmp(key) {
                        Ordering::Equal => {
                            if level < prec_height {
                                vec.push((next_addr, next));
                            }
                            if level == 0 {
                                res = next;
                                break 'level_loop;
                            } else {
                                next = prev_val;
                                level -= 1;
                                continue 'level_loop;
                            }
                        }
                        Ordering::Less => {
                            prev_val = next;
                            next_addr = FixedNode::<K, V>::get_next_addr(next, level);
                            next = next_addr_to_ptr(next_addr);
                            continue 'same_level;
                        }
                        Ordering::Greater => {
                            if level < prec_height {
                                vec.push((next_addr, next));
                            }
                            if level == 0 {
                                break 'level_loop;
                            }
                            next = prev_val;
                            level -= 1;
                            continue 'level_loop;
                        }
                    }
                }
            }
        }
        (res, vec)
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, None is returned.
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, key: K, mut value: V) -> Option<V>
    where
        K: Ord,
    {
        let this_height = self.hgen.gen_height();
        let sch_res = self.search_ext(&key, this_height);
        if !sch_res.0.is_null() {
            let node = FixedNode::<K, V>::ptr_to_mut(sch_res.0);
            std::mem::swap(&mut node.value, &mut value);
            Some(value)
        } else {
            let ins_node = Node::<K, V>::new(key, value, this_height);
            Self::like_nodes(&sch_res.1, ins_node, this_height);
            self.len += 1;
            None
        }
    }

    fn unlike_nodes(vec: &Vec<(*mut NodePtr, NodePtr)>, height: usize) {
        let mut level = height;
        let start = vec.len() - height;
        for &(prec, next) in &vec[start..] {
            level -= 1;
            let new_next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(next, level));
            set_next(prec, new_next);
        }
    }

    /// Removes a key from the map, returning the value at the key if the key was previously in the map.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        match self.remove_entry(key) {
            Some((_, v)) => Some(v),
            None => None,
        }
    }

    /// Removes a key from the map, returning the stored key and value if the key was previously in the map.
    pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let sch_res = self.search_ext(&key, self.max_height);
        if !sch_res.0.is_null() {
            let node_box = Node::<K, V>::from_node_ptr(sch_res.0);
            Self::unlike_nodes(&sch_res.1, node_box.height);
            self.len -= 1;
            return Some((node_box.key, node_box.value));
        }
        None
    }

    fn get_node_mut<'a>(&'a self, ptr: NodePtr) -> &'a mut FixedNode<K, V> {
        FixedNode::<K, V>::ptr_to_mut(ptr)
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        self.get_key_value(key).map(|kv| kv.1)
    }

    /// Returns a mutable reference to the value corresponding to the key.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let ptr = self.search(key);
        if !ptr.is_null() {
            let node = self.get_node_mut(ptr);
            return Some(&mut node.value);
        }
        None
    }

    ///Returns the key-value pair corresponding to the supplied key.
    pub fn get_key_value<Q>(&self, key: &Q) -> Option<(&K, &V)>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let ptr = self.search(key);
        if !ptr.is_null() {
            let node = self.get_node_mut(ptr);
            return Some((&node.key, &node.value));
        }
        None
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clears the map, removing all elements.
    pub fn clear(&mut self) {
        let null = std::ptr::null_mut();
        let mut node_ptr = self.next[0];
        for level in 0..self.max_height as usize {
            let next_addr = self.get_next_addr(level);
            set_next(next_addr, null);
        }
        while !node_ptr.is_null() {
            let next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(node_ptr, 0));
            let _box = Node::<K, V>::from_node_ptr(node_ptr);
            node_ptr = next;
        }
        self.len = 0;
    }

    /// Returns true if the map contains a value for the specified key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let ptr = self.search(key);
        !ptr.is_null()
    }

    /// Gets the given key’s corresponding entry in the map for in-place manipulation.
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V>
    where
        K: Ord,
    {
        let sch_res = self.search_ext(&key, self.max_height);
        if sch_res.0.is_null() {
            Entry::Vacant(VacantEntry {
                map: self,
                key,
                vec: sch_res.1,
            })
        } else {
            Entry::Occupied(OccupiedEntry {
                map: self,
                ptr: sch_res.0,
                vec: sch_res.1,
            })
        }
    }

    /// Gets an iterator over the keys of the map, in sorted order.
    pub fn keys(&self) -> Keys<'_, K, V> {
        Keys { inner: self.iter() }
    }

    /// Gets an iterator over the values of the map, in order by key.
    pub fn values(&self) -> Values<'_, K, V> {
        Values { inner: self.iter() }
    }

    /// Gets a mutable iterator over the values of the map, in order by key.
    pub fn values_mut(&mut self) -> ValuesMut<'_, K, V> {
        ValuesMut {
            inner: self.iter_mut(),
        }
    }

    /// Gets an iterator over the entries of the map, sorted by key.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            _m: PhantomData,
            ptr: self.next[0],
        }
    }

    /// Gets a mutable iterator over the entries of the map, sorted by key.
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut {
            _m: PhantomData,
            ptr: self.next[0],
        }
    }

    /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the map.
    pub fn first_key_value(&self) -> Option<(&K, &V)>
    where
        K: Ord,
    {
        let ptr = self.next[0];
        if ptr.is_null() {
            return None;
        }
        let node = self.get_node_mut(ptr);
        Some((&node.key, &node.value))
    }

    /// Returns the first entry in the map for in-place manipulation. The key of this entry is
    /// the minimum key in the map.
    pub fn first_entry(&mut self) -> Option<OccupiedEntry<'_, K, V>>
    where
        K: Ord,
    {
        let ptr = self.next[0];
        if ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(ptr);
        let mut vec = PrecVec::with_capacity(node.height);
        let mut level = node.height;
        while level > 0 {
            level -= 1;
            if self.next[level] == ptr {
                vec.push((self.get_next_addr(level), ptr));
            }
        }
        Some(OccupiedEntry {
            map: self,
            ptr,
            vec,
        })
    }

    /// Removes and returns the first element in the map. The key of this element is the minimum key
    /// that was in the map.
    pub fn pop_first(&mut self) -> Option<(K, V)>
    where
        K: Ord,
    {
        self.first_entry().map(|entry| entry.remove_entry())
    }

    fn search_last(&self) -> NodePtr {
        let null: *mut u8 = std::ptr::null_mut();
        let mut level = self.max_height as usize - 1;
        let mut next: NodePtr = null;
        let mut res: NodePtr = null;

        'level_loop: loop {
            if next.is_null() {
                next = next_addr_to_ptr(self.get_next_addr(level));
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
                    let tmp_next = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(next, level));
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

    /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the map.
    pub fn last_key_value(&self) -> Option<(&K, &V)>
    where
        K: Ord,
    {
        let ptr = self.search_last();
        if ptr.is_null() {
            return None;
        }
        let node = self.get_node_mut(ptr);
        Some((&node.key, &node.value))
    }

    /// Returns the last entry in the map for in-place manipulation. The key of this entry is
    /// the maximum key in the map.
    pub fn last_entry(&mut self) -> Option<OccupiedEntry<'_, K, V>>
    where
        K: Ord,
    {
        let ptr = self.search_last();
        if ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(ptr);
        let sch_res = self.search_ext(&node.key, node.height);
        Some(OccupiedEntry {
            map: self,
            ptr,
            vec: sch_res.1,
        })
    }

    /// Removes and returns the last element in the map. The key of this element is the maximum key that was in the map.
    pub fn pop_last(&mut self) -> Option<(K, V)>
    where
        K: Ord,
    {
        self.last_entry().map(|entry| entry.remove_entry())
    }

    fn search_first_with_bounds<Q>(&self, start: Bound<&Q>, end: Bound<&Q>) -> NodePtr
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
    {
        let null: *mut u8 = std::ptr::null_mut();
        let mut level = self.max_height as usize - 1;
        let mut next: NodePtr = null;
        let mut res: NodePtr = null;

        'level_loop: loop {
            let mut next_addr = self.get_next_addr(level);
            let mut prev_val: *mut u8 = null;
            if next.is_null() {
                next = next_addr_to_ptr(next_addr);
            }

            'same_level: loop {
                if next.is_null() {
                    if level == 0 {
                        break 'level_loop;
                    }
                    level -= 1;
                    continue 'level_loop;
                } else {
                    let node = FixedNode::<K, V>::ptr_to_mut(next);
                    let (meets, by_equal) = node.meets_start_bound(&start);
                    if meets {
                        res = next;
                        if level == 0 || by_equal {
                            break 'level_loop;
                        }
                        next = prev_val;
                        level -= 1;
                        continue 'level_loop;
                    } else {
                        prev_val = next;
                        next_addr = FixedNode::<K, V>::get_next_addr(next, level);
                        next = next_addr_to_ptr(next_addr);
                        continue 'same_level;
                    }
                }
            }
        }
        if !res.is_null() {
            let node = FixedNode::<K, V>::ptr_to_mut(res);
            if node.meets_end_bound(&end).0 {
                return res;
            }
        }
        null
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the map.
    pub fn range<Q, R>(&self, range: R) -> Range<'_, K, V, Q, R>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
        R: RangeBounds<Q>,
    {
        let start = range.start_bound();
        let end = range.end_bound();
        let ptr = self.search_first_with_bounds(start, end);
        Range {
            _m: PhantomData,
            ptr,
            bounds: range,
        }
    }

    /// Constructs a mutable double-ended iterator over a sub-range of elements in the map.
    pub fn range_mut<Q, R>(&mut self, range: R) -> RangeMut<'_, K, V, Q, R>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q> + Ord,
        R: RangeBounds<Q>,
    {
        let start = range.start_bound();
        let end = range.end_bound();
        let ptr = self.search_first_with_bounds(start, end);
        RangeMut {
            _m: PhantomData,
            ptr,
            bounds: range,
        }
    }
}

/// A view into a vacant entry in a [SkipTableMap]. It is part of the [Entry] enum.
pub struct VacantEntry<'a, K, V> {
    map: &'a mut SkipTableMap<K, V>,
    key: K,
    vec: Vec<(*mut NodePtr, NodePtr)>,
}

impl<'a, K: Ord, V> VacantEntry<'a, K, V> {
    /// Gets a reference to the key that would be used when inserting a value through the [VacantEntry].
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Take ownership of the key.
    pub fn into_key(self) -> K {
        self.key
    }

    /// Sets the value of the entry with the [VacantEntry]’s key, and returns a mutable reference to it.
    pub fn insert(self, value: V) -> &'a mut V {
        let entry = self.insert_entry(value);
        entry.into_mut()
    }

    /// Sets the value of the entry with the [VacantEntry]’s key, and returns an [OccupiedEntry].
    pub fn insert_entry(self, value: V) -> OccupiedEntry<'a, K, V> {
        let height = self.map.hgen.gen_height();
        let node = Node::<K, V>::new(self.key, value, height);
        SkipTableMap::<K, V>::like_nodes(&self.vec, node, height);
        self.map.len += 1;
        OccupiedEntry {
            map: self.map,
            ptr: node,
            vec: self.vec,
        }
    }
}

/// A view into an occupied entry in a [SkipTableMap]. It is part of the [Entry] enum.
pub struct OccupiedEntry<'a, K, V> {
    map: &'a mut SkipTableMap<K, V>,
    ptr: NodePtr,
    vec: Vec<(*mut NodePtr, NodePtr)>,
}

impl<'a, K, V> OccupiedEntry<'a, K, V> {
    /// Gets a reference to the key in the entry.
    pub fn key(&self) -> &K {
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        &node.key
    }

    /// Take ownership of the key and value from the map.
    pub fn remove_entry(self) -> (K, V) {
        let node = Node::<K, V>::from_node_ptr(self.ptr);
        SkipTableMap::<K, V>::unlike_nodes(&self.vec, node.height);
        self.map.len -= 1;
        (node.key, node.value)
    }

    /// Gets a reference to the value in the entry.
    pub fn get(&self) -> &V {
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        &node.value
    }

    /// Gets a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut V {
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        &mut node.value
    }

    /// Converts the entry into a mutable reference to its value.
    pub fn into_mut(self) -> &'a mut V {
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        &mut node.value
    }

    /// Sets the value of the entry with the [OccupiedEntry]’s key, and returns the entry’s old value.
    pub fn insert(&mut self, mut value: V) -> V {
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        std::mem::swap(&mut node.value, &mut value);
        value
    }

    /// Takes the value of the entry out of the map, and returns it.
    pub fn remove(self) -> V {
        self.remove_entry().1
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
/// This struct is created by [SkipTableMap::entry].
pub enum Entry<'a, K, V> {
    /// A vacant entry.
    Vacant(VacantEntry<'a, K, V>),
    /// An occupied entry.
    Occupied(OccupiedEntry<'a, K, V>),
}

impl<'a, K: Ord, V> Entry<'a, K, V> {
    /// Ensures a value is in the entry by inserting the default if empty, and returns a mutable
    /// reference to the value in the entry.
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Self::Vacant(ve) => ve.insert(default),
            Self::Occupied(oe) => oe.into_mut(),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the default function if empty,
    /// and returns a mutable reference to the value in the entry.
    pub fn or_insert_with<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce() -> V,
    {
        match self {
            Self::Occupied(oe) => oe.into_mut(),
            Self::Vacant(ve) => ve.insert(default()),
        }
    }

    /// Ensures a value is in the entry by inserting, if empty, the result of the default function.
    pub fn or_insert_with_key<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce(&K) -> V,
    {
        match self {
            Self::Occupied(oe) => oe.into_mut(),
            Self::Vacant(ve) => {
                let value = default(ve.key());
                ve.insert(value)
            }
        }
    }

    /// Returns a reference to this entry’s key.
    pub fn key(&self) -> &K {
        match self {
            Self::Vacant(ve) => ve.key(),
            Self::Occupied(oe) => oe.key(),
        }
    }

    /// Provides in-place mutable access to an occupied entry before any potential inserts into the map.
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Self::Occupied(mut oe) => {
                f(oe.get_mut());
                Self::Occupied(oe)
            }
            Self::Vacant(ve) => Self::Vacant(ve),
        }
    }

    /// Sets the value of the entry, and returns an [OccupiedEntry].
    pub fn insert_entry(self, value: V) -> OccupiedEntry<'a, K, V> {
        match self {
            Self::Occupied(mut oe) => {
                oe.insert(value);
                oe
            }
            Self::Vacant(ve) => ve.insert_entry(value),
        }
    }
}

impl<'a, K: Ord, V: Default> Entry<'a, K, V> {
    /// Ensures a value is in the entry by inserting the default value if empty, and returns
    /// a mutable reference to the value in the entry.
    pub fn or_default(self) -> &'a mut V {
        match self {
            Self::Vacant(ve) => ve.insert(V::default()),
            Self::Occupied(oe) => oe.into_mut(),
        }
    }
}

/// An iterator over the keys of a [SkipTableMap].
/// This struct is created by [SkipTableMap::keys].
pub struct Keys<'a, K, V> {
    inner: Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for Keys<'a, K, V> {
    type Item = &'a K;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|n| n.0)
    }
}

/// An iterator over the values of a [SkipTableMap].
/// This struct is created by [SkipTableMap::values].
pub struct Values<'a, K, V> {
    inner: Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|n| n.1)
    }
}

/// A mutable iterator over the values of a [SkipTableMap].
/// This struct is created by [SkipTableMap::values_mut].
pub struct ValuesMut<'a, K, V> {
    inner: IterMut<'a, K, V>,
}

impl<'a, K, V> Iterator for ValuesMut<'a, K, V> {
    type Item = &'a mut V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|n| n.1)
    }
}

/// An iterator over the entries of a [SkipTableMap].
/// This struct is created by [SkipTableMap::iter].
pub struct Iter<'a, K: 'a, V: 'a> {
    _m: PhantomData<&'a (K, V)>,
    ptr: NodePtr,
}

impl<'a, K: 'a, V: 'a> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        self.ptr = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(self.ptr, 0));
        Some((&node.key, &node.value))
    }
}

/// A mutable iterator over the entries of a [SkipTableMap].
/// This struct is created by [SkipTableMap::iter_mut].
pub struct IterMut<'a, K: 'a, V: 'a> {
    _m: PhantomData<&'a (K, V)>,
    ptr: NodePtr,
}

impl<'a, K: 'a, V: 'a> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        self.ptr = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(self.ptr, 0));
        Some((&node.key, &mut node.value))
    }
}

/// An iterator over a sub-range of entries in a [SkipTableMap].
/// This struct is created by [SkipTableMap::range].
pub struct Range<'a, K: 'a, V: 'a, Q, R>
where
    Q: Ord + ?Sized,
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    _m: PhantomData<&'a (K, V, Q)>,
    ptr: NodePtr,
    bounds: R,
}

impl<'a, K: 'a, V: 'a, Q, R> Iterator for Range<'a, K, V, Q, R>
where
    Q: Ord + ?Sized,
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        if !node.meets_end_bound(&self.bounds.end_bound()).0 {
            return None;
        }
        self.ptr = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(self.ptr, 0));
        Some((&node.key, &node.value))
    }
}

/// A mutable iterator over a sub-range of entries in a [SkipTableMap].
/// This struct is created by [SkipTableMap::range_mut].
pub struct RangeMut<'a, K: 'a, V: 'a, Q, R>
where
    Q: Ord + ?Sized,
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    _m: PhantomData<&'a (K, V, Q)>,
    ptr: NodePtr,
    bounds: R,
}

impl<'a, K: 'a, V: 'a, Q, R> Iterator for RangeMut<'a, K, V, Q, R>
where
    Q: Ord + ?Sized,
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    type Item = (&'a K, &'a mut V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr.is_null() {
            return None;
        }
        let node = FixedNode::<K, V>::ptr_to_mut(self.ptr);
        if !node.meets_end_bound(&self.bounds.end_bound()).0 {
            return None;
        }
        self.ptr = next_addr_to_ptr(FixedNode::<K, V>::get_next_addr(self.ptr, 0));
        Some((&node.key, &mut node.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc_tests::ALLOC;
    use rand::Rng;
    //use std::sync::atomic::Ordering;
    use std::{collections::BTreeSet, time::Instant};

    #[test]
    fn test_check_nodes() {
        let mut table = SkipTableMap::<u64, u64>::new(3);
        let mut rng: SmallRng = rand::make_rng();

        let max = 32usize;
        let mut vec = Vec::<u64>::with_capacity(max);
        while vec.len() < max {
            let num = rng.next_u64();
            if !vec.contains(&num) {
                vec.push(num);
            }
        }

        for idx in 0..max {
            let num = vec[idx];
            table.insert(num, num + 10);
        }

        println!("table={:?}", table);
        let mut next = table.next[0];
        while !next.is_null() {
            let node = Node::<u64, u64>::from_node_ptr(next);
            println!("node, ptr={:?}, node={:?}", next, node);
            next = node.next[0];
            let _ = Box::into_raw(node);
        }

        for num in &vec {
            let v = table.get(num).unwrap();
            assert_eq!(*v, *num + 10);
        }
        for num in &vec {
            let v = table.remove(num).unwrap();
            assert_eq!(v, *num + 10);
        }
    }

    #[test]
    #[ignore]
    fn test_perf() {
        let mut rng: SmallRng = rand::make_rng();
        // let obytes = ALLOC.bytes.load(Ordering::Acquire);
        // let ocount = ALLOC.count.load(Ordering::Acquire);
        {
            let mut table = SkipTableMap::<u64, u64>::new(24);

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
            println!("wduration={:?}, len={}", start_time.elapsed(), table.len());
            let last = u64::MAX;
            let mut next = table.next[0];
            while !next.is_null() {
                let node = Node::<u64, u64>::from_node_ptr(next);
                if last != u64::MAX {
                    assert!(last < node.key);
                }
                next = node.next[0];
                let _ = Box::into_raw(node);
            }
            let start_time = Instant::now();
            for num in &vec {
                let v = table.get(num).unwrap();
                assert_eq!(*v, *num + 10);
            }
            println!("rduration={:?}", start_time.elapsed());
            let start_time = Instant::now();
            for num in &vec {
                let v = table.remove(num).unwrap();
                assert_eq!(v, *num + 10);
            }
            println!("dduration={:?}, len={}", start_time.elapsed(), table.len());
            assert!(table.is_empty());
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
    fn test_basic_crud() {
        let mut map = SkipTableMap::<i32, String>::new(16);

        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        assert_eq!(map.insert(1, "one".to_string()), None);
        assert_eq!(map.insert(2, "two".to_string()), None);
        assert_eq!(map.insert(3, "three".to_string()), None);

        assert_eq!(map.len(), 3);
        assert!(!map.is_empty());

        assert_eq!(map.insert(2, "TWO".to_string()), Some("two".to_string()));

        assert_eq!(map.get(&1), Some(&"one".to_string()));
        assert_eq!(map.get(&2), Some(&"TWO".to_string()));
        assert_eq!(map.get(&4), None);
        assert!(map.contains_key(&3));
        assert!(!map.contains_key(&4));

        if let Some(v) = map.get_mut(&3) {
            *v = "THREE".to_string();
        }
        assert_eq!(map.get(&3), Some(&"THREE".to_string()));

        assert_eq!(map.remove(&2), Some("TWO".to_string()));
        assert_eq!(map.remove(&2), None);
        assert_eq!(map.len(), 2);

        assert_eq!(map.remove_entry(&1), Some((1, "one".to_string())));
        assert_eq!(map.len(), 1);

        map.clear();
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        assert_eq!(map.get(&3), None);
    }

    #[test]
    fn test_entry_api() {
        let mut map = SkipTableMap::<&str, i32>::new(8);

        map.entry("a").or_insert(100);
        assert_eq!(map.get("a"), Some(&100));

        map.entry("a").or_insert(200);
        assert_eq!(map.get("a"), Some(&100));

        map.entry("b").or_insert_with(|| 200);
        assert_eq!(map.get("b"), Some(&200));

        map.entry("c").or_insert_with_key(|k| k.len() as i32 * 10);
        assert_eq!(map.get("c"), Some(&10));

        map.entry("a").and_modify(|v| *v += 10);
        assert_eq!(map.get("a"), Some(&110));

        map.entry("non_existent").and_modify(|v| *v += 10);
        assert_eq!(map.get("non_existent"), None);

        map.entry("d").or_default();
        assert_eq!(map.get("d"), Some(&0));
    }

    #[test]
    fn test_first_and_last_operations() {
        let mut map = SkipTableMap::<i32, i32>::new(16);

        assert_eq!(map.first_key_value(), None);
        assert_eq!(map.last_key_value(), None);
        assert_eq!(map.pop_first(), None);
        assert_eq!(map.pop_last(), None);

        map.insert(30, 300);
        map.insert(10, 100);
        map.insert(20, 200);
        map.insert(40, 400);

        assert_eq!(map.first_key_value(), Some((&10, &100)));
        assert_eq!(map.last_key_value(), Some((&40, &400)));

        assert_eq!(map.pop_first(), Some((10, 100)));
        assert_eq!(map.first_key_value(), Some((&20, &200)));
        assert_eq!(map.len(), 3);

        assert_eq!(map.pop_last(), Some((40, 400)));
        assert_eq!(map.last_key_value(), Some((&30, &300)));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_iterators() {
        let mut map = SkipTableMap::<i32, i32>::new(8);
        map.insert(3, 30);
        map.insert(1, 10);
        map.insert(2, 20);

        let collected: Vec<_> = map.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(collected, vec![(1, 10), (2, 20), (3, 30)]);

        let keys: Vec<_> = map.keys().copied().collect();
        assert_eq!(keys, vec![1, 2, 3]);

        let values: Vec<_> = map.values().copied().collect();
        assert_eq!(values, vec![10, 20, 30]);

        for v in map.values_mut() {
            *v *= 2;
        }

        let updated_values: Vec<_> = map.values().copied().collect();
        assert_eq!(updated_values, vec![20, 40, 60]);
    }

    #[test]
    fn test_range() {
        let mut map = SkipTableMap::<i32, i32>::new(16);
        for i in (1..=10).rev() {
            map.insert(i, i * 10);
        }

        let range1: Vec<_> = map.range(3..=7).map(|(&k, &v)| (k, v)).collect();
        assert_eq!(range1, vec![(3, 30), (4, 40), (5, 50), (6, 60), (7, 70)]);

        let range2: Vec<_> = map
            .range((std::ops::Bound::Excluded(3), std::ops::Bound::Excluded(7)))
            .map(|(&k, &v)| (k, v))
            .collect();
        assert_eq!(range2, vec![(4, 40), (5, 50), (6, 60)]);

        let range3: Vec<_> = map.range(..).map(|(&k, &v)| (k, v)).collect();
        assert_eq!(range3.len(), 10);

        for (_, v) in map.range_mut(3..=5) {
            *v += 1;
        }
        assert_eq!(map.get(&3), Some(&31));
        assert_eq!(map.get(&4), Some(&41));
        assert_eq!(map.get(&5), Some(&51));
        assert_eq!(map.get(&6), Some(&60));
    }

    #[test]
    fn test_edge_cases_and_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct DropChecker<'a>(&'a AtomicBool);
        impl<'a> Drop for DropChecker<'a> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped1 = AtomicBool::new(false);
        let dropped2 = AtomicBool::new(false);

        {
            let mut map = SkipTableMap::<i32, DropChecker>::new(4);
            map.insert(1, DropChecker(&dropped1));
            map.insert(2, DropChecker(&dropped2));
            map.remove(&1);
            assert!(dropped1.load(Ordering::SeqCst));
            assert!(!dropped2.load(Ordering::SeqCst));
        }

        assert!(dropped2.load(Ordering::SeqCst));
    }

    #[test]
    fn test_concurrent_rwlock_access() {
        use std::sync::{Arc, RwLock};
        use std::thread;

        let map = Arc::new(RwLock::new(SkipTableMap::<i32, String>::new(16)));
        let mut handles = vec![];

        {
            let mut writer = map.write().unwrap();
            for i in 0..100 {
                writer.insert(i, format!("val_{}", i));
            }
        }

        for _ in 0..4 {
            let map_clone = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                let reader = map_clone.read().unwrap();
                for i in 0..100 {
                    if let Some(val) = reader.get(&i) {
                        assert_eq!(val, &format!("val_{}", i));
                    }
                }
                assert!(reader.len() >= 100);
            }));
        }

        for writer_id in 0..2 {
            let map_clone = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                for i in 100..200 {
                    let key = writer_id * 1000 + i;
                    let mut writer = map_clone.write().unwrap();
                    writer.insert(key, format!("concurrent_val_{}", key));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_map = map.read().unwrap();
        assert_eq!(final_map.len(), 300);
        assert_eq!(final_map.get(&0), Some(&"val_0".to_string()));
        assert_eq!(
            final_map.get(&1100),
            Some(&"concurrent_val_1100".to_string())
        );
    }

    #[test]
    fn test_concurrent_mutex_insert_remove() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let map = Arc::new(Mutex::new(SkipTableMap::<i32, i32>::new(16)));
        let mut handles = vec![];

        for t in 0..10 {
            let map_clone = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                let start = t * 100;
                let end = start + 100;

                for i in start..end {
                    let mut guard = map_clone.lock().unwrap();
                    guard.insert(i, i * 10);
                }

                for i in (start..end).filter(|k| k % 2 == 0) {
                    let mut guard = map_clone.lock().unwrap();
                    let removed = guard.remove(&i);
                    assert_eq!(removed, Some(i * 10));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_map = map.lock().unwrap();
        assert_eq!(final_map.len(), 500);

        for i in 0..1000 {
            if i % 2 == 0 {
                assert_eq!(final_map.get(&i), None);
            } else {
                assert_eq!(final_map.get(&i), Some(&(i * 10)));
            }
        }
    }
}
