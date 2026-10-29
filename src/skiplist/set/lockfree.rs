//! A set based on skip list. It's designed to be used in concurrent mode and performance
//! takes high priority. It has narrower APIs comapred to [super::ordinary].
//! It's implemented based on [crate::skiplist::map::lockfree].
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::skiplist::set::lockfree::SkipListSet;
//! fn main() {
//!     let mut set = SkipListSet::new(3, None);
//!     assert!(set.insert(10));
//!     assert!(set.contains(&10));
//!     assert!(!set.contains(&40));
//! }
//! ```

use crate::Result;
use crate::skiplist::map::lockfree::{EntryRef, SkipListMap, TunableConfig};
use std::borrow::Borrow;
use std::ops::Deref;

/// A set based on skip list.
pub struct SkipListSet<T> {
    inner: SkipListMap<T, ()>,
}

impl<T> SkipListSet<T> {
    /// Makes a new, empty [SkipListSet].
    ///
    /// Parameters:
    /// - `max_height`: The maximum height allowed. The allowed range is [1, 32].
    /// - `config`: The optional configuration for the backend [SkipListMap].
    pub fn new(max_height: usize, config: Option<TunableConfig>) -> Self {
        Self {
            inner: SkipListMap::<T, ()>::new(max_height, config),
        }
    }

    /// Returns `true` if the set contains an element equal to the value.
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.contains_key(value)
    }

    /// Returns a reference to the element in the set, if any, that is equal to the value.
    pub fn get<Q>(&self, value: &Q) -> Option<ElementRef<T>>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.get(value).unwrap().map(|e| ElementRef(e))
    }

    /// Adds a value to the set. Returns whether the value was newly inserted.
    pub fn insert(&self, value: T) -> bool
    where
        T: Ord,
    {
        match self.inner.insert(value, ()) {
            Ok(res) => res.is_none(),
            Err(_) => false, //Impossible arm
        }
    }

    /// Adds a value to the set, replacing the existing element, if any, that is equal to the value.
    pub fn replace(&self, value: T) -> Option<T>
    where
        T: Ord,
    {
        match self.inner.replace(value, ()) {
            Ok(opt_kv) => opt_kv.map(|kv| kv.0),
            Err(_) => None, //Impossible arm
        }
    }

    /// If the set contains an element equal to the value, removes it from the set and drops it.
    pub fn remove<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.remove_wo_kv(value)
    }

    /// Removes and returns the element in the set, if any, that is equal to the value.
    ///
    /// [crate::Error::ElementInUse] is returned if the the element is being used by others via
    /// [SkipListSet::get], since ownership of element can't be taken in that case.
    pub fn take<Q>(&self, value: &Q) -> Result<Option<T>>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.remove(value).map(|res| res.map(|kv| kv.0))
    }

    /// Returns the number of elements in the set. NOTE that by default it doesn't work (always
    /// returns 0) unless you configured the map with a
    /// [LenOption](crate::skiplist::map::lockfree::LenOption) rather than
    /// [LenOption::NotProvided](crate::skiplist::map::lockfree::LenOption::NotProvided).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns a reference to the first element in the set, if any.
    pub fn first_elem(&self) -> Option<ElementRef<T>> {
        self.inner.first_entry().unwrap().map(|e| ElementRef(e))
    }

    /// Returns a reference to the last element in the set, if any.
    pub fn last_elem(&self) -> Option<ElementRef<T>> {
        self.inner.last_entry().unwrap().map(|e| ElementRef(e))
    }

    /// Returns a reference to the element that immediately after the `value`, if any.
    pub fn next_elem<Q>(&self, value: &Q) -> Option<ElementRef<T>>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.next_entry(value).unwrap().map(|e| ElementRef(e))
    }

    /// Returns a reference to the element that immediately before the `value`, if any.
    pub fn prev_elem<Q>(&self, value: &Q) -> Option<ElementRef<T>>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.prev_entry(value).unwrap().map(|e| ElementRef(e))
    }

    /// Clears the set, removing all elements.
    /// <div class="warning">
    ///
    /// **CAUTION**
    ///
    /// See the CAUTION description for [super::lockfree::SkipListMap::clear]
    ///
    /// It may be an option to create a new set instead of calling this method.
    ///
    /// </div>
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Iterate elements with a function.
    /// <div class="warning">
    ///
    /// **CAUTION**
    ///
    /// See the CAUTION description for [super::lockfree::SkipListMap::iter_with]
    ///
    /// It may be an option to use [SkipListSet::next_elem] with with [SkipListSet::first_elem]
    /// to achieve iteration.
    ///
    /// </div>
    pub fn iter_with<F>(&self, mut func: F)
    where
        F: FnMut(ElementRef<T>),
    {
        self.inner.iter_with(|e| func(ElementRef(e)));
    }
}

/// The reference to a element in the [SkipListSet].
pub struct ElementRef<T>(EntryRef<T, ()>);

impl<T> Deref for ElementRef<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn create_test_set() -> SkipListSet<i32> {
        SkipListSet::new(8, None)
    }

    #[test]
    fn test_insert_and_contains() {
        let set = create_test_set();
        assert!(set.is_empty());

        assert!(set.insert(10));
        assert!(set.insert(20));

        assert!(!set.is_empty());

        assert!(set.contains(&10));
        assert!(set.contains(&20));
        assert!(!set.contains(&30));

        assert!(!set.insert(10));
    }

    #[test]
    fn test_get_and_deref() {
        let set = create_test_set();
        set.insert(42);

        let val_ref = set.get(&42);
        assert!(val_ref.is_some());

        let val_ref = val_ref.unwrap();
        assert_eq!(*val_ref, 42);

        assert!(set.get(&100).is_none());
    }

    #[test]
    fn test_replace() {
        #[derive(Debug, Eq)]
        struct Foo {
            id: i32,
            data: &'static str,
        }

        impl PartialEq for Foo {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }
        impl PartialOrd for Foo {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Foo {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.id.cmp(&other.id)
            }
        }

        let set = SkipListSet::new(16, None);

        let old = set.replace(Foo { id: 1, data: "v1" });
        assert!(old.is_none());

        let old = set.replace(Foo { id: 1, data: "v2" });
        assert!(old.is_some());
        assert_eq!(old.unwrap().data, "v1");

        let current = set.get(&Foo { id: 1, data: "" }).unwrap();
        assert_eq!(current.data, "v2");
    }

    #[test]
    fn test_remove() {
        let set = create_test_set();
        set.insert(100);
        set.insert(200);

        assert!(set.remove(&100));
        assert!(!set.contains(&100));

        assert!(!set.remove(&100));
        assert!(!set.remove(&300));

        assert!(set.contains(&200));
    }

    #[test]
    fn test_take() {
        let set = create_test_set();
        set.insert(50);

        let res_missing = set.take(&100);
        assert!(res_missing.is_ok());
        assert_eq!(res_missing.unwrap(), None);

        let res_existing = set.take(&50);
        assert!(res_existing.is_ok());
        assert_eq!(res_existing.unwrap(), Some(50));

        assert!(!set.contains(&50));
    }

    #[test]
    fn test_borrow_query() {
        let set = SkipListSet::new(16, None);
        set.insert("hello".to_string());

        assert!(set.contains("hello"));
        assert_eq!(*set.get("hello").unwrap(), "hello");
        assert!(set.remove("hello"));
        assert!(!set.contains("hello"));
    }

    #[test]
    fn test_concurrent_insert_and_contains() {
        let set = Arc::new(SkipListSet::<i32>::new(16, None));
        let threads: Vec<_> = (0..10)
            .map(|i| {
                let set_clone = Arc::clone(&set);
                thread::spawn(move || {
                    for j in 0..10000 {
                        let val = i * 10000 + j;
                        set_clone.insert(val);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        for i in 0..100000 {
            assert!(set.contains(&i), "Element {} missing", i);
        }
    }

    #[test]
    fn test_first_and_last_elem() {
        let set = create_test_set();

        assert!(set.first_elem().is_none());
        assert!(set.last_elem().is_none());

        set.insert(20);
        assert_eq!(*set.first_elem().unwrap(), 20);
        assert_eq!(*set.last_elem().unwrap(), 20);

        set.insert(10);
        set.insert(30);
        set.insert(5);

        assert_eq!(*set.first_elem().unwrap(), 5);
        assert_eq!(*set.last_elem().unwrap(), 30);
    }

    #[test]
    fn test_next_and_prev_elem() {
        let set = create_test_set();

        set.insert(20);
        set.insert(10);
        set.insert(40);
        set.insert(30);

        assert_eq!(*set.next_elem(&10).unwrap(), 20);
        assert_eq!(*set.next_elem(&25).unwrap(), 30);
        assert_eq!(*set.next_elem(&0).unwrap(), 10);
        assert!(set.next_elem(&40).is_none());
        assert!(set.next_elem(&50).is_none());

        assert_eq!(*set.prev_elem(&40).unwrap(), 30);
        assert_eq!(*set.prev_elem(&25).unwrap(), 20);
        assert_eq!(*set.prev_elem(&50).unwrap(), 40);
        assert!(set.prev_elem(&10).is_none());
        assert!(set.prev_elem(&0).is_none());
    }

    #[test]
    fn test_clear() {
        let set = create_test_set();

        set.insert(1);
        set.insert(2);
        set.insert(3);
        assert!(!set.is_empty());

        set.clear();

        assert!(set.is_empty());
        assert!(!set.contains(&1));
        assert!(set.first_elem().is_none());
        assert!(set.last_elem().is_none());
    }

    #[test]
    fn test_iter_with() {
        let set = create_test_set();
        set.insert(30);
        set.insert(10);
        set.insert(20);

        let mut collected = Vec::new();
        set.iter_with(|elem| {
            collected.push(*elem);
        });

        assert_eq!(collected, vec![10, 20, 30]);

        let empty_set = create_test_set();
        let mut empty_collected = Vec::new();
        empty_set.iter_with(|elem| {
            empty_collected.push(*elem);
        });
        assert!(empty_collected.is_empty());
    }
}
