//! A set based on skip list, with APIs similar to [std::collections::BTreeSet].
//! It's designed to be used in non-concurrent mode and the user needs to use synchronization
//! utilities (i.g. locks) to use it in concurrent mode.
//! It's implemented based on [crate::skiplist::map::ordinary].
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::skiplist::set::ordinary::SkipListSet;
//! fn main() {
//!     let mut set = SkipListSet::new(3);
//!     assert!(set.insert(10));
//!     assert!(set.contains(&10));
//!     assert!(!set.contains(&40));
//! }
//! ```

use crate::skiplist::map::ordinary::{Iter as MapIter, Range as MapRange, SkipListMap};
use std::borrow::Borrow;
use std::ops::{RangeBounds, RangeInclusive};

/// A set based on skip list.
pub struct SkipListSet<T> {
    inner: SkipListMap<T, ()>,
}

impl<T> SkipListSet<T> {
    /// Makes a new, empty [SkipListSet].
    ///
    /// Parameters:
    /// - `max_height`: The maximum height allowed. The allowed range is [1, 32].
    pub fn new(max_height: usize) -> Self {
        Self {
            inner: SkipListMap::<T, ()>::new(max_height),
        }
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the set.
    pub fn range<Q, R>(&self, range: R) -> Range<'_, T, Q, R>
    where
        Q: Ord + ?Sized,
        T: Borrow<Q> + Ord,
        R: RangeBounds<Q>,
    {
        Range {
            inner: self.inner.range(range),
        }
    }

    /// Visits the elements representing the difference, i.e., the elements that are in `self`
    /// but not in `other`, in ascending order.
    pub fn difference<'a>(&'a self, other: &'a Self) -> Difference<'a, T>
    where
        T: Ord,
    {
        Difference {
            iter: self.iter(),
            set_o: other,
        }
    }

    /// Visits the elements representing the symmetric difference, i.e., the elements that are in
    /// `self` or in `other` but not in both, in ascending order.
    pub fn symmetric_difference<'a>(&'a self, other: &'a Self) -> SymmetricDifference<'a, T>
    where
        T: Ord,
    {
        let mut iter = self.iter();
        let mut iter_o = other.iter();
        SymmetricDifference {
            val: iter.next(),
            val_o: iter_o.next(),
            iter,
            iter_o,
        }
    }

    /// Visits the elements representing the intersection, i.e., the elements that are both in
    /// `self` and `other`, in ascending order.
    pub fn intersection<'a>(&'a self, other: &'a Self) -> Intersection<'a, T>
    where
        T: Ord,
    {
        let inner = match (self.first(), self.last(), other.first(), other.last()) {
            (Some(min), Some(max), Some(o_min), Some(o_max)) => {
                let insec_min = min.max(o_min);
                let insec_max = max.min(o_max);
                let mut range = self.range(insec_min..=insec_max);
                let mut range_o = other.range(insec_min..=insec_max);
                Some(IntersectionInner {
                    val: range.next(),
                    val_o: range_o.next(),
                    range,
                    range_o,
                })
            }
            _ => None,
        };
        Intersection { inner }
    }

    /// Visits the elements representing the union, i.e., all the elements in `self` or `other`,
    /// without duplicates, in ascending order.
    pub fn union<'a>(&'a self, other: &'a Self) -> Union<'a, T>
    where
        T: Ord,
    {
        let mut iter = self.iter();
        let mut iter_o = other.iter();
        Union {
            val: iter.next(),
            val_o: iter_o.next(),
            iter,
            iter_o,
        }
    }

    /// Clears the set, removing all elements.
    pub fn clear(&mut self) {
        self.inner.clear();
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
    pub fn get<Q>(&self, value: &Q) -> Option<&T>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.get_key_value(value).map(|kv| kv.0)
    }

    /// Returns `true` if self has no elements in common with `other`.
    pub fn is_disjoint(&self, other: &Self) -> bool
    where
        T: Ord,
    {
        match (self.first(), self.last(), other.first(), other.last()) {
            (None, _, _, _) => true,
            (_, _, None, _) => true,
            (Some(min), Some(max), Some(o_min), Some(o_max)) => min.gt(o_max) || max.lt(o_min),
            _ => false, //impossible arm
        }
    }

    /// Returns a reference to the first element in the set, if any.
    pub fn first(&self) -> Option<&T>
    where
        T: Ord,
    {
        self.inner.first_key_value().map(|kv| kv.0)
    }

    /// Returns a reference to the last element in the set, if any.
    pub fn last(&self) -> Option<&T>
    where
        T: Ord,
    {
        self.inner.last_key_value().map(|kv| kv.0)
    }

    /// Returns true if the set is a subset of `other`.
    pub fn is_subset(&self, other: &Self) -> bool
    where
        T: Ord,
    {
        match (self.first(), self.last(), other.first(), other.last()) {
            (None, _, _, _) => true,
            (_, _, None, _) => false,
            (Some(min), Some(max), Some(o_min), Some(o_max)) => min.ge(o_min) && max.le(o_max),
            _ => false, //impossible arm
        }
    }

    /// Returns `true` if the set is a superset of `other`.
    pub fn is_superset(&self, other: &Self) -> bool
    where
        T: Ord,
    {
        match (self.first(), self.last(), other.first(), other.last()) {
            (_, _, None, _) => true,
            (None, _, _, _) => false,
            (Some(min), Some(max), Some(o_min), Some(o_max)) => min.le(o_min) && max.ge(o_max),
            _ => false, //impossible arm
        }
    }

    /// Removes the first element from the set and returns it, if any.
    pub fn pop_first(&mut self) -> Option<T>
    where
        T: Ord,
    {
        self.inner.pop_first().map(|kv| kv.0)
    }

    /// Removes the last element from the set and returns it, if any.
    pub fn pop_last(&mut self) -> Option<T>
    where
        T: Ord,
    {
        self.inner.pop_last().map(|kv| kv.0)
    }

    /// Adds a value to the set. Returns whether the value was newly inserted.
    pub fn insert(&mut self, value: T) -> bool
    where
        T: Ord,
    {
        self.inner.insert(value, ()).is_none()
    }

    /// Adds a value to the set, replacing the existing element, if any, that is equal to the value.
    pub fn replace(&mut self, value: T) -> Option<T>
    where
        T: Ord,
    {
        let mut old = None;
        if self.inner.contains_key(&value) {
            old = self.inner.remove_entry(&value).map(|kv| kv.0);
        }
        self.inner.insert(value, ());
        old
    }

    /// If the set contains an element equal to the value, removes it from the set and drops it.
    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.remove(value).is_some()
    }

    /// Removes and returns the element in the set, if any, that is equal to the value.
    pub fn take<Q>(&mut self, value: &Q) -> Option<T>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.remove_entry(value).map(|kv| kv.0)
    }

    /// Gets an iterator that visits the elements in the [SkipListSet] in ascending order.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self.inner.iter(),
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// An iterator over a sub-range of elements in a [SkipListSet].
/// This struct is created by [SkipListSet::range].
pub struct Range<'a, T, Q, R>
where
    Q: Ord + ?Sized,
    T: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    inner: MapRange<'a, T, (), Q, R>,
}

impl<'a, T: 'a, Q, R> Iterator for Range<'a, T, Q, R>
where
    Q: Ord + ?Sized,
    T: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
{
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        self.inner.next().map(|kv| kv.0)
    }
}

/// An iterator over the elements of a [SkipListSet].
/// This struct is created by [SkipListSet::iter].
pub struct Iter<'a, T: 'a> {
    inner: MapIter<'a, T, ()>,
}

impl<'a, T: 'a> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        self.inner.next().map(|kv| kv.0)
    }
}

/// An iterator over the elements in the union of two [SkipListSet]s.
/// This struct is created by [SkipListSet::union].
pub struct Union<'a, T: 'a + Ord> {
    iter: Iter<'a, T>,
    iter_o: Iter<'a, T>,
    val: Option<&'a T>,
    val_o: Option<&'a T>,
}

impl<'a, T: 'a + Ord> Iterator for Union<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        match (self.val, self.val_o) {
            (None, None) => None,
            (val @ Some(_), None) => {
                self.val = self.iter.next();
                val
            }
            (None, val @ Some(_)) => {
                self.val_o = self.iter_o.next();
                val
            }
            (val @ Some(v), val_o @ Some(v_o)) => match v.cmp(v_o) {
                std::cmp::Ordering::Equal => {
                    self.val = self.iter.next();
                    self.val_o = self.iter_o.next();
                    val
                }
                std::cmp::Ordering::Less => {
                    self.val = self.iter.next();
                    val
                }
                std::cmp::Ordering::Greater => {
                    self.val_o = self.iter_o.next();
                    val_o
                }
            },
        }
    }
}

/// An iterator over the elements in the symmetric difference of two [SkipListSet]s.
/// This struct is created by [SkipListSet::symmetric_difference].
pub struct SymmetricDifference<'a, T: 'a + Ord> {
    iter: Iter<'a, T>,
    iter_o: Iter<'a, T>,
    val: Option<&'a T>,
    val_o: Option<&'a T>,
}

impl<'a, T: 'a + Ord> Iterator for SymmetricDifference<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        loop {
            return match (self.val, self.val_o) {
                (None, None) => None,
                (val @ Some(_), None) => {
                    self.val = self.iter.next();
                    val
                }
                (None, val @ Some(_)) => {
                    self.val_o = self.iter_o.next();
                    val
                }
                (val @ Some(v), val_o @ Some(v_o)) => match v.cmp(v_o) {
                    std::cmp::Ordering::Equal => {
                        self.val = self.iter.next();
                        self.val_o = self.iter_o.next();
                        continue;
                    }
                    std::cmp::Ordering::Less => {
                        self.val = self.iter.next();
                        val
                    }
                    std::cmp::Ordering::Greater => {
                        self.val_o = self.iter_o.next();
                        val_o
                    }
                },
            };
        }
    }
}

/// An iterator over the elements in the difference of two [SkipListSet]s.
/// This struct is created by [SkipListSet::difference].
pub struct Difference<'a, T: 'a + Ord> {
    iter: Iter<'a, T>,
    set_o: &'a SkipListSet<T>,
}

impl<'a, T: 'a + Ord> Iterator for Difference<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        loop {
            return match self.iter.next() {
                None => None,
                val @ Some(v) => {
                    if self.set_o.contains(v) {
                        continue;
                    }
                    val
                }
            };
        }
    }
}

struct IntersectionInner<'a, T: 'a + Ord> {
    range: Range<'a, T, T, RangeInclusive<&'a T>>,
    range_o: Range<'a, T, T, RangeInclusive<&'a T>>,
    val: Option<&'a T>,
    val_o: Option<&'a T>,
}

/// An iterator over the elements in the intersection of two [SkipListSet]s.
/// This struct is created by [SkipListSet::intersection].
pub struct Intersection<'a, T: 'a + Ord> {
    inner: Option<IntersectionInner<'a, T>>,
}

impl<'a, T: 'a + Ord> Iterator for Intersection<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        match &mut self.inner {
            None => None,
            Some(inn) => loop {
                return match (inn.val, inn.val_o) {
                    (None, _) | (_, None) => None,
                    (val @ Some(v), Some(v_o)) => match v.cmp(v_o) {
                        std::cmp::Ordering::Equal => {
                            inn.val = inn.range.next();
                            inn.val_o = inn.range_o.next();
                            val
                        }
                        std::cmp::Ordering::Less => {
                            inn.val = inn.range.next();
                            continue;
                        }
                        std::cmp::Ordering::Greater => {
                            inn.val_o = inn.range_o.next();
                            continue;
                        }
                    },
                };
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_MAX_HEIGHT: usize = 16;

    #[test]
    fn test_new_and_empty() {
        let set: SkipListSet<i32> = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert_eq!(set.first(), None);
        assert_eq!(set.last(), None);
    }

    #[test]
    fn test_insert_and_contains() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        assert!(set.insert(10));
        assert!(set.insert(20));
        assert!(set.insert(30));

        assert!(!set.insert(10));

        assert_eq!(set.len(), 3);
        assert!(!set.is_empty());

        assert!(set.contains(&10));
        assert!(set.contains(&20));
        assert!(!set.contains(&40));

        assert_eq!(set.get(&20), Some(&20));
        assert_eq!(set.get(&40), None);
    }

    #[test]
    fn test_first_and_last() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        assert_eq!(set.first(), None);
        assert_eq!(set.last(), None);

        set.insert(20);
        set.insert(10);
        set.insert(30);

        assert_eq!(set.first(), Some(&10));
        assert_eq!(set.last(), Some(&30));
    }

    #[test]
    fn test_pop_first_and_pop_last() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        set.insert(10);
        set.insert(20);
        set.insert(30);
        set.insert(40);

        assert_eq!(set.pop_first(), Some(10));
        assert_eq!(set.len(), 3);

        assert_eq!(set.pop_last(), Some(40));
        assert_eq!(set.len(), 2);

        assert_eq!(set.first(), Some(&20));
        assert_eq!(set.last(), Some(&30));

        assert_eq!(set.pop_first(), Some(20));
        assert_eq!(set.pop_last(), Some(30));
        assert_eq!(set.pop_first(), None);
        assert_eq!(set.pop_last(), None);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_and_take() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        set.insert(10);
        set.insert(20);

        assert!(set.remove(&10));
        assert!(!set.remove(&10));
        assert_eq!(set.len(), 1);

        assert_eq!(set.take(&20), Some(20));
        assert_eq!(set.take(&20), None);
        assert!(set.is_empty());
    }

    #[test]
    fn test_replace() {
        #[derive(Debug, Eq)]
        struct Foo {
            id: i32,
            val: &'static str,
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

        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        let old = set.replace(Foo {
            id: 1,
            val: "first",
        });
        assert!(old.is_none());

        let old = set.replace(Foo {
            id: 1,
            val: "second",
        });
        assert_eq!(old.map(|f| f.val), Some("first"));

        assert_eq!(
            set.get(&Foo { id: 1, val: "" }).map(|f| f.val),
            Some("second")
        );
    }

    #[test]
    fn test_clear() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        set.insert(1);
        set.insert(2);
        set.insert(3);

        assert_eq!(set.len(), 3);
        set.clear();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert_eq!(set.first(), None);
    }

    #[test]
    fn test_iter() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        set.insert(30);
        set.insert(10);
        set.insert(20);

        let collected: Vec<&i32> = set.iter().collect();
        assert_eq!(collected, vec![&10, &20, &30]);
    }

    #[test]
    fn test_range() {
        let mut set = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        for i in 1..=5 {
            set.insert(i * 10); // 10, 20, 30, 40, 50
        }

        let res: Vec<&i32> = set.range(20..40).collect();
        assert_eq!(res, vec![&20, &30]);

        let res_inc: Vec<&i32> = set.range(20..=40).collect();
        assert_eq!(res_inc, vec![&20, &30, &40]);

        let res_from: Vec<&i32> = set.range(30..).collect();
        assert_eq!(res_from, vec![&30, &40, &50]);
    }

    #[test]
    fn test_difference() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [1, 2, 3, 4] {
            a.insert(x);
        }
        for x in [3, 4, 5, 6] {
            b.insert(x);
        }

        // a - b = {1, 2}
        let diff: Vec<&i32> = a.difference(&b).collect();
        assert_eq!(diff, vec![&1, &2]);
    }

    #[test]
    fn test_symmetric_difference() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [1, 2, 3, 4] {
            a.insert(x);
        }
        for x in [3, 4, 5, 6] {
            b.insert(x);
        }

        // a △ b = {1, 2, 5, 6}
        let sym_diff: Vec<&i32> = a.symmetric_difference(&b).collect();
        assert_eq!(sym_diff, vec![&1, &2, &5, &6]);
    }

    #[test]
    fn test_intersection() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [1, 2, 3, 4, 5] {
            a.insert(x);
        }
        for x in [3, 4, 5, 6, 7] {
            b.insert(x);
        }

        // a ∩ b = {3, 4, 5}
        let insec: Vec<&i32> = a.intersection(&b).collect();
        assert_eq!(insec, vec![&3, &4, &5]);

        let empty = SkipListSet::<i32>::new(DEFAULT_MAX_HEIGHT);
        let empty_insec: Vec<&i32> = a.intersection(&empty).collect();
        assert!(empty_insec.is_empty());
    }

    #[test]
    fn test_union() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [1, 3, 5] {
            a.insert(x);
        }
        for x in [2, 3, 4, 6] {
            b.insert(x);
        }

        // a ∪ b = {1, 2, 3, 4, 5, 6}
        let u: Vec<&i32> = a.union(&b).collect();
        assert_eq!(u, vec![&1, &2, &3, &4, &5, &6]);
    }

    #[test]
    fn test_is_disjoint() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut c = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [1, 2, 3] {
            a.insert(x);
        }
        for x in [4, 5, 6] {
            b.insert(x);
        }
        for x in [3, 4, 5] {
            c.insert(x);
        }

        assert!(a.is_disjoint(&b));
        assert!(!a.is_disjoint(&c));

        let empty = SkipListSet::<i32>::new(DEFAULT_MAX_HEIGHT);
        assert!(a.is_disjoint(&empty));
        assert!(empty.is_disjoint(&a));
    }

    #[test]
    fn test_is_subset_and_superset() {
        let mut a = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        let mut b = SkipListSet::new(DEFAULT_MAX_HEIGHT);

        for x in [2, 3] {
            a.insert(x);
        }
        for x in [1, 2, 3, 4] {
            b.insert(x);
        }

        assert!(a.is_subset(&b)); // 注意：源码中 is_subset 传值 `other: Self`
        assert!(b.is_superset(&a));

        let mut c = SkipListSet::new(DEFAULT_MAX_HEIGHT);
        c.insert(5);

        assert!(!c.is_subset(&b));
        assert!(!b.is_superset(&c));
    }
}
