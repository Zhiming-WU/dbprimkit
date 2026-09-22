use dbprimkit::skiptable::map::ordinary::SkipTableMap;

fn main() {
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
