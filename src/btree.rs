//! Not implemented.

use bytes::Bytes;

mod disk;

type KeyType = Bytes;
type ValueType = Bytes;

pub struct MemLeafNode {
    key: KeyType,
    value: ValueType,
}

pub struct MemIndexNode {
    sep_keys: Vec<KeyType>,
}
