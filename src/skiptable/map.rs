//! A key-value map based on skip table. [ordinary::SkipTableMap] is designed to used
//! in non-concurrent mode and the user needs to use synchronization utilities (i.g. locks)
//! to use it in concurrent mode. And [lockfree::SkipTableMap] is designed to used in
//! concurrent mode and performance takes high priority.
pub mod lockfree;
pub mod ordinary;
