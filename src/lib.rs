//! This crate aims to provide simple implementations for several database primitives (Work In Progress).
//!
//! Currently below items are thought to be available, but NOTE they may be changed in the future,
//! including their interfaces).
//! - [ringbuffer]
//! - [appender]
//! - [skiptable::map::ordinary] (ordinary skip table map)
//! - [sync::spinlock]
//! - [wal] (Write Ahead Log)

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

pub mod appender;
//pub mod arena;
//pub mod bloomfilter;
//pub mod btree; // B+ Tree
pub mod io;
//pub mod cbtree; // classic B Tree
//pub mod lsm;
pub mod ringbuffer;
pub mod skiptable;
pub mod sync;
pub mod wal; // Write Ahead Log

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    /// Miscellaneous error.
    MiscError(String),
    //#[error("Unknown lowlyering error")]
    //Unknown(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("IO error occurred")]
    IoError(#[from] std::io::Error),
    #[error("Provided path `{0}` is not a directory")]
    NotADirectory(String),
    #[error("Invalid Argument. Details: `{0}`")]
    InvalidArgument(String),
    #[error("Not supported. Details: `{0}`")]
    NotSupported(String),
    #[error("CRC check failed")]
    CrcCheckFailed,
    /// The ring buffer is full now. The caller may need to try again later.
    #[error("Ring buffer is full. Maybe try later.")]
    RingBufferFull,
    #[error("Log Entry too large [max_allowed:{0}, actual:{1}`")]
    LogEntryTooLarge(u32, u32),
    #[error("Corrupt file. Details: `{0}`")]
    CorruptFile(String),
    #[error("Decoding failed. Type: `{0}`. Details: `{1}`")]
    DecodingFailed(String, String),
    /// Some needed function (i.g. some backend) is not running.
    #[error("Function `{0}` is not running")]
    FunctionNotRunning(String),
    #[error("Element is in use")]
    ElementInUse,
    #[error("Element is in mutable use")]
    ElementInMutUse,
    #[error("Element is deleted")]
    ElementDeleted,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[repr(align(64))]
struct CacheAligned<T>(T);

pub(crate) struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedBuffer {
    pub(crate) fn new(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).unwrap();
        let raw_ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw_ptr).unwrap();
        Self { ptr, layout }
    }

    pub(crate) fn get_vec(&self) -> Vec<u8> {
        unsafe { Vec::from_raw_parts(self.ptr.as_ptr(), 0, self.layout.size()) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

// #[cfg(test)]
// pub(crate) mod alloc_tests {
//     use std::alloc::{GlobalAlloc, Layout, System};
//     use std::sync::atomic::{AtomicIsize, Ordering};

//     pub struct MyAlloc {
//         pub(crate) bytes: AtomicIsize,
//         pub(crate) count: AtomicIsize,
//     }

//     impl MyAlloc {
//         pub const fn new() -> Self {
//             Self {
//                 bytes: AtomicIsize::new(0),
//                 count: AtomicIsize::new(0),
//             }
//         }

//         pub fn assert_no_leaks(&self) {
//             assert_eq!(self.bytes.load(Ordering::Acquire), 0);
//             assert_eq!(self.count.load(Ordering::Acquire), 0);
//         }
//     }

//     unsafe impl GlobalAlloc for MyAlloc {
//         unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
//             unsafe {
//                 let ptr = System.alloc(layout);
//                 if !ptr.is_null() {
//                     self.bytes
//                         .fetch_add(layout.size() as isize, Ordering::SeqCst);
//                     self.count.fetch_add(1, Ordering::SeqCst);
//                 }
//                 ptr
//             }
//         }

//         unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
//             unsafe {
//                 System.dealloc(ptr, layout);
//                 self.bytes
//                     .fetch_sub(layout.size() as isize, Ordering::SeqCst);
//                 self.count.fetch_sub(1, Ordering::SeqCst);
//             }
//         }
//     }

//     #[global_allocator]
//     pub(crate) static ALLOC: MyAlloc = MyAlloc::new();
// }

// #[cfg(test)]
// pub(crate) mod tests {
//     #[test]
//     fn test_dummy() {}
// }
