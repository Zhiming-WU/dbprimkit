//! This crate provides simple implementations for several database primitives (Work In Progress).
//!
//! Currently below items are thought to be available, but NOTE they may be changed in the future,
//! including their interfaces).
//! - [ringbuffer]
//! - [appender]
//! - [wal]

pub mod appender;
pub mod arena;
pub mod bloomfilter;
pub mod btree; // B+ Tree
pub mod io;
//pub mod cbtree; // classic B Tree
pub mod lsm;
pub mod ringbuffer;
pub mod skiptable;
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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[repr(align(64))]
struct CacheAligned<T>(T);
