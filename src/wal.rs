//! A simple WAL (Write Ahead Log) implementation. See [sync] and [async] for more information.
//!
//! For use as writer, usually [sync::StdFileWalInstance] has a better performance
//! than [async::TokioFileWalInstance] since a dedicated flushing thread is used to
//! avoid the transfering overheads between the flushing task and tokio blocking IO threads.
//!
//! After opened, the [sync::WalWriter] can be used in async code since it has no
//! time-consuming blocking method.
//!
//! To tune the configuration, provide a [TunableConfig] to [sync::WalInstance::new] or [async::WalInstance::new].
//! A default configuration is used if none is provided.
//! The configuration is persisted and restored from the next time you create the WAL instance with the same name and
//! directory. The value of some items can be changed between WAL instance creations, while the persisted value is
//! always used for other items(like [TunableConfig::block_size]), unless the persisted configuration file is removed.
//! <br/>When possible, the value of a item may be sanitized to a proper value without returning a error.
//!
//! Note that, for performance consideration, persisting advanced LSN is designed to be asynchronized
//! ([sync::WalWriter::advance_lsn] and [async::WalWriter::advance_lsn] returns before the LSN is persisted), so there's
//! possibility the advanced LSN is lost if crash occurs, which means [sync::WalReader::get_min_lsn] and
//! [async::WalReader::get_min_lsn] may get a smaller value and some unneeded log entries are retrieved.

use crate::{CacheAligned, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, atomic::AtomicU64};

const MAX_MANI_ROT: u32 = 2;
const MAX_MANI_ROT_SIZE: u64 = 64 * 1024;

/// A log entry, with a LSN (Log Sequence Number) and a payload of bytes.
pub struct LogEntry {
    pub lsn: u64,
    pub payload: Bytes,
}

#[derive(Debug)]
struct LogRingBufferCell {
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<LogEntry>>,
}

#[derive(Debug)]
struct LogRingBuffer {
    lsn_base: u64,
    buf: Box<[LogRingBufferCell]>,
    mask: usize,
    head: CacheAligned<AtomicUsize>,
    tail: CacheAligned<AtomicUsize>,
}

unsafe impl Send for LogRingBuffer {}
unsafe impl Sync for LogRingBuffer {}

impl LogRingBuffer {
    pub fn new(size_order: u8, lsn_base: u64) -> Self {
        let size = 1usize << size_order;
        let mut buf = Vec::with_capacity(size);
        for i in 0..size {
            buf.push(LogRingBufferCell {
                seq: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Self {
            lsn_base,
            buf: buf.into_boxed_slice(),
            mask: size - 1,
            head: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
            tail: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
        }
    }

    pub fn push(&self, log: Bytes) -> Result<u64> {
        let mut tail = self.tail.0.load(Ordering::Relaxed);

        loop {
            let cell = &self.buf[tail & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let diff = seq as isize - tail as isize;

            if diff == 0 {
                match self.tail.0.compare_exchange_weak(
                    tail,
                    tail + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let lsn = self.lsn_base + seq as u64;
                        let entry = LogEntry { lsn, payload: log };
                        unsafe { (*cell.data.get()).write(entry) };
                        cell.seq.store(tail + 1, Ordering::Release);
                        return Ok(lsn);
                    }
                    Err(actual) => {
                        tail = actual;
                    }
                }
            } else if diff < 0 {
                return Err(Error::RingBufferFull);
            } else {
                tail = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    pub fn pop_for_sc(&self) -> Option<LogEntry> {
        let head = self.head.0.load(Ordering::Relaxed);
        let cell = &self.buf[head & self.mask];
        let seq = cell.seq.load(Ordering::Acquire);

        if seq == head + 1 {
            let value = unsafe { (*cell.data.get()).assume_init_read() };
            cell.seq.store(head + self.mask + 1, Ordering::Release);
            self.head.0.store(head + 1, Ordering::Relaxed);
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct ControlBlock {
    running: AtomicBool,
    min_lsn: AtomicU64,
    max_lsn: AtomicU64,
    flush_lsn: Arc<AtomicU64>,
    rbuf: LogRingBuffer,
    config: TunableConfig,
}

impl ControlBlock {
    fn new(min_lsn: u64, max_lsn: u64, flush_lsn: Arc<AtomicU64>, config: TunableConfig) -> Self {
        let base_lsn = max_lsn + 1;
        Self {
            running: AtomicBool::new(false),
            min_lsn: AtomicU64::new(min_lsn),
            max_lsn: AtomicU64::new(max_lsn),
            flush_lsn,
            rbuf: LogRingBuffer::new(config.ringbuf_size_order, base_lsn),
            config,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub(crate) enum Fullness {
    Full,
    First,
    Middle,
    Last,
}

impl TryFrom<u8> for Fullness {
    type Error = ();
    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        match v {
            0 => Ok(Fullness::Full),
            1 => Ok(Fullness::First),
            2 => Ok(Fullness::Middle),
            3 => Ok(Fullness::Last),
            _ => Err(()),
        }
    }
}

/*#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum WalFlags {
    Zipped = 0x01,
}*/

pub(crate) struct DiskLogEntry {
    payload: Bytes,
    _crc: u32,
    _len: u32,
    lsn: u64,
    _ver: u8,
    fullness: Fullness,
    _flags: u8,
    _resv: u8,
}

const LOG_HEAD_SIZE: usize = 20;

impl DiskLogEntry {
    fn encode(buf: &mut Vec<u8>, lsn: u64, payload: Bytes, fullness: Fullness) {
        let ostart = buf.len();
        buf.put_bytes(0, 4);
        buf.put_u32_ne(payload.len() as u32);
        buf.put_u64_ne(lsn);
        buf.put_u8(0);
        buf.put_u8(fullness as u8);
        buf.put_u8(0);
        buf.put_u8(0);
        buf.extend_from_slice(&payload);
        let crc = crc32c::crc32c(&buf[(ostart + 4)..]);
        buf[ostart..(ostart + 4)].copy_from_slice(&crc.to_ne_bytes());
    }

    fn decode_err(details: &str) -> Result<Self> {
        Err(Error::DecodingFailed("DiskLogEntry".into(), details.into()))
    }

    fn decode(src: &mut Bytes) -> Result<Self> {
        let olen = src.remaining();
        if olen < LOG_HEAD_SIZE {
            return Self::decode_err("Buffer is too short to decode");
        }

        let recv_crc = src.get_u32_ne();
        let entry_len = u32::from_ne_bytes(src[..4].try_into().unwrap()) as usize;
        if entry_len as usize > olen - LOG_HEAD_SIZE {
            return Self::decode_err("len field is too large");
        }

        let calc_crc = crc32c::crc32c(&src[..(LOG_HEAD_SIZE - 4 + entry_len)]);
        if calc_crc != recv_crc {
            return Self::decode_err("CRC check failed");
        }
        src.get_u32_ne(); // consume len field

        let lsn = src.get_u64_ne();
        let ver = src.get_u8();

        let fullness = Fullness::try_from(src.get_u8()).map_err(|_e| {
            Error::DecodingFailed("DiskLogEntry".into(), "Invalid fullness".into())
        })?;

        let flags = src.get_u8();
        let resv = src.get_u8();
        let payload = src.split_to(entry_len);

        Ok(Self {
            payload,
            _crc: recv_crc,
            _len: entry_len as u32,
            lsn,
            _ver: ver,
            fullness,
            _flags: flags,
            _resv: resv,
        })
    }
}

pub(crate) struct DiskBlockHeader {
    _crc: u32,
    _ver: u8,
    _resv: [u8; 3],
    min_lsn: u64,       // min LSN in this block
    max_lsn: u64,       // max LSN in this block
    whole_min_lsn: u64, // min LSN in the whole WAL instance
}

const BLOCK_HEAD_SIZE: usize = std::mem::size_of::<DiskBlockHeader>();

impl DiskBlockHeader {
    fn encode(buf: &mut Vec<u8>, min_lsn: u64) {
        buf.put_u64_ne(0);
        buf.put_u64_ne(min_lsn);
        buf.put_u64_ne(0); // place holder for max_lsn
        buf.put_u64_ne(0); // place holder for whole_min_lsn
    }

    fn decode_err(details: &str) -> Result<Self> {
        Err(Error::DecodingFailed(
            "DiskBlockHeader".into(),
            details.into(),
        ))
    }

    fn decode(src: &mut Bytes) -> Result<Self> {
        if src.remaining() < BLOCK_HEAD_SIZE {
            return Self::decode_err("Buffer is too short to decode");
        }

        let recv_crc = src.get_u32_ne();
        let calc_crc = crc32c::crc32c(&src[..(BLOCK_HEAD_SIZE - 4)]);
        if calc_crc != recv_crc {
            return Err(Error::CrcCheckFailed);
        }

        let ver = src.get_u8();
        if ver != 0 {
            return Self::decode_err("Invalid ver");
        }

        let mut resv = [0u8; 3];
        src.copy_to_slice(&mut resv);
        if resv != [0u8; 3] {
            return Self::decode_err("Invalid resv");
        }

        let min_lsn = src.get_u64_ne();
        let max_lsn = src.get_u64_ne();
        let whole_min_lsn = src.get_u64_ne();

        Ok(Self {
            _crc: recv_crc,
            _ver: ver,
            _resv: resv,
            min_lsn,
            max_lsn,
            whole_min_lsn,
        })
    }
}

struct DiskManiEntry {
    _crc: u32,
    _ver: u8,
    _resv: [u8; 3],
    min_lsn: u64,
}

impl Default for DiskManiEntry {
    fn default() -> Self {
        Self {
            _crc: 0u32,
            _ver: 0,
            _resv: [0u8; 3],
            min_lsn: 0,
        }
    }
}

const MANI_ENTRY_SIZE: usize = std::mem::size_of::<DiskManiEntry>();

impl DiskManiEntry {
    fn encode(buf: &mut BytesMut, min_lsn: u64) {
        buf.put_u64_ne(0);
        buf.put_u64_ne(min_lsn);
        let crc = crc32c::crc32c(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_ne_bytes());
    }

    fn decode_err(details: &str) -> Result<Self> {
        Err(Error::DecodingFailed(
            "DiskManiEntry".into(),
            details.into(),
        ))
    }

    fn decode(src: &mut Bytes) -> Result<Self> {
        if src.remaining() < MANI_ENTRY_SIZE {
            return Self::decode_err("Buffer is too short to decode");
        }

        let recv_crc = src.get_u32_ne();
        let calc_crc = crc32c::crc32c(&src[..(MANI_ENTRY_SIZE - 4)]);
        if calc_crc != recv_crc {
            return Self::decode_err("CRC check failed");
        }

        let ver = src.get_u8();

        let mut resv = [0u8; 3];
        src.copy_to_slice(&mut resv);

        let min_lsn = src.get_u64_ne();

        Ok(Self {
            _crc: recv_crc,
            _ver: ver,
            _resv: resv,
            min_lsn,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
/// Tunable configuration items for WAL instance.
pub struct TunableConfig {
    /// The size indicator (power of 2) of the ring buffer used by the flushing task to receive log entries.
    /// The default value is 8, which means the size is 256 entries.
    pub ringbuf_size_order: u8,
    /// The threshold size to switch to new data rotation. The default is 32MB (`32*1024*1024`).
    pub max_data_rot_size: u64,
    /// The maximum allowed size of a log entry. The default is 1MB (`1024*1024`).
    pub max_log_entry_size: u32,
    /// The block size. Log data is organized as logical blocks to quicker locate the log entry.
    /// It must be a power of 2, at range of [4KB, 256KB]. The default block size is 64KB (`64*1024`).
    pub block_size: u32,
    /// The buffer size to flush the log data in bulk. It should be both a multiple of the block size and
    /// a power of 2, at the range of [block_size, 1MB]. The default block size is 256KB (`256*1024`).
    pub bulk_flush_size: u32,
    /// Number of checks for new log entries before flushing an non-full flushing buffer. The default
    /// value is 5.
    pub checks_bef_flush_log: u32,
    /// Number of checks for new advanced LSN before persisting the advanced LSN. The default value is 0.
    pub checks_bef_flush_adv_lsn: u32,
}

impl TunableConfig {
    fn from_opt(config: Option<TunableConfig>) -> Self {
        if let Some(mut config) = config {
            config.sanitize();
            config
        } else {
            TunableConfig::default()
        }
    }

    fn sanitize(&mut self) {
        self.block_size = if let Some(size) = self.block_size.checked_next_power_of_two() {
            size
        } else {
            256 * 1024
        };
        if self.block_size < 4096 {
            self.block_size = 4096;
        } else if self.block_size > 256 * 1024 {
            self.block_size = 256 * 1024;
        }
        if self.bulk_flush_size % self.block_size != 0 {
            self.bulk_flush_size =
                ((self.bulk_flush_size + self.block_size - 1) / self.block_size) * self.block_size;
        }
        self.bulk_flush_size = if let Some(size) = self.bulk_flush_size.checked_next_power_of_two()
        {
            size
        } else {
            1024 * 1024
        };
        if self.bulk_flush_size < self.block_size {
            self.bulk_flush_size = self.block_size;
        } else if self.bulk_flush_size > 1024 * 1024 {
            self.bulk_flush_size = 1024 * 1024;
        }
    }
}

impl Default for TunableConfig {
    fn default() -> Self {
        Self {
            ringbuf_size_order: 8,
            max_data_rot_size: 32 * 1024 * 1024,
            max_log_entry_size: 1024 * 1024,
            block_size: 64 * 1024,
            bulk_flush_size: 256 * 1024,
            checks_bef_flush_log: 5,
            checks_bef_flush_adv_lsn: 0,
        }
    }
}

#[cfg(feature = "async")]
pub mod r#async;
pub mod sync;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;

    fn test_encode_decode_disk_mani_entry() {
        let mut buf = BytesMut::with_capacity(MANI_ENTRY_SIZE);
        DiskManiEntry::encode(&mut buf, 0x1234567890abcdef);
        let mut buf = buf.freeze();
        assert_eq!(buf.len(), MANI_ENTRY_SIZE);
        let decoded = DiskManiEntry::decode(&mut buf).unwrap();
        assert_eq!(0x1234567890abcdef, decoded.min_lsn);
    }

    fn test_encode_decode_disk_block_header() {
        let mut buf = Vec::with_capacity(BLOCK_HEAD_SIZE);
        DiskBlockHeader::encode(&mut buf, 0x1234567890abcdefu64);
        buf[16..24].copy_from_slice(&0xfedcba9876543210u64.to_ne_bytes());
        buf[24..32].copy_from_slice(&0x7777777788888888u64.to_ne_bytes());
        let crc = crc32c::crc32c(&buf[4..32]);
        buf[0..4].copy_from_slice(&crc.to_ne_bytes());
        assert_eq!(buf.len(), BLOCK_HEAD_SIZE);
        let mut bytes = Bytes::from(buf);
        let decoded = DiskBlockHeader::decode(&mut bytes).unwrap();
        assert_eq!(0x1234567890abcdefu64, decoded.min_lsn);
        assert_eq!(0xfedcba9876543210u64, decoded.max_lsn);
        assert_eq!(0x7777777788888888u64, decoded.whole_min_lsn);
    }

    fn gen_random_bytes(max_len: usize) -> Bytes {
        let mut rng = rand::rng();
        let len = rng.random_range(0..=max_len);
        let mut buf = BytesMut::with_capacity(len);
        unsafe { buf.set_len(len) };
        rng.fill(&mut buf);
        buf.freeze()
    }

    fn test_encode_decode_disk_log_entry() {
        let p1 = gen_random_bytes(64 * 1024);
        let p2 = gen_random_bytes(64 * 1024);
        let len = 2 * LOG_HEAD_SIZE + p1.len() + p2.len();
        let mut buf = Vec::with_capacity(len);
        DiskLogEntry::encode(&mut buf, 0x1234567890abcdefu64, p1.clone(), Fullness::Full);
        DiskLogEntry::encode(
            &mut buf,
            0xfedcba9876543210u64,
            p2.clone(),
            Fullness::Middle,
        );
        let mut bytes = Bytes::from(buf);
        let decoded1 = DiskLogEntry::decode(&mut bytes).unwrap();
        let decoded2 = DiskLogEntry::decode(&mut bytes).unwrap();
        assert_eq!(0x1234567890abcdefu64, decoded1.lsn);
        assert_eq!(p1.len() as u32, decoded1._len);
        assert_eq!(Fullness::Full, decoded1.fullness);
        assert_eq!(p1, decoded1.payload);
        assert_eq!(0xfedcba9876543210u64, decoded2.lsn);
        assert_eq!(p2.len() as u32, decoded2._len);
        assert_eq!(Fullness::Middle, decoded2.fullness);
        assert_eq!(p2, decoded2.payload);
    }

    #[test]
    fn test_encode_decode() {
        test_encode_decode_disk_mani_entry();
        test_encode_decode_disk_block_header();
        test_encode_decode_disk_log_entry();
    }
}
