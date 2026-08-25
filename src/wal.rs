//! A simple WAL (Write Ahead Log) implementation.
//!

use crate::{CacheAligned, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, atomic::AtomicU64};

const MAX_MANI_ROT: u32 = 2;
const MAX_MANI_ROT_SIZE: u64 = 1024 * 1024;
const MAX_DATA_ROT_SIZE: u64 = 32 * 1024 * 1024;
const BLOCK_SIZE: usize = 64 * 1024;
const BLOCK_MASK: usize = BLOCK_SIZE - 1;

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
    pub fn new(size_order: u8, lsn_base: u64) -> Result<Self> {
        let size = 1usize << size_order;
        let max_isize = isize::MAX as usize;
        if size > max_isize {
            return Err(Error::InvalidArgument("`size_order` is too large".into()));
        }

        let mut buf = Vec::with_capacity(size);
        for i in 0..size {
            buf.push(LogRingBufferCell {
                seq: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Ok(Self {
            lsn_base,
            buf: buf.into_boxed_slice(),
            mask: size - 1,
            head: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
            tail: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
        })
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
}

impl ControlBlock {
    fn new(min_lsn: u64, max_lsn: u64, flush_lsn: Arc<AtomicU64>) -> Self {
        let base_lsn = max_lsn + 1;
        Self {
            running: AtomicBool::new(false),
            min_lsn: AtomicU64::new(min_lsn),
            max_lsn: AtomicU64::new(max_lsn),
            flush_lsn,
            rbuf: LogRingBuffer::new(8, base_lsn).unwrap(),
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

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum WalFlags {
    Zipped = 0x01,
}

pub(crate) struct DiskLogEntry {
    payload: Bytes,
    crc: u32,
    len: u32,
    lsn: u64,
    ver: u8,
    fullness: Fullness,
    flags: u8,
    resv: u8,
}

const LOG_HEAD_SIZE: usize = 20;

impl DiskLogEntry {
    fn new(lsn: u64, payload: Bytes, fullness: Fullness) -> Self {
        let len = payload.len();
        Self {
            payload: payload,
            crc: 0,
            len: len as u32,
            lsn,
            ver: 0,
            fullness,
            flags: 0,
            resv: 0,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        let ostart = buf.len();
        buf.put_bytes(0, 4);
        buf.put_u32_ne(self.len);
        buf.put_u64_ne(self.lsn);
        buf.put_u8(self.ver);
        buf.put_u8(self.fullness as u8);
        buf.put_u8(self.flags as u8);
        buf.put_u8(self.resv);
        buf.extend_from_slice(&self.payload);
        let crc = crc32c::crc32c(&buf[(ostart + 4)..]);
        buf[ostart..(ostart + 4)].copy_from_slice(&crc.to_ne_bytes());
    }

    fn encode_by_params(buf: &mut BytesMut, lsn: u64, payload: Bytes, fullness: Fullness) {
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
            crc: recv_crc,
            len: entry_len as u32,
            lsn,
            ver,
            fullness,
            flags,
            resv,
        })
    }
}

pub(crate) struct DiskBlockHeader {
    crc: u32,
    ver: u8,
    resv: [u8; 3],
    min_lsn: u64,
    max_lsn: u64,
}

impl Default for DiskBlockHeader {
    fn default() -> Self {
        Self {
            crc: 0u32,
            ver: 0,
            resv: [0u8; 3],
            min_lsn: 0,
            max_lsn: 0,
        }
    }
}

const BLOCK_HEAD_SIZE: usize = std::mem::size_of::<DiskBlockHeader>();

impl DiskBlockHeader {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_bytes(0, 4);
        buf.put_u8(self.ver);
        buf.put(&self.resv[..]);
        buf.put_u64_ne(self.min_lsn);
        buf.put_u64_ne(self.max_lsn);
        let crc = crc32c::crc32c(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_ne_bytes());
    }

    fn encode_by_params(buf: &mut BytesMut, min_lsn: u64) {
        buf.put_u64_ne(0);
        buf.put_u64_ne(min_lsn);
        buf.put_u64_ne(0);
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

        Ok(Self {
            crc: recv_crc,
            ver,
            resv,
            min_lsn,
            max_lsn,
        })
    }
}

struct DiskManiEntry {
    crc: u32,
    ver: u8,
    resv: [u8; 3],
    min_lsn: u64,
    min_rot: u32,
    max_rot: u32,
}

impl Default for DiskManiEntry {
    fn default() -> Self {
        Self {
            crc: 0u32,
            ver: 0,
            resv: [0u8; 3],
            min_lsn: 0,
            min_rot: 0,
            max_rot: 0,
        }
    }
}

const MANI_ENTRY_SIZE: usize = std::mem::size_of::<DiskManiEntry>();

impl DiskManiEntry {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_bytes(0, 4);
        buf.put_u8(self.ver);
        buf.put(&self.resv[..]);
        buf.put_u64_ne(self.min_lsn);
        buf.put_u32_ne(self.min_rot);
        buf.put_u32_ne(self.max_rot);
        let crc = crc32c::crc32c(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_ne_bytes());
    }

    fn encode_by_params(buf: &mut BytesMut, min_lsn: u64) {
        buf.put_u64_ne(0);
        buf.put_u64_ne(min_lsn);
        buf.put_u32_ne(0);
        buf.put_u32_ne(0);
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
        let min_rot = src.get_u32_ne();
        let max_rot = src.get_u32_ne();

        Ok(Self {
            crc: recv_crc,
            ver,
            resv,
            min_lsn,
            min_rot,
            max_rot,
        })
    }
}

/// Currently not implemented.
pub struct TunnableConfig {
    pub max_mani_rot: u32,
    pub max_mani_rot_size: u64,
    pub max_data_rot: u32,
    pub max_data_rot_size: u64,
    pub log_flush_sleep_thres: u32,
    pub log_flush_sleep_interval: u32,
    pub log_flush_disturb_thres: u32,
}

#[cfg(feature = "async")]
pub mod r#async;
pub mod sync;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;

    fn test_encode_decode_disk_mani_entry() {
        let mut entry = DiskManiEntry::default();
        entry.min_lsn = 0x1234567890abcdef;
        entry.max_rot = 0x12345678;
        entry.min_rot = 0x87654321;
        let mut buf = BytesMut::with_capacity(MANI_ENTRY_SIZE);
        entry.encode(&mut buf);
        let mut buf = buf.freeze();
        assert_eq!(buf.len(), MANI_ENTRY_SIZE);
        let decoded = DiskManiEntry::decode(&mut buf).unwrap();
        assert_eq!(entry.min_lsn, decoded.min_lsn);
        assert_eq!(entry.min_lsn, decoded.min_lsn);
        assert_eq!(entry.min_lsn, decoded.min_lsn);
    }

    fn test_encode_decode_disk_block_header() {
        let mut entry = DiskBlockHeader::default();
        entry.min_lsn = 0x1234567890abcdef;
        entry.max_lsn = 0xfedcba9876543210;
        let mut buf = BytesMut::with_capacity(BLOCK_HEAD_SIZE);
        entry.encode(&mut buf);
        let mut buf = buf.freeze();
        assert_eq!(buf.len(), BLOCK_HEAD_SIZE);
        let decoded = DiskBlockHeader::decode(&mut buf).unwrap();
        assert_eq!(entry.min_lsn, decoded.min_lsn);
        assert_eq!(entry.max_lsn, decoded.max_lsn);
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
        let e1 = DiskLogEntry::new(0x1234567890abcdef, p1, Fullness::Full);
        let e2 = DiskLogEntry::new(0xfedcba9876543210, p2, Fullness::Middle);
        let mut buf = BytesMut::with_capacity(len);
        e1.encode(&mut buf);
        e2.encode(&mut buf);
        let mut buf = buf.freeze();
        let decoded1 = DiskLogEntry::decode(&mut buf).unwrap();
        let decoded2 = DiskLogEntry::decode(&mut buf).unwrap();
        assert_eq!(e1.lsn, decoded1.lsn);
        assert_eq!(e1.len, decoded1.len);
        assert_eq!(e1.fullness, decoded1.fullness);
        assert_eq!(e1.payload, decoded1.payload);
        assert_eq!(e2.lsn, decoded2.lsn);
        assert_eq!(e2.len, decoded2.len);
        assert_eq!(e2.fullness, decoded2.fullness);
        assert_eq!(e2.payload, decoded2.payload);
    }

    #[test]
    fn test_encode_decode() {
        test_encode_decode_disk_mani_entry();
        test_encode_decode_disk_block_header();
        test_encode_decode_disk_log_entry();
    }
}
