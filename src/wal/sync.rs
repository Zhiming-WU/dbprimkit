//! A simple WAL (Write Ahead Log) implementation used in non-async code.
//!
//! Here is an example for using it for appending log entries;
//! ```rust
//! use bytes::Bytes;
//! use dbprimkit::wal::sync::StdFileWalInstance;
//!
//! fn main() {
//!     let inst = StdFileWalInstance::new("my_wal", "/tmp", None).unwrap();
//!     let writer = inst.open_wal_writer().unwrap();
//!     // Append log entries
//!     let lsn1 = writer.append(bytes::Bytes::from("my first log")).unwrap();
//!     let lsn2 = writer.append(bytes::Bytes::from("my second log")).unwrap();
//!     // Check the maximum LSN of log entries which have been persisted
//!     let max_lsn = writer.get_max_lsn();
//!     // Notify the WAL that logs below or equal to `max_lsn` are no long needed.
//!     writer.advance_lsn(max_lsn).unwrap();
//! }
//! ```
//!
//! Here is am example for using it for retrieving log entries (i.g. for playbacks).
//! ```rust
//! use dbprimkit::wal::sync::StdFileWalInstance;
//!
//! fn main() {
//!     let inst = StdFileWalInstance::new("my_wal", "/tmp", None).unwrap();
//!     let reader = inst.open_wal_reader().unwrap();
//!     // Check the LSN range.
//!     let min_lsn = reader.get_max_lsn();
//!     let max_lsn = reader.get_max_lsn();
//!     // Retrieve log entries
//!     let iter = reader.get_log_iter(min_lsn).unwrap();
//!     for item in iter {
//!         if let Ok(log) = item {
//!             // Use the log entry
//!         }
//!     }
//! }
//! ```
use super::*;
use crate::Error::{
    CorruptFile, FunctionNotRunning, InvalidArgument, IoError, LogEntryTooLarge, MiscError,
};
use crate::appender::sync::*;
use crate::io::{IoBackend, OpenOptions, StdFileIoBackend};
use crate::{AlignedBuffer, Error, Result};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

struct WalDataRotNameProviderImpl<IO: IoBackend> {
    dir: PathBuf,
    prefix: String,
    flush_lsn: Arc<AtomicU64>,
    _m: PhantomData<IO>,
}

impl<IO: IoBackend> WalDataRotNameProviderImpl<IO> {
    fn new<P: AsRef<Path>>(dir: P, prefix: &str, flush_lsn: Arc<AtomicU64>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            prefix: prefix.to_string(),
            flush_lsn,
            _m: PhantomData,
        }
    }

    fn gen_rot_name(&self, rot_no: u32) -> String {
        let next_lsn = self.flush_lsn.load(Ordering::Acquire) + 1;
        format!("{}{:010}.{:020}", self.prefix, rot_no, next_lsn)
    }

    fn parse_rot_name(&self, name: &str) -> Option<(u32, u64)> {
        if !name.starts_with(&self.prefix) {
            return None;
        }
        let tail = &name[self.prefix.len()..];
        if tail.len() != 31 || tail.as_bytes()[10] != b'.' {
            return None;
        }
        let rot_no: u32 = (&tail[..10]).parse().ok()?;
        let next_lsn: u64 = (&tail[11..]).parse().ok()?;
        Some((rot_no, next_lsn))
    }

    fn list_rot_names(&self) -> Result<Vec<String>> {
        IO::list_files(
            &self.dir,
            Some(|name: &str| self.parse_rot_name(name).is_some()),
        )
    }

    fn restore_latest_rot_info(&self) -> Result<Option<(u32, u64, String)>> {
        let mut names = self.list_rot_names()?;
        if names.is_empty() {
            return Ok(None);
        }
        let name = names.remove(names.len() - 1);
        if let Some((rot_no, next_lsn)) = self.parse_rot_name(&name) {
            return Ok(Some((rot_no, next_lsn, name)));
        }
        Err(MiscError(format!(
            "Failed to restore rotation number from `{}`",
            name
        )))
    }
}

struct WalDataRotNameProvider<IO: IoBackend> {
    inner: Arc<WalDataRotNameProviderImpl<IO>>,
}

impl<IO: IoBackend> RotNameProvider<IO> for WalDataRotNameProvider<IO> {
    fn gen_rot_name(&self, rot_no: u32) -> String {
        self.inner.gen_rot_name(rot_no)
    }

    fn list_rot_names(&self) -> Result<Vec<String>> {
        self.inner.list_rot_names()
    }

    fn restore_latest_rot_info(&self) -> Result<Option<(u32, String)>> {
        let res = self.inner.restore_latest_rot_info()?;
        Ok(res.map(|(rot_no, _next_lsn, name)| (rot_no, name)))
    }
}

fn read_latest_manifest_entry<IO: IoBackend>(
    reader: &mut IO::Reader,
    file_size: u64,
) -> Result<DiskManiEntry> {
    let entry_size = MANI_ENTRY_SIZE as u64;
    if file_size < entry_size {
        return Err(CorruptFile("The manifest file is too small".into()));
    }
    // if the last entry is corrupt, read the last non-corrupt one.
    let offset = (file_size / entry_size - 1) * entry_size;
    reader.seek(std::io::SeekFrom::Start(offset))?;
    let mut buf = BytesMut::zeroed(entry_size as usize);
    reader.read_exact(&mut buf)?;
    let entry = DiskManiEntry::decode(&mut buf.freeze())?;
    Ok(entry)
}

fn restore_lsn_from_data<IO: IoBackend>(
    reader: &mut IO::Reader,
    file_size: u64,
    block_size: u64,
) -> Result<(u64, u64)> {
    if file_size < block_size {
        return Err(CorruptFile("The data file is too small".into()));
    }
    if file_size % block_size != 0 {
        return Err(CorruptFile(
            "File size is not multiple of block size".into(),
        ));
    }
    let offset = file_size - block_size;
    reader.seek(std::io::SeekFrom::Start(offset))?;
    let mut buf = BytesMut::zeroed(BLOCK_HEAD_SIZE as usize);
    reader.read_exact(&mut buf)?;
    let header = DiskBlockHeader::decode(&mut buf.freeze())?;
    Ok((header.whole_min_lsn, header.max_lsn))
}

fn restore_lsn<IO: IoBackend>(
    mani_rotator: &DefaultRotator<IO>,
    data_rotator: &DefaultRotator<IO, WalDataRotNameProvider<IO>>,
    block_size: u64,
) -> Result<(u64, u64)> {
    match mani_rotator.restore_latest_rot(false)? {
        None => Ok((0, 0)),
        Some((mut reader, file_size)) => {
            let mut min_lsn = if file_size == 0 {
                0
            } else {
                let mani_entry = read_latest_manifest_entry::<IO>(&mut reader, file_size)?;
                mani_entry.min_lsn
            };
            let (min_lsn_from_data, max_lsn) = match data_rotator.restore_latest_rot(false)? {
                None => return Err(MiscError("Missing data rotations".into())),
                Some((mut data_reader, data_file_size)) => {
                    if file_size == 0 {
                        (0, 0)
                    } else {
                        restore_lsn_from_data::<IO>(&mut data_reader, data_file_size, block_size)?
                    }
                }
            };
            min_lsn = min_lsn.max(min_lsn_from_data);
            if min_lsn == 0 && max_lsn > 0 {
                min_lsn = 1;
            }
            Ok((min_lsn, max_lsn))
        }
    }
}

fn remove_no_use_rots<IO: IoBackend>(
    data_rotator: &DefaultRotator<IO, WalDataRotNameProvider<IO>>,
    data_rnp: &WalDataRotNameProviderImpl<IO>,
    min_lsn: u64,
) -> Result<u64> {
    let mut next_no_use_lsn = u64::MAX;
    let names = data_rnp.list_rot_names()?;
    for (idx, name) in names.iter().enumerate() {
        if idx > 0 {
            let (_, lsn) = data_rnp.parse_rot_name(&name).unwrap();
            if lsn <= min_lsn {
                data_rotator.remove_rot(&names[idx - 1])?;
            } else {
                next_no_use_lsn = lsn;
                break;
            }
        }
    }
    Ok(next_no_use_lsn)
}

fn open_reader<IO: IoBackend>(
    rotator: &DefaultRotator<IO, WalDataRotNameProvider<IO>>,
    rot_name: &str,
    block_size: u64,
) -> Result<(IO::Reader, u64)> {
    let (reader, fsize) = rotator.open_read_rot(&rot_name, false)?;
    if fsize % block_size as u64 != 0 {
        return Err(CorruptFile(
            "File size is not multiple of block size".into(),
        ));
    }
    Ok((reader, fsize))
}

fn new_wal_fields<IO: IoBackend>(
    inst: &WalInstance<IO>,
) -> Result<(
    DefaultRotator<IO>,
    DefaultRotator<IO, WalDataRotNameProvider<IO>>,
    Arc<WalDataRotNameProviderImpl<IO>>,
    Arc<AtomicU64>,
    u64,
    u64,
)> {
    let mani_rotator = DefaultRotator::<IO>::new(
        AsRef::<Path>::as_ref(&inst.dir),
        &format!("{}_walmani.", &inst.name),
        super::MAX_MANI_ROT,
        super::MAX_MANI_ROT_SIZE,
    )?;

    let flush_lsn = Arc::new(AtomicU64::new(0));
    let data_rnp = Arc::new(WalDataRotNameProviderImpl::<IO>::new(
        AsRef::<Path>::as_ref(&inst.dir),
        &format!("{}_waldata_", &inst.name),
        flush_lsn.clone(),
    ));

    let data_rotator = DefaultRotator::<IO, WalDataRotNameProvider<IO>>::with_rot_name_resolver(
        AsRef::<Path>::as_ref(&inst.dir),
        0,
        inst.config.max_data_rot_size,
        WalDataRotNameProvider::<IO> {
            inner: data_rnp.clone(),
        },
    )?;

    let (min_lsn, max_lsn) =
        restore_lsn(&mani_rotator, &data_rotator, inst.config.block_size as u64)?;
    Ok((
        mani_rotator,
        data_rotator,
        data_rnp,
        flush_lsn,
        min_lsn,
        max_lsn,
    ))
}

struct FlushingControlBlock<IO: IoBackend> {
    cb: Arc<ControlBlock>,
    mani_app: RotAppender<IO, DefaultRotator<IO>>,
    data_app: RotAppender<IO, DefaultRotator<IO, WalDataRotNameProvider<IO>>>,
    data_rnp: Arc<WalDataRotNameProviderImpl<IO>>,
    mani_buf: BytesMut,
    last_min_lsn: u64,
    next_no_use_lsn: u64,
    _aligned_buf: AlignedBuffer,
    data_buf: Vec<u8>,
    max_flushed_lsn: u64,
    last_filled_lsn: u64,
    block_size: usize,
    block_mask: usize,
    buf_size: usize,
    log_check_cnt: u32,
    adv_lsn_check_cnt: u32,
    checks_bef_flush_log: u32,
    checks_bef_flush_adv_lsn: u32,
}

impl<IO: IoBackend> FlushingControlBlock<IO> {
    fn flush_mani_entry(&mut self, min_lsn: u64) -> Result<()> {
        DiskManiEntry::encode(&mut self.mani_buf, min_lsn);
        self.mani_app
            .append(&self.mani_buf.split().freeze(), true)?;
        self.mani_app.flush()?;
        // reuse the memory
        self.mani_buf.reserve(MANI_ENTRY_SIZE);
        self.last_min_lsn = min_lsn;
        self.adv_lsn_check_cnt = 0;
        Ok(())
    }

    fn fix_block_headers(&mut self) {
        let len = self.data_buf.len();
        if len > 0 {
            let offset = (len - 1) & (!self.block_mask);
            self.data_buf[(offset + 16)..(offset + 24)]
                .copy_from_slice(&self.last_filled_lsn.to_ne_bytes());
            self.last_min_lsn = self.cb.min_lsn.load(Ordering::Acquire);
            self.data_buf[(offset + 24)..(offset + 32)]
                .copy_from_slice(&self.last_min_lsn.to_ne_bytes());
            let crc = crc32c::crc32c(&self.data_buf[(offset + 4)..(offset + 32)]);
            self.data_buf[offset..(offset + 4)].copy_from_slice(&crc.to_ne_bytes());
        }
    }

    fn flush_data_blocks(&mut self, rotatable: bool) -> Result<()> {
        self.cb
            .flush_lsn
            .store(self.last_filled_lsn, Ordering::Release);
        let min_lsn = self.cb.min_lsn.load(Ordering::Acquire);
        // For the ease of retrieving the persisted min lsn in the case that the WAL instance crashed
        // after switching to a zero-sized new rotation (in such case whole_min_lsn can not be read from
        // the latest rotation file), persist the min lsn in the manifest.
        if self.data_app.needs_new_rot() && min_lsn > self.last_min_lsn {
            self.flush_mani_entry(min_lsn)?;
        }
        let new_rot = self.data_app.append(&self.data_buf, rotatable)?;
        if new_rot {
            self.next_no_use_lsn =
                remove_no_use_rots(self.data_app.get_rotator(), &self.data_rnp, min_lsn)?;
        } else {
            self.data_app.flush()?;
        }
        // reuse memory
        self.data_buf.clear();
        if rotatable {
            self.cb
                .max_lsn
                .store(self.max_flushed_lsn, Ordering::Release);
        }
        self.log_check_cnt = 0;
        self.adv_lsn_check_cnt = 0;
        Ok(())
    }
}

impl<IO: IoBackend> Drop for FlushingControlBlock<IO> {
    fn drop(&mut self) {
        // Call it to avoid deallocating underlying memory by Vec, which should be done by AlignedBuffer.
        let buf = std::mem::take(&mut self.data_buf);
        let _ = buf.into_raw_parts();
    }
}

/// An WAL writer used to append and persist log entries.
#[derive(Debug)]
pub struct WalWriter<IO: IoBackend + Send> {
    inst: WalInstance<IO>,
    cb: Arc<ControlBlock>,
}

impl<IO: IoBackend + Send> WalWriter<IO> {
    fn new(inst: WalInstance<IO>) -> Result<Self> {
        let (mani_rotator, data_rotator, data_rnp, flush_lsn, min_lsn, max_lsn) =
            new_wal_fields(&inst)?;
        let cb = Arc::new(ControlBlock::new(min_lsn, max_lsn, flush_lsn, inst.config));
        let mani_app = RotAppender::new(mani_rotator, false)?;
        let data_app = RotAppender::new(data_rotator, true)?;
        let out = Self {
            inst,
            cb: cb.clone(),
        };
        cb.running.store(true, Ordering::Release);
        std::thread::spawn(move || {
            Self::flushing_task(cb, mani_app, data_app, data_rnp);
        });
        Ok(out)
    }

    fn flushing_task_inner(
        cb: Arc<ControlBlock>,
        mani_app: RotAppender<IO, DefaultRotator<IO>>,
        data_app: RotAppender<IO, DefaultRotator<IO, WalDataRotNameProvider<IO>>>,
        data_rnp: Arc<WalDataRotNameProviderImpl<IO>>,
    ) -> Result<()> {
        let interval = Duration::from_millis(1);
        /*const MANI_FLUSH_LSN_GAP_THRES: u64 = 100000;
        const MANI_FLUSH_SLEEP_THRES: u32 = 3600000;
        const DATA_FLUSH_SLEEP_THRES: u32 = 5;
        const DATA_FLUSH_DISTURB_THRES: u32 = 100000;*/

        let mut fcb = {
            let config = cb.config;
            let min_lsn = cb.min_lsn.load(Ordering::Acquire);
            let next_no_use_lsn = remove_no_use_rots(data_app.get_rotator(), &data_rnp, min_lsn)?;
            let aligned_buf = AlignedBuffer::new(config.bulk_flush_size as usize, 4096);
            let data_buf = aligned_buf.get_vec();
            let mut fcb = FlushingControlBlock::<IO> {
                mani_app,
                data_app,
                data_rnp,
                mani_buf: BytesMut::with_capacity(MANI_ENTRY_SIZE),
                last_min_lsn: min_lsn,
                next_no_use_lsn,
                _aligned_buf: aligned_buf,
                data_buf,
                max_flushed_lsn: cb.max_lsn.load(Ordering::Acquire),
                last_filled_lsn: 0,
                cb,
                log_check_cnt: 0,
                adv_lsn_check_cnt: 0,
                block_size: config.block_size as usize,
                block_mask: config.block_size as usize - 1,
                buf_size: config.bulk_flush_size as usize,
                checks_bef_flush_log: config.checks_bef_flush_log,
                checks_bef_flush_adv_lsn: config.checks_bef_flush_adv_lsn,
            };
            if min_lsn == 0 {
                fcb.flush_mani_entry(min_lsn)?;
            }
            fcb
        };

        let mut backoff_count = 0u32;
        while fcb.cb.running.load(Ordering::Acquire) {
            let mut noop = true;

            while let Some(LogEntry {
                lsn,
                payload: mut log,
            }) = fcb.cb.rbuf.pop_for_sc()
            {
                noop = false;
                let mut split = false;
                let mut handled = false;
                while !handled {
                    if (fcb.data_buf.len() & fcb.block_mask) == 0 {
                        fcb.fix_block_headers();
                        DiskBlockHeader::encode(&mut fcb.data_buf, lsn);
                    }
                    let left = log.len();
                    let bk_left = fcb.block_size - (fcb.data_buf.len() & fcb.block_mask);
                    if LOG_HEAD_SIZE + left <= bk_left {
                        // enough buffer to put log
                        let fullness = if split {
                            split = false;
                            Fullness::Last
                        } else {
                            Fullness::Full
                        };
                        DiskLogEntry::encode(&mut fcb.data_buf, lsn, log.split_to(left), fullness);
                        fcb.last_filled_lsn = lsn;
                        fcb.max_flushed_lsn = lsn;
                        handled = true;
                    } else if LOG_HEAD_SIZE < bk_left && (split || !fcb.data_app.needs_new_rot()) {
                        // split
                        let fullness = if split {
                            Fullness::Middle
                        } else {
                            split = true;
                            Fullness::First
                        };
                        let part_log = log.split_to(bk_left - LOG_HEAD_SIZE);
                        DiskLogEntry::encode(&mut fcb.data_buf, lsn, part_log, fullness);
                        fcb.last_filled_lsn = lsn;
                    } else if bk_left > 0 {
                        // fill zero
                        fcb.data_buf.put_bytes(0, bk_left);
                    }
                    if fcb.data_buf.len() == fcb.buf_size
                        || (fcb.data_app.needs_new_rot()
                            && fcb.data_buf.len() & fcb.block_mask == 0)
                    {
                        fcb.fix_block_headers();
                        fcb.flush_data_blocks(!split)?;
                    }
                }
            }

            // long time no flush. flush it now.
            if !fcb.data_buf.is_empty() && fcb.log_check_cnt >= fcb.checks_bef_flush_log {
                // fill up to block boundary
                let bk_len = fcb.data_buf.len() & fcb.block_mask;
                if bk_len != 0 {
                    fcb.data_buf.put_bytes(0, fcb.block_size - bk_len);
                }
                fcb.fix_block_headers();
                fcb.flush_data_blocks(true)?;
            }

            let min_lsn = fcb.cb.min_lsn.load(Ordering::Acquire);
            if min_lsn > fcb.last_min_lsn {
                if min_lsn >= fcb.next_no_use_lsn {
                    noop = false;
                    fcb.flush_mani_entry(min_lsn)?;
                    fcb.next_no_use_lsn =
                        remove_no_use_rots(fcb.data_app.get_rotator(), &fcb.data_rnp, min_lsn)?;
                }
                if fcb.adv_lsn_check_cnt >= fcb.checks_bef_flush_adv_lsn {
                    noop = false;
                    fcb.flush_mani_entry(min_lsn)?;
                }
            }

            if noop {
                if backoff_count <= 200 {
                    backoff_count += 1;
                }
                if backoff_count <= 100 {
                    std::hint::spin_loop();
                } else {
                    if backoff_count <= 200 {
                        thread::yield_now();
                    } else {
                        std::thread::sleep(interval);
                    }
                    if !fcb.data_buf.is_empty() {
                        fcb.log_check_cnt += 1;
                    }
                    if min_lsn > fcb.last_min_lsn {
                        fcb.adv_lsn_check_cnt += 1;
                    }
                }
            } else {
                backoff_count = 0;
            }
        }
        Ok(())
    }

    fn flushing_task(
        cb: Arc<ControlBlock>,
        mani_app: RotAppender<IO, DefaultRotator<IO>>,
        data_app: RotAppender<IO, DefaultRotator<IO, WalDataRotNameProvider<IO>>>,
        data_rnp: Arc<WalDataRotNameProviderImpl<IO>>,
    ) {
        if let Err(err) = Self::flushing_task_inner(cb.clone(), mani_app, data_app, data_rnp) {
            eprintln!("Error occurred in flushing thread: err={:?}", err);
            cb.running.store(false, Ordering::Release);
        }
    }

    fn check_running(&self) -> Result<()> {
        if !self.cb.running.load(Ordering::Acquire) {
            return Err(FunctionNotRunning("WAL flushing".into()));
        }
        Ok(())
    }

    /// Append a log entry. If it works OK, an LSN (Log Sequence Number) of `u64` is returned.
    /// Call [Self::get_max_lsn] to check whether this log entry has been persisted. (If the LSN returned
    /// by [Self::get_max_lsn] is larger than or equal to the LSN returned by this method, this log
    /// entry has been persisted.)
    pub fn append(&self, log: Bytes) -> Result<u64> {
        self.check_running()?;
        if log.len() > self.cb.config.max_log_entry_size as usize {
            return Err(LogEntryTooLarge(
                log.len() as u32,
                self.cb.config.max_log_entry_size,
            ));
        }
        self.cb.rbuf.push(log)
    }

    /// Get the maximum LSN of the log entries which have been persisted.
    pub fn get_max_lsn(&self) -> u64 {
        self.cb.max_lsn.load(Ordering::Acquire)
    }

    /// Notify the WAL writer that logs whose LSN is equal to or less than the `lsn` parameter
    /// are no longer needed and can be dropped from the persistence.
    pub fn advance_lsn(&self, lsn: u64) -> Result<()> {
        self.check_running()?;
        let curr_max_lsn = self.cb.max_lsn.load(Ordering::Acquire);
        let curr_min_lsn = self.cb.min_lsn.load(Ordering::Acquire);
        if lsn > curr_max_lsn {
            return Err(InvalidArgument("`lsn` is too large".into()));
        }
        if lsn > curr_min_lsn {
            self.cb.min_lsn.store(lsn + 1, Ordering::Release);
        }
        Ok(())
    }

    /// Stop using the WAL writer.
    pub fn stop(self) -> WalInstance<IO> {
        self.cb.running.store(false, Ordering::Release);
        self.inst
    }
}

/// An WAL reader used to retrieve log entries, for playbacks in recovery case, etc.
pub struct WalReader<IO: IoBackend> {
    inst: WalInstance<IO>,
    min_lsn: u64,
    max_lsn: u64,
    data_rotator: DefaultRotator<IO, WalDataRotNameProvider<IO>>,
    data_rnp: Arc<WalDataRotNameProviderImpl<IO>>,
}

/// An [LogIterator] from which logs can be retrieved.
pub struct LogIterator<'a, IO: IoBackend> {
    wal: &'a WalReader<IO>,
    reader: Option<IO::Reader>,
    cached: VecDeque<LogEntry>,
    middle_log_lsn: u64,
    middle_log_bufs: Vec<Bytes>,
    next_lsn: u64,
    rot_names: VecDeque<String>,
    config: TunableConfig,
}

impl<'a, IO: IoBackend> LogIterator<'a, IO> {
    // Read next block and parse it into the cache, until at least one log entry cached.
    // Return `false' if no log entry was read or `true` if any entry was read.
    fn read_next(&mut self) -> Result<bool> {
        //let debug = self.next_lsn > 300;
        let ocnt = self.cached.len();
        let block_size = self.config.block_size as usize;
        while let Some(reader) = &mut self.reader {
            let mut bbuf = BytesMut::with_capacity(block_size);
            unsafe { bbuf.set_len(block_size) };
            let res = reader.read_exact(&mut bbuf);
            match res {
                // reach EOF of current rotation, try next rotation
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                    let rot_name = self.rot_names.pop_front();
                    let rot_name = match rot_name {
                        None => {
                            if !self.middle_log_bufs.is_empty() {
                                return Err(CorruptFile("Unended paritial log".into()));
                            }
                            break;
                        }
                        Some(rot_name) => rot_name,
                    };
                    let (rd, _) =
                        open_reader(&self.wal.data_rotator, &rot_name, block_size as u64)?;
                    self.reader = Some(rd);
                    continue;
                }
                Err(err) => {
                    return Err(Error::from(err));
                }
                _ => {
                    let mut buf = bbuf.split().freeze();
                    buf.advance(BLOCK_HEAD_SIZE);
                    loop {
                        let log = DiskLogEntry::decode(&mut buf)?;
                        match log.fullness {
                            Fullness::Full => {
                                self.cached.push_back(LogEntry {
                                    lsn: log.lsn,
                                    payload: log.payload,
                                });
                            }
                            Fullness::First => {
                                if !self.middle_log_bufs.is_empty() {
                                    return Err(CorruptFile(
                                        "Partial log entry not finished".into(),
                                    ));
                                }
                                self.middle_log_lsn = log.lsn;
                                self.middle_log_bufs.push(log.payload);
                            }
                            fullness => {
                                if self.middle_log_bufs.is_empty() {
                                    return Err(CorruptFile(
                                        "Unexpected paritial log entry".into(),
                                    ));
                                } else {
                                    if self.middle_log_lsn != log.lsn {
                                        return Err(CorruptFile(
                                            "Unexpected paritial log lsn".into(),
                                        ));
                                    }
                                }
                                self.middle_log_bufs.push(log.payload);
                                if let Fullness::Last = fullness {
                                    let size: usize =
                                        self.middle_log_bufs.iter().map(|b| b.len()).sum();
                                    let mut payload = BytesMut::with_capacity(size);
                                    unsafe { payload.set_len(size) };
                                    let mut start = 0usize;
                                    for pl in self.middle_log_bufs.drain(..) {
                                        let end = start + pl.len();
                                        payload[start..end].copy_from_slice(&pl);
                                        start = end;
                                    }
                                    self.cached.push_back(LogEntry {
                                        lsn: log.lsn,
                                        payload: payload.freeze(),
                                    });
                                }
                            }
                        };
                        if buf.remaining() < LOG_HEAD_SIZE
                            || u64::from_ne_bytes(buf[0..8].try_into().unwrap()) == 0
                        {
                            break;
                        }
                    }
                    if ocnt != self.cached.len() {
                        break;
                    }
                }
            }
        }
        Ok(ocnt != self.cached.len())
    }
}

impl<'a, IO: IoBackend> Iterator for LogIterator<'a, IO> {
    type Item = Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.cached.pop_front() {
                if entry.lsn != self.next_lsn {
                    println!(
                        "debug: Unexpected LSN in log entry, exp={}, actual={}",
                        self.next_lsn, entry.lsn
                    );
                    return Some(Err(MiscError("Unexpected LSN in log entry".into())));
                }
                self.next_lsn += 1;
                return Some(Ok(entry));
            }

            match self.read_next() {
                Err(err) => return Some(Err(err)),
                Ok(false) => return None,
                Ok(true) => continue,
            }
        }
    }
}

impl<IO: IoBackend> WalReader<IO> {
    fn new(inst: WalInstance<IO>) -> Result<Self> {
        let (_, data_rotator, data_rnp, _, min_lsn, max_lsn) = new_wal_fields(&inst)?;
        Ok(Self {
            inst,
            min_lsn,
            max_lsn,
            data_rotator,
            data_rnp,
        })
    }

    /// Get an [LogIterator] from which log entriess can be retrieved.
    pub fn get_log_iter<'a>(&'a self, start_lsn: u64) -> Result<LogIterator<'a, IO>> {
        if start_lsn < self.min_lsn || start_lsn > self.max_lsn || self.min_lsn == 0 {
            return Err(InvalidArgument("start_lsn is out of range".into()));
        }

        let mut rot_names = VecDeque::from(self.data_rnp.list_rot_names()?);
        let mut rot_name: Option<String> = None;
        {
            let mut last_min_lsn = 0u64;
            let mut last_name: Option<String> = None;
            while let Some(name) = rot_names.pop_front() {
                let res = self.data_rnp.parse_rot_name(&name).unwrap();
                if last_min_lsn <= start_lsn && start_lsn < res.1 {
                    rot_names.push_front(name);
                    rot_name = last_name;
                    break;
                } else if start_lsn >= res.1 && rot_names.is_empty() {
                    rot_name = Some(name);
                    break;
                }
                last_min_lsn = res.1;
                last_name = Some(name);
            }
        }
        if rot_name.is_none() {
            return Err(MiscError(
                "Unable to find data rotation matching `start_lsn`".into(),
            ));
        }
        let rot_name = rot_name.unwrap();

        let config = self.inst.config;
        let block_size = config.block_size as usize;
        let mut cached = VecDeque::new();
        let (mut reader, fsize) = open_reader(&self.data_rotator, &rot_name, block_size as u64)?;
        let mut offset = 0u64;
        let mut bbuf = BytesMut::with_capacity(block_size);
        let mut hbuf = BytesMut::with_capacity(BLOCK_HEAD_SIZE);
        let mut middle_log_lsn = 0u64;
        let mut middle_log_bufs = Vec::new();
        while offset < fsize && fsize - offset >= config.block_size as u64 {
            reader.seek(SeekFrom::Start(offset))?;
            unsafe { hbuf.set_len(BLOCK_HEAD_SIZE) };
            reader.read_exact(&mut hbuf)?;
            let header = DiskBlockHeader::decode(&mut hbuf.split().freeze())?;
            if header.min_lsn <= start_lsn && start_lsn <= header.max_lsn {
                reader.seek(SeekFrom::Start(offset))?;
                unsafe { bbuf.set_len(block_size) };
                let res = reader.read_exact(&mut bbuf);
                match res {
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                        break;
                    }
                    err @ Err(_) => {
                        return Err(Error::from(err.unwrap_err()));
                    }
                    _ => {}
                }
                let mut buf = bbuf.split().freeze();
                buf.advance(BLOCK_HEAD_SIZE);
                loop {
                    let log = DiskLogEntry::decode(&mut buf)?;
                    if log.lsn < start_lsn {
                        continue;
                    }
                    match log.fullness {
                        Fullness::Full => {
                            cached.push_back(LogEntry {
                                lsn: log.lsn,
                                payload: log.payload,
                            });
                        }
                        Fullness::First => {
                            middle_log_lsn = log.lsn;
                            middle_log_bufs.push(log.payload);
                        }
                        _ => {
                            return Err(CorruptFile("Unexpected paritial log entry".into()));
                        }
                    };
                    if buf.remaining() < LOG_HEAD_SIZE
                        || u64::from_ne_bytes(buf[0..8].try_into().unwrap()) == 0
                    {
                        break;
                    }
                }
                let mut log_iter = LogIterator::<'a, IO> {
                    wal: self,
                    reader: Some(reader),
                    cached,
                    middle_log_lsn,
                    middle_log_bufs,
                    next_lsn: start_lsn,
                    rot_names,
                    config,
                };
                if log_iter.cached.is_empty() && !log_iter.read_next()? {
                    break;
                }
                return Ok(log_iter);
            }
            hbuf.reserve(BLOCK_HEAD_SIZE);
            unsafe { hbuf.set_len(BLOCK_HEAD_SIZE) };
            offset += block_size as u64;
        }

        Err(MiscError(
            "Unable to find log entry matching `start_lsn`".into(),
        ))
    }

    /// Get the maximum LSN.
    pub fn get_max_lsn(&self) -> u64 {
        if self.max_lsn < self.min_lsn {
            return 0;
        }
        self.max_lsn
    }

    /// Get the minimal LSN.
    pub fn get_min_lsn(&self) -> u64 {
        if self.max_lsn < self.min_lsn {
            return 0;
        }
        self.min_lsn
    }

    /// Finish using this [WalReader] and get the [WalInstance] which opened it.
    pub fn finish(self) -> WalInstance<IO> {
        self.inst
    }
}

/// An WAL instance used to open a [WalWriter] or a [WalReader].
#[derive(Debug)]
pub struct WalInstance<IO: IoBackend> {
    name: String,
    dir: PathBuf,
    config: TunableConfig,
    _m: PhantomData<IO>,
}

impl<IO: IoBackend> WalInstance<IO> {
    fn save_config<P: AsRef<Path>>(path: P, config: &TunableConfig) -> Result<()> {
        let opts = OpenOptions::new().with_create(true).with_write(true);
        let writer = IO::open_writer(path, &opts)?;
        serde_json::to_writer_pretty(writer, config).map_err(|err| {
            MiscError(format!(
                "Error occurred in saving WAL config, err={:?}",
                err
            ))
        })?;
        Ok(())
    }

    fn init_config<P: AsRef<Path>>(
        name: &str,
        dir: P,
        config_arg: Option<TunableConfig>,
    ) -> Result<TunableConfig> {
        let dir_path = dir.as_ref().to_path_buf();
        IO::check_path_is_dir(dir_path.as_path())?;
        let conf_path = dir_path.join(format!("{}.cfg", name));
        let opts = OpenOptions::new().with_read(true);
        let config: TunableConfig = match IO::open_reader(conf_path.as_path(), &opts) {
            Err(IoError(io_e)) if io_e.kind() == ErrorKind::NotFound => {
                let conf = TunableConfig::from_opt(config_arg);
                Self::save_config(conf_path.as_path(), &conf)?;
                conf
            }
            Ok(reader) => {
                let persisted = serde_json::from_reader::<IO::Reader, TunableConfig>(reader)
                    .map_err(|err| {
                        MiscError(format!(
                            "Error occurred in parsing WAL config, err={:?}",
                            err
                        ))
                    })?;
                if let Some(mut conf) = config_arg {
                    conf.block_size = persisted.block_size;
                    conf.sanitize();
                    if conf != persisted {
                        Self::save_config(conf_path.as_path(), &conf)?;
                    }
                    conf
                } else {
                    persisted
                }
            }
            Err(err) => return Err(err),
        };
        Ok(config)
    }

    /// Get the configuration.
    pub fn get_config(&self) -> TunableConfig {
        self.config
    }

    /// Create a [WalInstance].
    ///
    /// Parameters:
    /// - `name`: The name of the [WalInstance]. Data files are prefixed by this name.
    /// - `dir`: The directory where the data files are stored.
    /// - `config`: Optional configuration.
    pub fn new<P: AsRef<Path>>(name: &str, dir: P, config: Option<TunableConfig>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let config = Self::init_config(name, dir.as_path(), config)?;
        Ok(Self {
            name: name.into(),
            dir,
            config,
            _m: PhantomData,
        })
    }

    /// Open a WAL writer for appending log. Note that WAL writer
    /// and WAL reader of the same WAL instance can not be used
    /// at the same time.
    pub fn open_wal_writer(self) -> Result<WalWriter<IO>> {
        WalWriter::<IO>::new(self)
    }

    /// Open a WAL reader for reading log. Note that WAL reader
    /// and WAL writer of the same WAL instance can not be used
    /// at the same time.
    pub fn open_wal_reader(self) -> Result<WalReader<IO>> {
        WalReader::<IO>::new(self)
    }
}

/// The WAL instance type based on std::fs::File backend.
pub type StdFileWalInstance = WalInstance<StdFileIoBackend>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error::RingBufferFull;
    use rand::RngExt;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    fn do_test_clean(dir: &str, name: &str) {
        let dir = AsRef::<Path>::as_ref(dir).to_path_buf();
        let names =
            StdFileIoBackend::list_files(&dir, Some(|n: &str| n.starts_with(name))).unwrap();
        for n in names {
            let p = &dir.join(&n);
            StdFileIoBackend::remove_file(p).unwrap();
        }
    }

    fn test_wal(
        dir_arg: &str,
        name: &str,
        lsn_limit: u64,
        adv: bool,
        do_clean: bool,
        config: Option<TunableConfig>,
    ) {
        let dir = AsRef::<Path>::as_ref(dir_arg).to_path_buf();
        let mut rng = rand::rng();
        let mut bufs = Vec::<Bytes>::new();
        let long_thr_cnt = 2usize;
        let normal_thr_cnt = 14usize;
        for idx in 0..(long_thr_cnt + normal_thr_cnt) {
            let len = if idx < normal_thr_cnt {
                if idx == 5 {
                    0
                } else {
                    rng.random_range(0..4096)
                }
            } else {
                rng.random_range(256 * 1024..1024 * 1024)
            };
            let mut bm = BytesMut::with_capacity(len);
            let byte = rng.random::<u8>();
            //println!("debug: len={}, byte={:02x}", len, byte);
            bm.put_bytes(byte, len);
            bufs.push(bm.freeze());
        }

        let inst = StdFileWalInstance::new(name, dir.as_path(), config).unwrap();
        let writer = Arc::new(inst.open_wal_writer().unwrap());

        let wstart_time = Instant::now();
        let mut handles = vec![];
        for idx in 0..bufs.len() {
            let buf = bufs[idx].clone();
            let wal = writer.clone();
            handles.push(thread::spawn(move || {
                let mut logs = Vec::new();
                let mut lsn;
                let mut adv_lsn = 0u64;
                loop {
                    let res = wal.append(buf.clone());
                    lsn = match res {
                        Err(RingBufferFull) => {
                            thread::sleep(Duration::from_micros(1));
                            continue;
                        }
                        Err(_) => res.unwrap(),
                        Ok(lsn) => lsn,
                    };

                    logs.push(LogEntry {
                        lsn,
                        payload: buf.clone(),
                    });
                    if buf.len() > 256 * 1024 {
                        thread::sleep(Duration::from_micros(1));
                    }
                    if idx == 0 && adv {
                        if lsn % 1000 == 0 {
                            let max_lsn = wal.get_max_lsn();
                            if max_lsn > 1000 {
                                adv_lsn = max_lsn - 1000;
                                wal.advance_lsn(adv_lsn).unwrap();
                            }
                        }
                    }

                    if lsn >= lsn_limit {
                        break;
                    }
                }
                return (logs, adv_lsn);
            }));
        }

        let mut max_wlsn = 0u64;
        let mut log_map = BTreeMap::<u64, Bytes>::new();
        let mut max_adv_lsn = 0u64;
        let mut total_wbytes = 0u64;
        for handle in handles {
            let res = handle.join().unwrap();
            if res.1 > 0 {
                max_adv_lsn = res.1;
            }
            for log in res.0 {
                if log_map.contains_key(&log.lsn) {
                    panic!("duplicated LSN found");
                }
                if max_wlsn < log.lsn {
                    max_wlsn = log.lsn;
                }
                total_wbytes += log.payload.len() as u64;
                log_map.insert(log.lsn, log.payload);
            }
        }
        let mut flashed_lsn = writer.get_max_lsn();
        let mut last_flashed_lsn = 0u64;
        let mut printed = false;
        while max_wlsn > flashed_lsn {
            if flashed_lsn != last_flashed_lsn {
                printed = false;
                last_flashed_lsn = flashed_lsn;
            } else {
                if !printed {
                    eprintln!("No change in flashed_lsn, lsn={}", flashed_lsn);
                    printed = true;
                }
            }
            thread::sleep(Duration::from_micros(1));
            flashed_lsn = writer.get_max_lsn();
        }
        println!(
            "info: max_wlsn={}, total_wbytes={}, wduration={:?}",
            max_wlsn,
            total_wbytes,
            wstart_time.elapsed()
        );

        let inst = Arc::try_unwrap(writer).unwrap().stop();
        let rstart_time = Instant::now();
        let reader = inst.open_wal_reader().unwrap();
        let min_rlsn = reader.get_min_lsn();
        assert!(min_rlsn <= max_adv_lsn + 1);
        let iter = reader.get_log_iter(min_rlsn).unwrap();
        let mut max_rlsn = 0u64;
        let mut total_rbytes = 0u64;
        for log in iter {
            let rlog = log.unwrap();
            if rlog.lsn <= max_rlsn {
                panic!(
                    "rLSN not in increase order, rlsn={}, max_rlsn={}",
                    rlog.lsn, max_rlsn
                );
            }
            max_rlsn = rlog.lsn;
            total_rbytes += rlog.payload.len() as u64;
            let wpayload = log_map
                .remove(&rlog.lsn)
                .expect(&format!("Unexpected LSN in rlog, lsn={}", rlog.lsn));
            assert_eq!(
                rlog.payload,
                wpayload,
                "lsn={}, llen={}, rlen={}",
                rlog.lsn,
                rlog.payload.len(),
                wpayload.len(),
            );
        }
        assert_eq!(max_wlsn, max_rlsn);
        println!(
            "info: max_rlsn={}, total_rbytes={}, rduration={:?}",
            max_rlsn,
            total_rbytes,
            rstart_time.elapsed()
        );
        if do_clean {
            do_test_clean(dir_arg, name);
        }
    }

    #[test]
    fn test_wal_mem_small_adv() {
        test_wal(
            "/dev/shm",
            "test_wal_mem_small_adv_sync",
            10000,
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_wal_mem_small_noadv() {
        test_wal(
            "/dev/shm",
            "test_wal_mem_small_noadv_sync",
            10000,
            false,
            true,
            None,
        );
    }

    #[test]
    fn test_wal_disk_small_adv() {
        test_wal(
            "/tmp",
            "test_wal_disk_small_adv_sync",
            10000,
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_wal_disk_small_noadv() {
        test_wal(
            "/tmp",
            "test_wal_disk_small_noadv_sync",
            10000,
            false,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_wal_mem_large_adv() {
        test_wal(
            "/dev/shm",
            "test_wal_mem_large_adv_sync",
            1000000,
            true,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_wal_mem_large_noadv() {
        test_wal(
            "/dev/shm",
            "test_wal_mem_large_noadv_sync",
            1000000,
            false,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_wal_disk_large_adv() {
        test_wal(
            "/tmp",
            "test_wal_disk_large_adv_sync",
            1000000,
            true,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_wal_disk_large_noadv() {
        test_wal(
            "/tmp",
            "test_wal_disk_large_noadv_sync",
            1000000,
            false,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_twice_wal_mem_large_adv() {
        test_wal(
            "/dev/shm",
            "test_twice_wal_mem_large_adv_sync",
            1000000,
            true,
            false,
            None,
        );
        test_wal(
            "/dev/shm",
            "test_twice_wal_mem_large_adv_sync",
            2000000,
            true,
            true,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_twice_wal_disk_large_adv() {
        test_wal(
            "/tmp",
            "test_twice_wal_disk_large_adv_sync",
            1000000,
            true,
            false,
            None,
        );
        test_wal(
            "/tmp",
            "test_twice_wal_disk_large_adv_sync",
            2000000,
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_wal_config_config() {
        let name = "test_wal_config_config_sync";
        let dir = "/dev/shm";
        let mut inst = StdFileWalInstance::new(name, dir, None).unwrap();
        let mut cfg = inst.get_config();
        assert_eq!(TunableConfig::default(), cfg);
        cfg.bulk_flush_size = 320 * 1024;
        cfg.block_size = 128 * 1024;
        inst = StdFileWalInstance::new(name, dir, Some(cfg)).unwrap();
        cfg = inst.get_config();
        assert_eq!(cfg.block_size, 64 * 1024); // expect not changed
        assert_eq!(cfg.bulk_flush_size, 512 * 1024); // expect changed and sanitized
        do_test_clean(dir, name);
        cfg = TunableConfig::default();
        cfg.block_size = 32 * 1024;
        cfg.bulk_flush_size = 128 * 1024;
        inst = StdFileWalInstance::new(name, dir, Some(cfg)).unwrap();
        cfg = inst.get_config();
        assert_eq!(cfg.block_size, 32 * 1024);
        assert_eq!(cfg.bulk_flush_size, 128 * 1024);
        do_test_clean(dir, name);
    }

    #[test]
    fn test_wal_config_mem_small_adv() {
        let mut cfg = TunableConfig::default();
        cfg.block_size = 128 * 1024;
        cfg.bulk_flush_size = 512 * 1024;
        cfg.max_data_rot_size = 128 * 1024 * 1024;
        test_wal(
            "/dev/shm",
            "test_wal_config_mem_small_adv_sync",
            10000,
            true,
            true,
            Some(cfg),
        );
    }

    #[test]
    #[ignore]
    fn test_wal_config_mem_large_adv() {
        let mut cfg = TunableConfig::default();
        cfg.block_size = 128 * 1024;
        cfg.bulk_flush_size = 512 * 1024;
        cfg.max_data_rot_size = 128 * 1024 * 1024;
        test_wal(
            "/dev/shm",
            "test_wal_config_mem_large_adv_sync",
            1000000,
            true,
            true,
            Some(cfg),
        );
    }
}
