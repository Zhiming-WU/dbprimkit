//! A simple WAL (Write Ahead Log) implementation used in non-async code.
//!
//! Here is an example for using it for appending log entries;
//! ```rust
//! use bytes::Bytes;
//! use dbprimkit::wal::sync::StdFileWalInstance;
//!
//! fn main() {
//!     let inst = StdFileWalInstance::new("my_wal", "/tmp");
//!     let writer = inst.open_wal_writer().unwrap();
//!     // Append log entries
//!     let lsn1 = writer.append(bytes::Bytes::from("my first log")).unwrap();
//!     let lsn2 = writer.append(bytes::Bytes::from("my first log")).unwrap();
//!     // Check the maxinum LSN of log entries which have been persisted
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
//!     let inst = StdFileWalInstance::new("my_wal", "/tmp");
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
use crate::Error::{CorruptFile, InvalidArgument, MiscError};
use crate::appender::sync::*;
use crate::io::{IoBackend, StdFileIoBackend};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

fn restore_max_lsn<IO: IoBackend>(reader: &mut IO::Reader, file_size: u64) -> Result<u64> {
    let block_size = BLOCK_SIZE as u64;
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
    Ok(header.max_lsn)
}

fn restore_lsn<IO: IoBackend>(
    mani_rotator: &DefaultRotator<IO>,
    data_rotator: &DefaultRotator<IO, WalDataRotNameProvider<IO>>,
) -> Result<(u64, u64)> {
    match mani_rotator.restore_latest_rot(false)? {
        None => Ok((0, 0)),
        Some((mut reader, file_size)) => {
            let mani_entry = read_latest_manifest_entry::<IO>(&mut reader, file_size)?;
            let max_lsn = match data_rotator.restore_latest_rot(false)? {
                None => return Err(MiscError("Missing data rotations".into())),
                Some((mut data_reader, data_file_size)) => {
                    restore_max_lsn::<IO>(&mut data_reader, data_file_size)?
                }
            };
            let mut min_lsn = mani_entry.min_lsn;
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
) -> Result<(IO::Reader, u64)> {
    let (reader, fsize) = rotator.open_read_rot(&rot_name, false)?;
    if fsize % BLOCK_SIZE as u64 != 0 {
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
        super::MAX_DATA_ROT_SIZE,
        WalDataRotNameProvider::<IO> {
            inner: data_rnp.clone(),
        },
    )?;

    let (min_lsn, max_lsn) = restore_lsn(&mani_rotator, &data_rotator)?;
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
    mani_flush_sleep_cnt: u32,
    last_min_lsn: u64,
    next_no_use_lsn: u64,
    data_flush_sleep_cnt: u32,
    data_flush_cnt: u32,
    data_flush_disturbed: bool,
    data_buf: BytesMut,
    max_flushed_lsn: u64,
    last_filled_lsn: u64,
}

impl<IO: IoBackend> FlushingControlBlock<IO> {
    fn flush_mani_entry(&mut self, min_lsn: u64) -> Result<()> {
        DiskManiEntry::encode_by_params(&mut self.mani_buf, min_lsn);
        self.mani_app
            .append(&self.mani_buf.split().freeze(), true)?;
        self.mani_app.flush()?;
        // reuse the memory
        self.mani_buf.reserve(MANI_ENTRY_SIZE);
        self.last_min_lsn = min_lsn;
        Ok(())
    }

    fn fix_block_headers(&mut self) {
        let len = self.data_buf.len();
        if len > 0 {
            let offset = (len - 1) & (!BLOCK_MASK);
            self.data_buf[(offset + 16)..(offset + 24)]
                .copy_from_slice(&self.last_filled_lsn.to_ne_bytes());
            let crc = crc32c::crc32c(&self.data_buf[(offset + 4)..(offset + 24)]);
            self.data_buf[offset..(offset + 4)].copy_from_slice(&crc.to_ne_bytes());
        }
    }

    fn flush_data_blocks(&mut self, rotatable: bool) -> Result<()> {
        self.cb
            .flush_lsn
            .store(self.last_filled_lsn, Ordering::Release);
        let new_rot = self
            .data_app
            .append(&self.data_buf.split().freeze(), rotatable)?;
        if new_rot {
            let min_lsn = self.cb.min_lsn.load(Ordering::Acquire);
            Self::flush_mani_entry(self, min_lsn)?;
            self.next_no_use_lsn =
                remove_no_use_rots(self.data_app.get_rotator(), &self.data_rnp, min_lsn)?;
        } else {
            self.data_app.flush()?;
        }
        // reuse memory
        self.data_buf.reserve(BUF_SIZE);
        if rotatable {
            self.cb
                .max_lsn
                .store(self.max_flushed_lsn, Ordering::Release);
        }
        self.data_flush_sleep_cnt = 0;
        Ok(())
    }
}

const BUF_SIZE: usize = BLOCK_SIZE * 4;

#[derive(Debug)]
pub struct WalWriter<IO: IoBackend + Send> {
    inst: WalInstance<IO>,
    cb: Arc<ControlBlock>,
}

impl<IO: IoBackend + Send> WalWriter<IO> {
    fn new(inst: WalInstance<IO>) -> Result<Self> {
        let (mani_rotator, data_rotator, data_rnp, flush_lsn, min_lsn, max_lsn) =
            new_wal_fields(&inst)?;
        let cb = Arc::new(ControlBlock::new(min_lsn, max_lsn, flush_lsn));
        let mani_app = RotAppender::new(mani_rotator, true)?;
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
        const MANI_FLUSH_LSN_GAP_THRES: u64 = 100000;
        const MANI_FLUSH_SLEEP_THRES: u32 = 3600000;
        const DATA_FLUSH_SLEEP_THRES: u32 = 5;
        const DATA_FLUSH_DISTURB_THRES: u32 = 100000;

        let mut fcb = {
            let min_lsn = cb.min_lsn.load(Ordering::Acquire);
            let next_no_use_lsn = remove_no_use_rots(data_app.get_rotator(), &data_rnp, min_lsn)?;
            let mut fcb = FlushingControlBlock::<IO> {
                mani_app,
                data_app,
                data_rnp,
                mani_buf: BytesMut::with_capacity(MANI_ENTRY_SIZE),
                mani_flush_sleep_cnt: 0,
                last_min_lsn: min_lsn,
                next_no_use_lsn,
                data_flush_sleep_cnt: 0,
                data_flush_cnt: 0,
                data_flush_disturbed: false,
                data_buf: BytesMut::with_capacity(BUF_SIZE),
                max_flushed_lsn: cb.max_lsn.load(Ordering::Acquire),
                last_filled_lsn: 0,
                cb,
            };
            if min_lsn == 0 {
                fcb.flush_mani_entry(min_lsn)?;
            }
            fcb
        };

        while fcb.cb.running.load(Ordering::Acquire) {
            let min_lsn = fcb.cb.min_lsn.load(Ordering::Acquire);
            if min_lsn > fcb.last_min_lsn {
                if min_lsn >= fcb.next_no_use_lsn {
                    fcb.next_no_use_lsn =
                        remove_no_use_rots(fcb.data_app.get_rotator(), &fcb.data_rnp, min_lsn)?;
                }
                if min_lsn - fcb.last_min_lsn >= MANI_FLUSH_LSN_GAP_THRES
                    || fcb.mani_flush_sleep_cnt >= MANI_FLUSH_SLEEP_THRES
                {
                    fcb.flush_mani_entry(min_lsn)?;
                    fcb.last_min_lsn = min_lsn;
                }
            }

            while let Some(LogEntry {
                lsn,
                payload: mut log,
            }) = fcb.cb.rbuf.pop_for_sc()
            {
                let mut split = false;
                let mut handled = false;
                while !handled {
                    if (fcb.data_buf.len() & BLOCK_MASK) == 0 {
                        fcb.fix_block_headers();
                        DiskBlockHeader::encode_by_params(&mut fcb.data_buf, lsn);
                    }
                    let left = log.len();
                    let bk_left = BLOCK_SIZE - (fcb.data_buf.len() & BLOCK_MASK);
                    if LOG_HEAD_SIZE + left <= bk_left {
                        // enough buffer to put log
                        let fullness = if split {
                            split = false;
                            Fullness::Last
                        } else {
                            Fullness::Full
                        };
                        DiskLogEntry::encode_by_params(
                            &mut fcb.data_buf,
                            lsn,
                            log.split_to(left),
                            fullness,
                        );
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
                        DiskLogEntry::encode_by_params(&mut fcb.data_buf, lsn, part_log, fullness);
                        fcb.last_filled_lsn = lsn;
                    } else if bk_left > 0 {
                        // fill zero
                        fcb.data_buf.put_bytes(0, bk_left);
                    }
                    if fcb.data_buf.len() == BUF_SIZE
                        || (fcb.data_app.needs_new_rot() && fcb.data_buf.len() & BLOCK_MASK == 0)
                    {
                        fcb.fix_block_headers();
                        fcb.flush_data_blocks(!split)?;
                    }
                }

                fcb.data_flush_cnt += 1;
                if fcb.data_flush_cnt > DATA_FLUSH_DISTURB_THRES {
                    fcb.data_flush_disturbed = true;
                    fcb.data_flush_cnt = 0;
                    break;
                }
            }

            // long time no flush. flush it now.
            if fcb.data_flush_sleep_cnt >= DATA_FLUSH_SLEEP_THRES {
                if fcb.data_buf.len() > 0 {
                    // fill up to block boundary
                    let bk_len = fcb.data_buf.len() & BLOCK_MASK;
                    if bk_len != 0 {
                        fcb.data_buf.put_bytes(0, BLOCK_SIZE - bk_len);
                    }
                    fcb.fix_block_headers();
                    fcb.flush_data_blocks(true)?;
                }
                fcb.data_flush_sleep_cnt = 0;
            }

            // no sleep if disturbed
            if fcb.data_flush_disturbed {
                fcb.data_flush_disturbed = false;
                continue;
            }

            std::thread::sleep(interval);
            fcb.mani_flush_sleep_cnt += 1;
            fcb.data_flush_sleep_cnt += 1;
        }
        Ok(())
    }

    fn flushing_task(
        cb: Arc<ControlBlock>,
        mani_app: RotAppender<IO, DefaultRotator<IO>>,
        data_app: RotAppender<IO, DefaultRotator<IO, WalDataRotNameProvider<IO>>>,
        data_rnp: Arc<WalDataRotNameProviderImpl<IO>>,
    ) {
        if let Err(_err) = Self::flushing_task_inner(cb.clone(), mani_app, data_app, data_rnp) {
            cb.running.store(false, Ordering::Release);
        }
    }

    fn check_running(&self) -> Result<()> {
        if !self.cb.running.load(Ordering::Acquire) {
            return Err(Error::FunctionNotRunning("WAL flushing".into()));
        }
        Ok(())
    }

    /// Append a log entry. If it works OK, an LSN (Log Sequence Number) of `u64` is returned.
    /// Call [Self::get_max_lsn] to check whether this log entry has been persisted. (If the LSN returned
    /// by [Self::get_max_lsn] is larger than or equal to the LSN returned by this method, this log
    /// entry has been persisted.)
    pub fn append(&self, log: Bytes) -> Result<u64> {
        self.check_running()?;
        Ok(self.cb.rbuf.push(log)?)
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
}

impl<'a, IO: IoBackend> LogIterator<'a, IO> {
    // Read next block and parse it into the cache, until at least one log entry cached.
    // Return `false' if no log entry was read or `true` if any entry was read.
    fn read_next(&mut self) -> Result<bool> {
        //let debug = self.next_lsn > 300;
        let ocnt = self.cached.len();
        while let Some(reader) = &mut self.reader {
            let mut bbuf = BytesMut::with_capacity(BLOCK_SIZE);
            unsafe { bbuf.set_len(BLOCK_SIZE) };
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
                    let (rd, _) = open_reader(&self.wal.data_rotator, &rot_name)?;
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
                            fullness @ _ => {
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

        let mut cached = VecDeque::new();
        let (mut reader, fsize) = open_reader(&self.data_rotator, &rot_name)?;
        let mut offset = 0u64;
        let mut bbuf = BytesMut::with_capacity(BLOCK_SIZE);
        let mut hbuf = BytesMut::with_capacity(BLOCK_HEAD_SIZE);
        let mut middle_log_lsn = 0u64;
        let mut middle_log_bufs = Vec::new();
        while offset < fsize && fsize - offset >= BLOCK_SIZE as u64 {
            reader.seek(SeekFrom::Start(offset))?;
            unsafe { hbuf.set_len(BLOCK_HEAD_SIZE) };
            reader.read_exact(&mut hbuf)?;
            let header = DiskBlockHeader::decode(&mut hbuf.split().freeze())?;
            if header.min_lsn <= start_lsn && start_lsn <= header.max_lsn {
                reader.seek(SeekFrom::Start(offset))?;
                unsafe { bbuf.set_len(BLOCK_SIZE) };
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
                };
                if log_iter.cached.is_empty() {
                    if !log_iter.read_next()? {
                        break;
                    }
                }
                return Ok(log_iter);
            }
            hbuf.reserve(BLOCK_HEAD_SIZE);
            unsafe { hbuf.set_len(BLOCK_HEAD_SIZE) };
            offset += BLOCK_SIZE as u64;
        }

        return Err(MiscError(
            "Unable to find log entry matching `start_lsn`".into(),
        ));
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
    _m: PhantomData<IO>,
}

impl<IO: IoBackend> WalInstance<IO> {
    /// Create a [WalInstance].
    ///
    /// Parameters:
    /// - `name`: The name of the [WalInstance]. Data files are prefixed by this name.
    /// - `dir`: The directory where the data files are stored.
    pub fn new<P: AsRef<Path>>(name: &str, dir: P) -> Self {
        Self {
            name: name.into(),
            dir: dir.as_ref().to_path_buf(),
            _m: PhantomData,
        }
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

    fn test_wal(dir: &str, name: &str, lsn_limit: u64, adv: bool) {
        let dir = AsRef::<Path>::as_ref(dir).to_path_buf();
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

        let inst = StdFileWalInstance::new(name, dir.as_path());
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
        println!("wduration: {:?}", wstart_time.elapsed());
        let inst = Arc::try_unwrap(writer).unwrap().stop();
        let rstart_time = Instant::now();
        let reader = inst.open_wal_reader().unwrap();
        let min_rlsn = reader.get_min_lsn();
        println!("debug: min_rlsn={}", min_rlsn);
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
        let names =
            StdFileIoBackend::list_files(&dir, Some(|n: &str| n.starts_with(name))).unwrap();
        for n in names {
            let p = &dir.join(&n);
            StdFileIoBackend::remove_file(p).unwrap();
        }
    }

    #[test]
    fn test_wal_mem_small_adv() {
        test_wal("/dev/shm", "test_wal_mem_small_adv_sync", 10000, true);
    }

    #[test]
    fn test_wal_mem_small_noadv() {
        test_wal("/dev/shm", "test_wal_mem_small_noadv_sync", 10000, false);
    }

    #[test]
    fn test_wal_disk_small_adv() {
        test_wal("/tmp", "test_wal_disk_small_adv_sync", 10000, true);
    }

    #[test]
    fn test_wal_disk_small_noadv() {
        test_wal("/tmp", "test_wal_disk_small_noadv_sync", 10000, false);
    }

    #[test]
    #[ignore]
    fn test_wal_mem_large_adv() {
        test_wal("/dev/shm", "test_wal_mem_large_adv_sync", 1000000, true);
    }

    #[test]
    #[ignore]
    fn test_wal_mem_large_noadv() {
        test_wal("/dev/shm", "test_wal_mem_large_noadv_sync", 1000000, false);
    }

    #[test]
    #[ignore]
    fn test_wal_disk_large_adv() {
        test_wal("/tmp", "test_wal_disk_large_adv_sync", 1000000, true);
    }

    #[test]
    #[ignore]
    fn test_wal_disk_large_noadv() {
        test_wal("/tmp", "test_wal_disk_large_noadv_sync", 1000000, false);
    }
}
