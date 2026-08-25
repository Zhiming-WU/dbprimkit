//! A simple appender implementation with rotation support, used in non-async code.
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::appender::sync::{DefaultStdFileRotator, StdFileRotAppender};
//! fn main() {
//!     let rotator =
//!         DefaultStdFileRotator::new("/tmp", "my_data.", 2, 4194304).unwrap();
//!     let mut appender = StdFileRotAppender::new(rotator, false).unwrap();
//!     appender.append(b"my contents part1", true).unwrap();
//!     appender.append(b"my contents part2", true).unwrap();
//!     // ...
//!     appender.flush().unwrap();
//! }
//! ```
use crate::io::{IoBackend, OpenOptions, StdFileIoBackend, Syncable};
use crate::{Error, Result};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// A provider to provide the new rotation name or list the ratation names.
pub trait RotNameProvider<IO: IoBackend> {
    /// Generate a rotation name for the rotation whose number is `rot_no`.
    fn gen_rot_name(&self, rot_no: u32) -> String;

    /// Restore the information (number, name, etc.) of the latest rotation from the persistent layer
    /// for the recovery case (e.g., the program or the system restarted).  If no such rotation exists,
    /// None is returned.
    fn restore_latest_rot_info(&self) -> Result<Option<(u32, String)>>;

    /// List rotation names, in ascending order (the latest rotation is in the last).
    fn list_rot_names(&self) -> Result<Vec<String>>;
}

/// A default rotation name provider. It assumes rotations are stored in a particular directory with
/// a particular name prefix.
pub struct DefaultRotNameProvider<IO: IoBackend> {
    dir: PathBuf,
    pub(crate) prefix: String,
    _m: PhantomData<IO>,
}

impl<IO: IoBackend> DefaultRotNameProvider<IO> {
    pub fn new<P: AsRef<Path>>(dir: P, prefix: &str) -> Result<Self> {
        IO::check_path_is_dir(&dir)?;
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            prefix: prefix.to_string(),
            _m: PhantomData,
        })
    }
}

impl<IO: IoBackend> RotNameProvider<IO> for DefaultRotNameProvider<IO> {
    fn gen_rot_name(&self, rot_no: u32) -> String {
        format!("{}{:010}", self.prefix, rot_no)
    }

    fn list_rot_names(&self) -> Result<Vec<String>> {
        IO::list_files(&self.dir, Some(|name: &str| name.starts_with(&self.prefix)))
    }

    fn restore_latest_rot_info(&self) -> Result<Option<(u32, String)>> {
        let mut names = self.list_rot_names()?;
        if names.is_empty() {
            return Ok(None);
        }
        let name = names.remove(names.len() - 1);
        if let Some(sub) = name.get(self.prefix.len()..) {
            if let Ok(no) = sub.parse::<u32>() {
                return Ok(Some((no, name)));
            }
        }
        Err(Error::MiscError(format!(
            "Failed to restore rotation number from `{}`",
            name
        )))
    }
}

/// A rotator to support rotations for the appender. It's also for reading contents
/// from rotations in recovery case.
pub trait Rotator<IO: IoBackend> {
    /// Initialize and return a 'Writer' and its size for appending.
    ///
    /// Parameters:
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn init_appending(&mut self, direct_io: bool) -> Result<(IO::Writer, u64)>;

    /// Whether needs to switch to a new rotation for next appending.
    ///
    /// Parameters:
    /// - `rot_size`: The existing size of the current rotation.
    fn needs_new_rot(&self, rot_size: u64) -> bool;

    /// Switch to a new rotation and return a `Writer` for appending. The name of the new rotation
    /// is generated from [RotNameProvider::gen_rot_name].
    ///
    /// Parameters:
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn new_rot(&mut self, direct_io: bool) -> Result<IO::Writer>;

    /// Open a `Writer` for a rotation.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names].
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn open_write_rot(&self, name: &str, direct_io: bool) -> Result<IO::Writer>;

    /// List names of existing ratations, mainly for recovery case.
    fn list_rot_names(&self) -> Result<Vec<String>>;

    /// Open a `Reader` for a rotation. The size of the rotation is also returned.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names]
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn open_read_rot(&self, name: &str, direct_io: bool) -> Result<(IO::Reader, u64)>;

    /// Remove a rotation.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names].
    fn remove_rot(&self, name: &str) -> Result<()>;
}

/// A default [Rotator] implementation.
pub struct DefaultRotator<IO: IoBackend, RNP: RotNameProvider<IO> = DefaultRotNameProvider<IO>> {
    dir: PathBuf,
    latest_rot_name: Option<String>,
    cur_rot_no: u32,
    max_rot: u32,
    rot_size_max: u64,
    rnp: RNP,
    _m: PhantomData<IO>,
}

impl<IO: IoBackend> DefaultRotator<IO, DefaultRotNameProvider<IO>> {
    /// Create a [DefaultRotator] based on [DefaultRotNameProvider].
    ///
    /// Parameters:
    /// - `dir`: The directory where rotations are stored.
    /// - `prefix`: The rotation name prefix.
    /// - `max_rot`: The maximum number of rotations allowed. `0` indicates no limit.
    /// - `rot_size_max`: The rotation size threshold to decide to switch to new rotation.
    pub fn new<P: AsRef<Path>>(
        dir: P,
        prefix: &str,
        max_rot: u32,
        rot_size_max: u64,
    ) -> Result<Self> {
        IO::check_path_is_dir(&dir)?;
        let dir = dir.as_ref().to_path_buf();
        let rnp = DefaultRotNameProvider::<IO> {
            dir: dir.clone(),
            prefix: String::from(prefix),
            _m: PhantomData,
        };
        let mut out = Self {
            dir,
            latest_rot_name: None,
            cur_rot_no: 0,
            max_rot,
            rot_size_max,
            rnp,
            _m: PhantomData,
        };
        out.restore_from_storage()?;
        Ok(out)
    }
}

impl<IO: IoBackend, RNP: RotNameProvider<IO>> DefaultRotator<IO, RNP> {
    /// Create a [DefaultRotator] based on a provided [RotNameProvider].
    ///
    /// Parameters:
    /// - `dir`: The directory where rotations are stored.
    /// - `max_rot`: The maximum number of rotations allowed. `0` indicates no limit.
    /// - `rot_size_max`: The rotation size threshold to decide to switch to new rotation.
    /// - `rnp`: The provided [RotNameProvider].
    pub fn with_rot_name_resolver<P: AsRef<Path>>(
        dir: P,
        max_rot: u32,
        rot_size_max: u64,
        rnp: RNP,
    ) -> Result<Self> {
        IO::check_path_is_dir(&dir)?;
        let dir = dir.as_ref().to_path_buf();
        let mut out = Self {
            dir,
            latest_rot_name: None,
            cur_rot_no: 0,
            max_rot,
            rot_size_max,
            rnp,
            _m: PhantomData,
        };
        out.restore_from_storage()?;
        Ok(out)
    }

    fn restore_from_storage(&mut self) -> Result<()> {
        let info = self.rnp.restore_latest_rot_info()?;
        if let Some((no, rot_name)) = info {
            self.cur_rot_no = no;
            self.latest_rot_name = Some(rot_name)
        }
        Ok(())
    }

    pub(crate) fn restore_latest_rot(&self, direct_io: bool) -> Result<Option<(IO::Reader, u64)>> {
        match self.rnp.restore_latest_rot_info()? {
            None => Ok(None),
            Some((_, rot_name)) => Ok(Some(self.open_read_rot(&rot_name, direct_io)?)),
        }
    }
}

impl<IO: IoBackend, RNP: RotNameProvider<IO>> Rotator<IO> for DefaultRotator<IO, RNP> {
    fn needs_new_rot(&self, rot_size: u64) -> bool {
        rot_size >= self.rot_size_max
    }

    fn init_appending(&mut self, direct_io: bool) -> Result<(IO::Writer, u64)> {
        let mut esize = 0u64;
        let rot_name = if self.latest_rot_name.is_some() {
            let name = self.latest_rot_name.take().unwrap();
            let path = self.dir.join(&name);
            esize = IO::file_size(&path)?;
            name
        } else {
            self.rnp.gen_rot_name(self.cur_rot_no)
        };
        Ok((self.open_write_rot(&rot_name, direct_io)?, esize))
    }

    fn new_rot(&mut self, direct_io: bool) -> Result<IO::Writer> {
        let rot_names = self.list_rot_names()?;
        if self.max_rot != 0 && rot_names.len() >= self.max_rot as usize {
            println!("Removing oldest rot, name={}", &rot_names[0]);
            self.remove_rot(&rot_names[0])?;
        }
        let next_no = self.cur_rot_no + 1;
        let rot_name = self.rnp.gen_rot_name(next_no);
        let out = self.open_write_rot(&rot_name, direct_io)?;
        self.cur_rot_no = next_no;
        Ok(out)
    }

    fn open_write_rot(&self, name: &str, direct_io: bool) -> Result<IO::Writer> {
        let path = self.dir.join(name);
        let opt = OpenOptions::new()
            .with_append(true)
            .with_create(true)
            .with_direct(direct_io);
        let out = IO::open_writer(path, &opt)?;
        Ok(out)
    }

    fn list_rot_names(&self) -> Result<Vec<String>> {
        self.rnp.list_rot_names()
    }

    fn open_read_rot(&self, name: &str, direct_io: bool) -> Result<(IO::Reader, u64)> {
        let path = self.dir.join(name);
        let size = IO::file_size(&path)?;
        let opt = OpenOptions::new()
            .with_append(true)
            .with_create(true)
            .with_direct(direct_io);
        Ok((IO::open_reader(path, &opt)?, size))
    }

    fn remove_rot(&self, name: &str) -> Result<()> {
        let path = self.dir.join(name);
        IO::remove_file(path)
    }
}

/// DefaultRotator based on [StdFileIoBackend] and [DefaultRotNameProvider].
pub type DefaultStdFileRotator =
    DefaultRotator<StdFileIoBackend, DefaultRotNameProvider<StdFileIoBackend>>;

/// An appender with ratation support.
pub struct RotAppender<IO: IoBackend, R: Rotator<IO>> {
    direct_io: bool,
    esize: u64,
    rot: R,
    writer: IO::Writer,
    _m: PhantomData<IO>,
}

impl<IO: IoBackend, R: Rotator<IO>> RotAppender<IO, R> {
    /// Create a new [RotAppender]. [Rotator::init_appending] is called in this function.
    ///
    /// Parameters:
    /// - `rotator`: The [Rotator] which controls the rotations.
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    pub fn new(mut rot: R, direct_io: bool) -> Result<Self> {
        let writer = rot.init_appending(direct_io)?;
        Ok(Self {
            direct_io,
            esize: writer.1,
            rot,
            writer: writer.0,
            _m: PhantomData,
        })
    }

    /// Append the contents in the parameter `payload`. If the parameter `rotatable` is true and the current
    /// rotation reaches the condition (determined by [Rotator::needs_new_rot]) to switch to a new one, the contents
    /// in the appender is flushed and synchronized to the persistent storage and the appender switches to
    /// a new rotation.
    ///
    /// Parameters:
    /// - `payload`: The payload to be appneded.
    /// - `rotatable`: Whether rotation switching is allowed after this payload. Usually it's `true` but sometimes
    /// it's `false` for some reason (i.g. data integrity).
    pub fn append(&mut self, payload: &[u8], rotatable: bool) -> Result<bool> {
        let mut new_rot = false;
        let asize = payload.len() as u64;
        self.writer.write_all(payload)?;
        self.esize += asize;
        if rotatable && self.rot.needs_new_rot(self.esize) {
            self.writer.flush()?;
            self.writer.sync_all()?;
            self.writer = self.rot.new_rot(self.direct_io)?;
            self.esize = 0;
            new_rot = true;
        }
        Ok(new_rot)
    }

    /// Flush and synchronize the contents in the [RotAppender] to the persistent storage.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.sync_all()?;
        Ok(())
    }

    /// Whether needs to switch to a new rotation for next appending.
    pub fn needs_new_rot(&self) -> bool {
        self.rot.needs_new_rot(self.esize)
    }

    /// Get a reference to the inner [Rotator] (for listing ratations, reading from a rotation, etc.).
    pub fn get_rotator(&self) -> &R {
        &self.rot
    }
}

/// RotAppender based on [StdFileIoBackend] and [DefaultStdFileRotator].
pub type StdFileRotAppender = RotAppender<StdFileIoBackend, DefaultStdFileRotator>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::*;
    use bytes::BytesMut;
    use rand::RngExt;
    use std::io::Read;

    #[test]
    fn test_default_rot_name_provider() {
        let test_dir = "/tmp";
        let rnp =
            DefaultRotNameProvider::<StdFileIoBackend>::new(test_dir, "drnp_test_sync.").unwrap();
        let name1 = rnp.gen_rot_name(3);
        let name2 = rnp.gen_rot_name(5);
        assert_eq!(name1, String::from("drnp_test_sync.0000000003"));
        assert_eq!(name2, String::from("drnp_test_sync.0000000005"));
        let res = rnp.restore_latest_rot_info().unwrap();
        assert!(res.is_none());
        let opts = OpenOptions::new().with_create(true);
        let paths = vec![format!("/tmp/{}", name1), format!("/tmp/{}", name2)];
        let exp_names = vec![name1, name2];
        StdFileIoBackend::open_writer(&paths[0], &opts).unwrap();
        StdFileIoBackend::open_writer(&paths[1], &opts).unwrap();
        let names = rnp.list_rot_names().unwrap();
        assert_eq!(names, exp_names);
        let res = rnp.restore_latest_rot_info().unwrap().unwrap();
        assert_eq!(res.0, 5);
        assert_eq!(res.1, exp_names[1]);
        StdFileIoBackend::remove_file(&paths[0]).unwrap();
        StdFileIoBackend::remove_file(&paths[1]).unwrap();
        println!("{}, {}", &paths[0], &paths[1]);
    }

    #[test]
    fn test_default_rotator() {
        let capa = 2560usize;
        let mut dr = DefaultStdFileRotator::new("/tmp", "dr_test_sync.", 3, 4096).unwrap();
        assert!(!dr.needs_new_rot(1095));
        assert!(dr.needs_new_rot(4096));
        assert!(dr.needs_new_rot(4097));
        let mut buf_mut = BytesMut::with_capacity(capa);
        unsafe { buf_mut.set_len(capa) };
        let mut rng = rand::rng();
        rng.fill(&mut buf_mut);
        let buf = buf_mut.freeze();
        let mut wr = dr.init_appending(false).unwrap().0;
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        let mut wr = dr.new_rot(false).unwrap();
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        assert_eq!(dr.list_rot_names().unwrap().len(), 2);
        let mut wr = dr.new_rot(false).unwrap();
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        assert_eq!(dr.list_rot_names().unwrap().len(), 3);
        let mut wr = dr.new_rot(false).unwrap();
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        assert_eq!(dr.list_rot_names().unwrap().len(), 3);
        let mut wr = dr.new_rot(false).unwrap();
        wr.write_all(&buf.clone()).unwrap();
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        let exp_names = vec![
            String::from("dr_test_sync.0000000002"),
            String::from("dr_test_sync.0000000003"),
            String::from("dr_test_sync.0000000004"),
        ];
        assert_eq!(dr.list_rot_names().unwrap(), exp_names);
        let mut rd = dr.open_read_rot("dr_test_sync.0000000004", false).unwrap();
        assert_eq!(rd.1, (capa * 2) as u64);
        let mut rd_buf_mut = BytesMut::with_capacity(capa);
        unsafe { rd_buf_mut.set_len(capa) };
        rd.0.read_exact(&mut rd_buf_mut).unwrap();
        assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
        let mut dr = DefaultStdFileRotator::new("/tmp", "dr_test_sync.", 3, 4096).unwrap();
        let mut wr = dr.init_appending(false).unwrap().0;
        wr.write_all(&buf.clone()).unwrap();
        wr.flush().unwrap();
        let rd = dr.open_read_rot("dr_test_sync.0000000004", false).unwrap();
        assert_eq!(rd.1, (capa * 3) as u64);
        dr.remove_rot(&exp_names[0]).unwrap();
        dr.remove_rot(&exp_names[1]).unwrap();
        dr.remove_rot(&exp_names[2]).unwrap();
        let mut wr = dr.new_rot(true).unwrap();
        wr.write_all(b"dummy").unwrap();
        wr.write_all(b"dummy").unwrap();
        wr.write_all(&buf.clone()).unwrap();
        let rd = dr.open_read_rot("dr_test_sync.0000000005", true).unwrap();
        assert_eq!(rd.1, (capa + 10) as u64);
        dr.remove_rot("dr_test_sync.0000000005").unwrap();
    }

    #[test]
    fn test_rot_appender() {
        let capa = 3072usize;
        let rotator = DefaultStdFileRotator::new("/tmp", "ra_test_sync.", 3, 8096).unwrap();
        let mut appender = StdFileRotAppender::new(rotator, false).unwrap();
        let mut buf_mut = BytesMut::with_capacity(capa);
        unsafe { buf_mut.set_len(capa) };
        let mut rng = rand::rng();
        rng.fill(&mut buf_mut);
        let buf = buf_mut.freeze();
        let mut idx = 0usize;
        while idx < 16 {
            appender.append(&buf.clone(), true).unwrap();
            idx += 1;
            let rots = appender.get_rotator().list_rot_names().unwrap();
            let rot_cnt = (((idx + 3) / 3) as usize).min(3);
            let buf_cnt = idx % 3;
            assert_eq!(appender.esize, (buf_cnt * capa) as u64);
            assert_eq!(rots.len(), rot_cnt, "idx={}", idx);
        }
        appender.flush().unwrap();
        let exp_rots = vec![
            String::from("ra_test_sync.0000000003"),
            String::from("ra_test_sync.0000000004"),
            String::from("ra_test_sync.0000000005"),
        ];
        let rots = appender.get_rotator().list_rot_names().unwrap();
        assert_eq!(rots, exp_rots);
        let exp_sizes = [capa * 3, capa * 3, capa];
        for (idx, name) in rots.iter().enumerate() {
            let mut rd = appender.get_rotator().open_read_rot(name, false).unwrap();
            assert_eq!(rd.1, exp_sizes[idx] as u64);
            let mut rd_buf_mut = BytesMut::with_capacity(capa);
            unsafe { rd_buf_mut.set_len(capa) };
            rd.0.read_exact(&mut rd_buf_mut).unwrap();
            assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
        }
        let rotator = DefaultStdFileRotator::new("/tmp", "ra_test_sync.", 3, 8096).unwrap();
        let mut appender = StdFileRotAppender::new(rotator, false).unwrap();
        while idx < 32 {
            appender.append(&buf.clone(), true).unwrap();
            idx += 1;
            let rots = appender.get_rotator().list_rot_names().unwrap();
            let buf_cnt = idx % 3;
            assert_eq!(appender.esize, (buf_cnt * capa) as u64);
            assert_eq!(rots.len(), 3);
        }
        let exp_rots = vec![
            String::from("ra_test_sync.0000000008"),
            String::from("ra_test_sync.0000000009"),
            String::from("ra_test_sync.0000000010"),
        ];
        let rots = appender.get_rotator().list_rot_names().unwrap();
        assert_eq!(rots, exp_rots);
        let exp_sizes = [capa * 3, capa * 3, capa * 2];
        for (idx, name) in rots.iter().enumerate() {
            let mut rd = appender.get_rotator().open_read_rot(name, false).unwrap();
            assert_eq!(rd.1, exp_sizes[idx] as u64);
            let mut rd_buf_mut = BytesMut::with_capacity(capa);
            unsafe { rd_buf_mut.set_len(capa) };
            rd.0.read_exact(&mut rd_buf_mut).unwrap();
            assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
            appender.get_rotator().remove_rot(name).unwrap();
        }
    }
}
