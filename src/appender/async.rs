//! A simple appender implementation with rotation support, used in async code.
//!
//! Below is an example.
//! ```rust
//! use dbprimkit::appender::r#async::{DefaultTokioFileRotator, TokioFileRotAppender};
//!
//! #[tokio::main]
//! async fn main() {
//!     let rotator =
//!         DefaultTokioFileRotator::new("/tmp", "my_data.", 2, 4194304).await.unwrap();
//!     let mut appender = TokioFileRotAppender::new(rotator, false).await.unwrap();
//!     appender.append(b"my contents part1", true).await.unwrap();
//!     appender.append(b"my contents part2", true).await.unwrap();
//!     // ...
//!     appender.flush().await.unwrap();
//! }
//! ```
use crate::io::{AsyncIoBackend, AsyncSyncable, OpenOptions, TokioFileIoBackend};
use crate::{Error, Result};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

/// A provider to provide the new rotation name or list the ratation names.
pub trait RotNameProvider<IO: AsyncIoBackend + Sync> {
    /// Generate a rotation name for the rotation whose number is `rot_no`.
    fn gen_rot_name(&self, rot_no: u32) -> impl Future<Output = String> + Send;

    /// Restore the information (number, name, etc.) of the latest rotation from the persistent layer
    /// for the recovery case (e.g., the program or the system restarted).  If no such rotation exists,
    /// None is returned.
    fn restore_latest_rot_info(&self)
    -> impl Future<Output = Result<Option<(u32, String)>>> + Send;

    /// List rotation names, in ascending order (the latest rotation is in the last).
    fn list_rot_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;
}

/// A default rotation name provider. It assumes rotations are stored in a particular directory with
/// a particular name prefix.
pub struct DefaultRotNameProvider<IO: AsyncIoBackend> {
    dir: PathBuf,
    pub(crate) prefix: String,
    _m: PhantomData<IO>,
}

impl<IO: AsyncIoBackend> DefaultRotNameProvider<IO> {
    pub async fn new<P: AsRef<Path>>(dir: P, prefix: &str) -> Result<Self> {
        IO::check_path_is_dir(dir.as_ref()).await?;
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            prefix: prefix.to_string(),
            _m: PhantomData,
        })
    }
}

impl<IO: AsyncIoBackend + Sync> RotNameProvider<IO> for DefaultRotNameProvider<IO> {
    async fn gen_rot_name(&self, rot_no: u32) -> String {
        format!("{}{:010}", self.prefix, rot_no)
    }

    async fn list_rot_names(&self) -> Result<Vec<String>> {
        IO::list_files(
            AsRef::<Path>::as_ref(&self.dir),
            Some(|name: &str| name.starts_with(&self.prefix)),
        )
        .await
    }

    async fn restore_latest_rot_info(&self) -> Result<Option<(u32, String)>> {
        let mut names = self.list_rot_names().await?;
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
pub trait Rotator<IO: AsyncIoBackend> {
    /// Initialize and return a 'Writer' and its size for appending.
    ///
    /// Parameters:
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn init_appending(
        &mut self,
        direct_io: bool,
    ) -> impl Future<Output = Result<(IO::Writer, u64)>> + Send;

    /// Whether needs to switch to a new rotation for next appending.
    ///
    /// Parameters:
    /// - `rot_size`: The existing size of the current rotation.
    fn needs_new_rot(&self, rot_size: u64) -> impl Future<Output = bool> + Send;

    /// Switch to a new rotation and return a `Writer` for appending. The name of the new rotation
    /// is generated from [RotNameProvider::gen_rot_name].
    ///
    /// Parameters:
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn new_rot(&mut self, direct_io: bool) -> impl Future<Output = Result<IO::Writer>> + Send;

    /// Open a `Writer` for a rotation.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names].
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn open_write_rot(
        &self,
        name: &str,
        direct_io: bool,
    ) -> impl Future<Output = Result<IO::Writer>> + Send;

    /// List names of existing ratations, mainly for recovery case.
    fn list_rot_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Open a `Reader` for a rotation. The size of the rotation is also returned.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names]
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    fn open_read_rot(
        &self,
        name: &str,
        direct_io: bool,
    ) -> impl Future<Output = Result<(IO::Reader, u64)>> + Send;

    /// Remove a rotation.
    ///
    /// Parameters:
    /// - `name`: The name of the rotation to open, usually selected from the list returned by [Self::list_rot_names].
    fn remove_rot(&self, name: &str) -> impl Future<Output = Result<()>> + Send;
}

/// A default [Rotator] implementation.
pub struct DefaultRotator<
    IO: AsyncIoBackend + Sync,
    RNP: RotNameProvider<IO> = DefaultRotNameProvider<IO>,
> {
    dir: PathBuf,
    latest_rot_name: Option<String>,
    cur_rot_no: u32,
    max_rot: u32,
    rot_size_max: u64,
    rnp: RNP,
    _m: PhantomData<IO>,
}

impl<IO: AsyncIoBackend + Sync + Send> DefaultRotator<IO, DefaultRotNameProvider<IO>> {
    /// Create a [DefaultRotator] based on [DefaultRotNameProvider].
    ///
    /// Parameters:
    /// - `dir`: The directory where rotations are stored.
    /// - `prefix`: The rotation name prefix.
    /// - `max_rot`: The maximum number of rotations allowed. `0` indicates no limit.
    /// - `rot_size_max`: The rotation size threshold to decide to switch to new rotation.
    pub async fn new<P: AsRef<Path>>(
        dir: P,
        prefix: &str,
        max_rot: u32,
        rot_size_max: u64,
    ) -> Result<Self> {
        IO::check_path_is_dir(dir.as_ref()).await?;
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
        out.restore_from_storage().await?;
        Ok(out)
    }
}

impl<IO: AsyncIoBackend + Sync + Send, RNP: RotNameProvider<IO> + Sync + Send>
    DefaultRotator<IO, RNP>
{
    /// Create a [DefaultRotator] based on a provided [RotNameProvider].
    ///
    /// Parameters:
    /// - `dir`: The directory where rotations are stored.
    /// - `max_rot`: The maximum number of rotations allowed. `0` indicates no limit.
    /// - `rot_size_max`: The rotation size threshold to decide to switch to new rotation.
    /// - `rnp`: The provided [RotNameProvider].
    pub async fn with_rot_name_resolver<P: AsRef<Path>>(
        dir: P,
        max_rot: u32,
        rot_size_max: u64,
        rnp: RNP,
    ) -> Result<Self> {
        IO::check_path_is_dir(dir.as_ref()).await?;
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
        out.restore_from_storage().await?;
        Ok(out)
    }

    async fn restore_from_storage(&mut self) -> Result<()> {
        let info = self.rnp.restore_latest_rot_info().await?;
        if let Some((no, rot_name)) = info {
            self.cur_rot_no = no;
            self.latest_rot_name = Some(rot_name)
        }
        Ok(())
    }

    pub(crate) async fn restore_latest_rot(
        &self,
        direct_io: bool,
    ) -> Result<Option<(IO::Reader, u64)>> {
        match self.rnp.restore_latest_rot_info().await? {
            None => Ok(None),
            Some((_, rot_name)) => Ok(Some(self.open_read_rot(&rot_name, direct_io).await?)),
        }
    }
}

impl<IO: AsyncIoBackend + Sync + Send, RNP: RotNameProvider<IO> + Sync + Send> Rotator<IO>
    for DefaultRotator<IO, RNP>
{
    async fn needs_new_rot(&self, rot_size: u64) -> bool {
        rot_size >= self.rot_size_max
    }

    async fn init_appending(&mut self, direct_io: bool) -> Result<(IO::Writer, u64)> {
        let mut esize = 0u64;
        let rot_name = if self.latest_rot_name.is_some() {
            let name = self.latest_rot_name.take().unwrap();
            let path = self.dir.join(&name);
            esize = IO::file_size(&path).await?;
            name
        } else {
            self.rnp.gen_rot_name(self.cur_rot_no).await
        };
        Ok((self.open_write_rot(&rot_name, direct_io).await?, esize))
    }

    async fn new_rot(&mut self, direct_io: bool) -> Result<IO::Writer> {
        let rot_names = self.list_rot_names().await?;
        if self.max_rot != 0 && rot_names.len() >= self.max_rot as usize {
            println!("Removing oldest rot, name={}", &rot_names[0]);
            self.remove_rot(&rot_names[0]).await?;
        }
        let next_no = self.cur_rot_no + 1;
        let rot_name = self.rnp.gen_rot_name(next_no).await;
        let out = self.open_write_rot(&rot_name, direct_io).await?;
        self.cur_rot_no = next_no;
        Ok(out)
    }

    async fn open_write_rot(&self, name: &str, direct_io: bool) -> Result<IO::Writer> {
        let path = self.dir.join(name);
        let opt = OpenOptions::new()
            .with_append(true)
            .with_create(true)
            .with_direct(direct_io);
        let out = IO::open_writer(path, &opt).await?;
        Ok(out)
    }

    async fn list_rot_names(&self) -> Result<Vec<String>> {
        self.rnp.list_rot_names().await
    }

    async fn open_read_rot(&self, name: &str, direct_io: bool) -> Result<(IO::Reader, u64)> {
        let path = self.dir.join(name);
        let size = IO::file_size(&path).await?;
        let opt = OpenOptions::new()
            .with_append(true)
            .with_create(true)
            .with_direct(direct_io);
        Ok((IO::open_reader(path, &opt).await?, size))
    }

    async fn remove_rot(&self, name: &str) -> Result<()> {
        let path = self.dir.join(name);
        IO::remove_file(path).await
    }
}

/// DefaultRotator based on [TokioFileIoBackend] and [DefaultRotNameProvider].
pub type DefaultTokioFileRotator =
    DefaultRotator<TokioFileIoBackend, DefaultRotNameProvider<TokioFileIoBackend>>;

/// An appender with ratation support.
pub struct RotAppender<IO: AsyncIoBackend, R: Rotator<IO>> {
    direct_io: bool,
    esize: u64,
    rot: R,
    writer: IO::Writer,
    _m: PhantomData<IO>,
}

impl<IO: AsyncIoBackend, R: Rotator<IO>> RotAppender<IO, R> {
    /// Create a new [RotAppender]. [Rotator::init_appending] is called in this function.
    ///
    /// Parameters:
    /// - `rotator`: The [Rotator] which controls the rotations.
    /// - `direct_io`: Whether direct IO is applied. It relies on the IO backend support.
    pub async fn new(mut rot: R, direct_io: bool) -> Result<Self> {
        let writer = rot.init_appending(direct_io).await?;
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
    pub async fn append(&mut self, payload: &[u8], rotatable: bool) -> Result<bool> {
        let mut new_rot = false;
        let asize = payload.len() as u64;
        self.writer.write_all(payload).await?;
        self.esize += asize;
        if rotatable && self.rot.needs_new_rot(self.esize).await {
            self.writer.flush().await?;
            self.writer.sync_all().await?;
            self.writer = self.rot.new_rot(self.direct_io).await?;
            self.esize = 0;
            new_rot = true;
        }
        Ok(new_rot)
    }

    /// Flush and synchronize the contents in the [RotAppender] to the persistent storage.
    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        self.writer.sync_all().await?;
        Ok(())
    }

    /// Whether needs to switch to a new rotation for next appending.
    pub async fn needs_new_rot(&self) -> bool {
        self.rot.needs_new_rot(self.esize).await
    }

    /// Get a reference to the inner [Rotator] (for listing ratations, reading from a rotation, etc.).
    pub fn get_rotator(&self) -> &R {
        &self.rot
    }
}

/// RotAppender based on [TokioFileIoBackend] and [DefaultTokioFileRotator].
pub type TokioFileRotAppender = RotAppender<TokioFileIoBackend, DefaultTokioFileRotator>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::*;
    use bytes::BytesMut;
    use rand::RngExt;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_default_rot_name_provider() {
        let test_dir = "/tmp";
        let rnp = DefaultRotNameProvider::<TokioFileIoBackend>::new(test_dir, "drnp_test_async.")
            .await
            .unwrap();
        let name1 = rnp.gen_rot_name(3).await;
        let name2 = rnp.gen_rot_name(5).await;
        assert_eq!(name1, String::from("drnp_test_async.0000000003"));
        assert_eq!(name2, String::from("drnp_test_async.0000000005"));
        let res = rnp.restore_latest_rot_info().await.unwrap();
        assert!(res.is_none());
        let opts = OpenOptions::new().with_create(true);
        let paths = vec![format!("/tmp/{}", name1), format!("/tmp/{}", name2)];
        let exp_names = vec![name1, name2];
        TokioFileIoBackend::open_writer(&paths[0], &opts)
            .await
            .unwrap();
        TokioFileIoBackend::open_writer(&paths[1], &opts)
            .await
            .unwrap();
        let names = rnp.list_rot_names().await.unwrap();
        assert_eq!(names, exp_names);
        let res = rnp.restore_latest_rot_info().await.unwrap().unwrap();
        assert_eq!(res.0, 5);
        assert_eq!(res.1, exp_names[1]);
        TokioFileIoBackend::remove_file(&paths[0]).await.unwrap();
        TokioFileIoBackend::remove_file(&paths[1]).await.unwrap();
        println!("{}, {}", &paths[0], &paths[1]);
    }

    #[tokio::test]
    async fn test_default_rotator() {
        let capa = 2560usize;
        let mut dr = DefaultTokioFileRotator::new("/tmp", "dr_test_async.", 3, 4096)
            .await
            .unwrap();
        assert!(!dr.needs_new_rot(1095).await);
        assert!(dr.needs_new_rot(4096).await);
        assert!(dr.needs_new_rot(4097).await);
        let mut buf_mut = BytesMut::with_capacity(capa);
        unsafe { buf_mut.set_len(capa) };
        let mut rng = rand::rng();
        rng.fill(&mut buf_mut);
        let buf = buf_mut.freeze();
        let mut wr = dr.init_appending(false).await.unwrap().0;
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        let mut wr = dr.new_rot(false).await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        assert_eq!(dr.list_rot_names().await.unwrap().len(), 2);
        let mut wr = dr.new_rot(false).await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        assert_eq!(dr.list_rot_names().await.unwrap().len(), 3);
        let mut wr = dr.new_rot(false).await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        assert_eq!(dr.list_rot_names().await.unwrap().len(), 3);
        let mut wr = dr.new_rot(false).await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        let exp_names = vec![
            String::from("dr_test_async.0000000002"),
            String::from("dr_test_async.0000000003"),
            String::from("dr_test_async.0000000004"),
        ];
        assert_eq!(dr.list_rot_names().await.unwrap(), exp_names);
        let mut rd = dr
            .open_read_rot("dr_test_async.0000000004", false)
            .await
            .unwrap();
        assert_eq!(rd.1, (capa * 2) as u64);
        let mut rd_buf_mut = BytesMut::with_capacity(capa);
        unsafe { rd_buf_mut.set_len(capa) };
        rd.0.read_exact(&mut rd_buf_mut).await.unwrap();
        assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
        let mut dr = DefaultTokioFileRotator::new("/tmp", "dr_test_async.", 3, 4096)
            .await
            .unwrap();
        let mut wr = dr.init_appending(false).await.unwrap().0;
        wr.write_all(&buf.clone()).await.unwrap();
        wr.flush().await.unwrap();
        let rd = dr
            .open_read_rot("dr_test_async.0000000004", false)
            .await
            .unwrap();
        assert_eq!(rd.1, (capa * 3) as u64);
        dr.remove_rot(&exp_names[0]).await.unwrap();
        dr.remove_rot(&exp_names[1]).await.unwrap();
        dr.remove_rot(&exp_names[2]).await.unwrap();
        let mut wr = dr.new_rot(true).await.unwrap();
        wr.write_all(b"dummy").await.unwrap();
        wr.write_all(b"dummy").await.unwrap();
        wr.write_all(&buf.clone()).await.unwrap();
        let rd = dr
            .open_read_rot("dr_test_async.0000000005", true)
            .await
            .unwrap();
        assert_eq!(rd.1, (capa + 10) as u64);
        dr.remove_rot("dr_test_async.0000000005").await.unwrap();
    }

    #[tokio::test]
    async fn test_rot_appender() {
        let capa = 3072usize;
        let rotator = DefaultTokioFileRotator::new("/tmp", "ra_test_async.", 3, 8096)
            .await
            .unwrap();
        let mut appender = TokioFileRotAppender::new(rotator, false).await.unwrap();
        let mut buf_mut = BytesMut::with_capacity(capa);
        unsafe { buf_mut.set_len(capa) };
        let mut rng = rand::rng();
        rng.fill(&mut buf_mut);
        let buf = buf_mut.freeze();
        let mut idx = 0usize;
        while idx < 16 {
            appender.append(&buf.clone(), true).await.unwrap();
            idx += 1;
            let rots = appender.get_rotator().list_rot_names().await.unwrap();
            let rot_cnt = (((idx + 3) / 3) as usize).min(3);
            let buf_cnt = idx % 3;
            assert_eq!(appender.esize, (buf_cnt * capa) as u64);
            assert_eq!(rots.len(), rot_cnt, "idx={}", idx);
        }
        appender.flush().await.unwrap();
        let exp_rots = vec![
            String::from("ra_test_async.0000000003"),
            String::from("ra_test_async.0000000004"),
            String::from("ra_test_async.0000000005"),
        ];
        let rots = appender.get_rotator().list_rot_names().await.unwrap();
        assert_eq!(rots, exp_rots);
        let exp_sizes = [capa * 3, capa * 3, capa];
        for (idx, name) in rots.iter().enumerate() {
            let mut rd = appender
                .get_rotator()
                .open_read_rot(name, false)
                .await
                .unwrap();
            assert_eq!(rd.1, exp_sizes[idx] as u64);
            let mut rd_buf_mut = BytesMut::with_capacity(capa);
            unsafe { rd_buf_mut.set_len(capa) };
            rd.0.read_exact(&mut rd_buf_mut).await.unwrap();
            assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
        }
        let rotator = DefaultTokioFileRotator::new("/tmp", "ra_test_async.", 3, 8096)
            .await
            .unwrap();
        let mut appender = TokioFileRotAppender::new(rotator, false).await.unwrap();
        while idx < 32 {
            appender.append(&buf.clone(), true).await.unwrap();
            idx += 1;
            let rots = appender.get_rotator().list_rot_names().await.unwrap();
            let buf_cnt = idx % 3;
            assert_eq!(appender.esize, (buf_cnt * capa) as u64);
            assert_eq!(rots.len(), 3);
        }
        let exp_rots = vec![
            String::from("ra_test_async.0000000008"),
            String::from("ra_test_async.0000000009"),
            String::from("ra_test_async.0000000010"),
        ];
        let rots = appender.get_rotator().list_rot_names().await.unwrap();
        assert_eq!(rots, exp_rots);
        let exp_sizes = [capa * 3, capa * 3, capa * 2];
        for (idx, name) in rots.iter().enumerate() {
            let mut rd = appender
                .get_rotator()
                .open_read_rot(name, false)
                .await
                .unwrap();
            assert_eq!(rd.1, exp_sizes[idx] as u64);
            let mut rd_buf_mut = BytesMut::with_capacity(capa);
            unsafe { rd_buf_mut.set_len(capa) };
            rd.0.read_exact(&mut rd_buf_mut).await.unwrap();
            assert_eq!(rd_buf_mut.split_to(capa).freeze(), buf);
            appender.get_rotator().remove_rot(name).await.unwrap();
        }
    }
}
