//! Common IO basics, mainly persistence backend abstraction used in other parts of this crate.
use crate::{Error, Result};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
#[cfg(feature = "async")]
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

/// File open options, used in opening file by IO backend.
#[derive(Default)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub append: bool,
    pub direct: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }
    pub fn with_write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }
    pub fn with_create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }
    pub fn with_append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }
    pub fn with_direct(mut self, direct: bool) -> Self {
        self.direct = direct;
        self
    }
}

/// For a writer that can synchronize contents to the persistent storage.
pub trait Syncable {
    /// Synchronize all contents to the persistent storage.
    fn sync_all(&self) -> Result<()>;
}

impl Syncable for std::fs::File {
    fn sync_all(&self) -> Result<()> {
        Ok(std::fs::File::sync_all(&self)?)
    }
}

/// Persistent IO backend.
pub trait IoBackend: Send + Sync + 'static {
    type Reader: Read + Seek + Sync + Send;
    type Writer: Write + Seek + Syncable + Sync + Send;
    type ReadWriter: Read + Write + Seek + Syncable + Sync + Send;

    /// Make sure a path is a directory.
    fn check_path_is_dir<P: AsRef<Path>>(path: P) -> Result<()>;

    /// Open a reader.
    fn open_reader<P: AsRef<Path>>(path: P, options: &OpenOptions) -> Result<Self::Reader>;

    /// Open a writer.
    fn open_writer<P: AsRef<Path>>(path: P, options: &OpenOptions) -> Result<Self::Writer>;

    /// Open a readwriter.
    fn open_read_writer<P: AsRef<Path>>(path: P, options: &OpenOptions)
    -> Result<Self::ReadWriter>;

    /// List files in a directory, maybe filtered by a filter.
    fn list_files<P: AsRef<Path>, F: Fn(&str) -> bool>(
        dir: P,
        filter: Option<F>,
    ) -> Result<Vec<String>>;

    /// Remove a file.
    fn remove_file<P: AsRef<Path>>(path: P) -> Result<()>;

    /// Return the size of a file.
    fn file_size<P: AsRef<Path>>(path: P) -> Result<u64>;
}

/// [IoBackend] based on [std::fs::File].
#[derive(Debug)]
pub struct StdFileIoBackend {}

impl StdFileIoBackend {
    fn opt_to_std_opt(options: &OpenOptions, read: bool, write: bool) -> std::fs::OpenOptions {
        let mut opt = std::fs::OpenOptions::new();
        if read {
            opt.read(true);
        }
        if write {
            opt.write(true);
            if options.create {
                opt.create(true);
            }
            if options.append {
                opt.append(true);
            }
        }
        if options.direct {
            opt.custom_flags(libc::O_DIRECT);
        }
        opt
    }
}

impl IoBackend for StdFileIoBackend {
    type Reader = std::fs::File;
    type Writer = std::fs::File;
    type ReadWriter = std::fs::File;

    fn check_path_is_dir<P: AsRef<Path>>(path: P) -> Result<()> {
        let meta = std::fs::metadata(&path)?;
        if !meta.is_dir() {
            let path_str = path.as_ref().to_string_lossy().to_string();
            return Err(Error::NotADirectory(path_str));
        }
        Ok(())
    }

    fn open_reader<P: AsRef<Path>>(path: P, options: &OpenOptions) -> Result<Self::Reader> {
        Ok(Self::opt_to_std_opt(options, true, false).open(path)?)
    }

    fn open_writer<P: AsRef<Path>>(path: P, options: &OpenOptions) -> Result<Self::Writer> {
        Ok(Self::opt_to_std_opt(options, false, true).open(path)?)
    }

    fn open_read_writer<P: AsRef<Path>>(
        path: P,
        options: &OpenOptions,
    ) -> Result<Self::ReadWriter> {
        Ok(Self::opt_to_std_opt(options, true, true).open(path)?)
    }

    fn list_files<P: AsRef<Path>, F: Fn(&str) -> bool>(
        dir: P,
        filter: Option<F>,
    ) -> Result<Vec<String>> {
        let rd = dir.as_ref().read_dir()?;
        let mut out = Vec::new();
        for e in rd {
            if let Ok(e) = e {
                if let Ok(typ) = e.file_type() {
                    if typ.is_file() {
                        if let Some(file_name) = e.file_name().to_str() {
                            if filter.as_ref().map_or(true, |f| f(file_name)) {
                                out.push(file_name.to_string());
                            }
                        }
                    }
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
        Ok(std::fs::remove_file(path)?)
    }

    fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        Ok(path.as_ref().metadata()?.len())
    }
}

/// For a writer used in async code that can synchronize contents to the persistent storage.
pub trait AsyncSyncable {
    /// Synchronize all contents to the persistent storage.
    fn sync_all(&self) -> impl Future<Output = Result<()>> + Send;
}

impl AsyncSyncable for tokio::fs::File {
    async fn sync_all(&self) -> Result<()> {
        Ok(tokio::fs::File::sync_all(&self).await?)
    }
}

/// Persistent IO backend used in async code.
#[cfg(feature = "async")]
pub trait AsyncIoBackend {
    type Reader: AsyncRead + AsyncSeek + Unpin + Send + Sync;
    type Writer: AsyncWrite + AsyncSeek + AsyncSyncable + Unpin + Send + Sync;
    type ReadWriter: AsyncRead + AsyncWrite + AsyncSeek + AsyncSyncable + Unpin + Send + Sync;

    fn check_path_is_dir<P: AsRef<Path> + Send>(path: P)
    -> impl Future<Output = Result<()>> + Send;

    fn open_reader<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> impl Future<Output = Result<Self::Reader>> + Send;

    fn open_writer<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> impl Future<Output = Result<Self::Writer>> + Send;

    fn open_read_writer<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> impl Future<Output = Result<Self::ReadWriter>> + Send;

    fn list_files<P: AsRef<Path> + Send, F: Fn(&str) -> bool + Send>(
        dir: P,
        filter: Option<F>,
    ) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Remove a file.
    fn remove_file<P: AsRef<Path> + Send>(path: P) -> impl Future<Output = Result<()>> + Send;

    fn file_size<P: AsRef<Path> + Send>(path: P) -> impl Future<Output = Result<u64>> + Send;
}

/// [AsyncIoBackend] based on [tokio::fs::File].
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct TokioFileIoBackend {}

#[cfg(feature = "async")]
impl TokioFileIoBackend {
    fn opt_to_std_opt(options: &OpenOptions, read: bool, write: bool) -> tokio::fs::OpenOptions {
        let mut opt = tokio::fs::OpenOptions::new();
        if read {
            opt.read(true);
        }
        if write {
            opt.write(true);
            if options.create {
                opt.create(true);
            }
            if options.append {
                opt.append(true);
            }
        }
        if options.direct {
            opt.custom_flags(libc::O_DIRECT);
        }
        opt
    }
}

#[cfg(feature = "async")]
impl AsyncIoBackend for TokioFileIoBackend {
    type Reader = tokio::fs::File;
    type Writer = tokio::fs::File;
    type ReadWriter = tokio::fs::File;

    async fn check_path_is_dir<P: AsRef<Path> + Send>(path: P) -> Result<()> {
        let meta = tokio::fs::metadata(path.as_ref()).await?;
        if !meta.is_dir() {
            let path_str = path.as_ref().to_string_lossy().to_string();
            return Err(Error::NotADirectory(path_str));
        }
        Ok(())
    }

    async fn open_reader<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> Result<Self::Reader> {
        Ok(Self::opt_to_std_opt(options, true, false)
            .open(path.as_ref())
            .await?)
    }

    async fn open_writer<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> Result<Self::Writer> {
        Ok(Self::opt_to_std_opt(options, false, true)
            .open(path.as_ref())
            .await?)
    }

    async fn open_read_writer<P: AsRef<Path> + Send>(
        path: P,
        options: &OpenOptions,
    ) -> Result<Self::ReadWriter> {
        Ok(Self::opt_to_std_opt(options, true, true)
            .open(path.as_ref())
            .await?)
    }

    async fn list_files<P: AsRef<Path> + Send, F: Fn(&str) -> bool + Send>(
        dir: P,
        filter: Option<F>,
    ) -> Result<Vec<String>> {
        let mut rd = tokio::fs::read_dir(dir.as_ref()).await?;
        let mut out = Vec::new();
        while let Ok(Some(e)) = rd.next_entry().await {
            if let Ok(typ) = e.file_type().await {
                if typ.is_file() {
                    if let Some(file_name) = e.file_name().to_str() {
                        if filter.as_ref().map_or(true, |f| f(file_name)) {
                            out.push(file_name.to_string());
                        }
                    }
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    async fn remove_file<P: AsRef<Path> + Send>(path: P) -> Result<()> {
        Ok(tokio::fs::remove_file(path).await?)
    }

    async fn file_size<P: AsRef<Path> + Send>(path: P) -> Result<u64> {
        Ok(tokio::fs::metadata(path.as_ref()).await?.len())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};

    type SharedBuffer = Arc<Mutex<Cursor<Vec<u8>>>>;

    #[derive(Default)]
    struct MemFs {
        files: Mutex<HashMap<PathBuf, SharedBuffer>>,
        directories: Mutex<Vec<PathBuf>>,
    }

    impl MemFs {
        fn global() -> &'static MemFs {
            static FS: OnceLock<MemFs> = OnceLock::new();
            FS.get_or_init(|| {
                let fs = MemFs::default();
                fs.directories.lock().unwrap().push(PathBuf::from("/"));
                fs.directories.lock().unwrap().push(PathBuf::from("."));
                fs
            })
        }

        pub fn reset() {
            let fs = Self::global();
            fs.files.lock().unwrap().clear();
            let mut dirs = fs.directories.lock().unwrap();
            dirs.clear();
            dirs.push(PathBuf::from("/"));
            dirs.push(PathBuf::from("."));
        }

        pub fn add_dir<P: AsRef<Path>>(path: P) {
            let mut dirs = Self::global().directories.lock().unwrap();
            let p = path.as_ref().to_path_buf();
            if !dirs.contains(&p) {
                dirs.push(p);
            }
        }
    }

    pub struct MemStream {
        buffer: SharedBuffer,
    }

    impl MemStream {
        fn new(buffer: SharedBuffer) -> Self {
            Self { buffer }
        }
    }

    impl Read for MemStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut inner = self.buffer.lock().unwrap();
            inner.read(buf)
        }
    }

    impl Write for MemStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut inner = self.buffer.lock().unwrap();
            inner.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            let mut inner = self.buffer.lock().unwrap();
            inner.flush()
        }
    }

    impl Seek for MemStream {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            let mut inner = self.buffer.lock().unwrap();
            inner.seek(pos)
        }
    }

    impl Syncable for MemStream {
        fn sync_all(&self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct MemIoBackend;

    impl MemIoBackend {
        pub fn reset_mem_fs() {
            MemFs::reset();
        }

        pub fn register_dir<P: AsRef<Path>>(path: P) {
            MemFs::add_dir(path);
        }
    }

    impl IoBackend for MemIoBackend {
        type Reader = MemStream;
        type Writer = MemStream;
        type ReadWriter = MemStream;

        fn check_path_is_dir<P: AsRef<Path>>(path: P) -> Result<()> {
            let fs = MemFs::global();
            let path_ref = path.as_ref();
            let dirs = fs.directories.lock().unwrap();

            if dirs.iter().any(|d| d == path_ref) {
                Ok(())
            } else {
                Err(Error::NotADirectory(
                    path_ref.to_string_lossy().into_owned(),
                ))
            }
        }

        fn open_reader<P: AsRef<Path>>(path: P, _options: &OpenOptions) -> Result<Self::Reader> {
            let fs = MemFs::global();
            let files = fs.files.lock().unwrap();
            let path_buf = path.as_ref().to_path_buf();

            if let Some(buf) = files.get(&path_buf) {
                buf.lock().unwrap().set_position(0);
                Ok(MemStream::new(Arc::clone(buf)))
            } else {
                Err(Error::IoError(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File not found",
                )))
            }
        }

        fn open_writer<P: AsRef<Path>>(path: P, _options: &OpenOptions) -> Result<Self::Writer> {
            let fs = MemFs::global();
            let mut files = fs.files.lock().unwrap();
            let path_buf = path.as_ref().to_path_buf();

            let buffer = files
                .entry(path_buf)
                .or_insert_with(|| Arc::new(Mutex::new(Cursor::new(Vec::new()))));

            Ok(MemStream::new(Arc::clone(buffer)))
        }

        fn open_read_writer<P: AsRef<Path>>(
            path: P,
            _options: &OpenOptions,
        ) -> Result<Self::ReadWriter> {
            Self::open_writer(path, _options)
        }

        fn list_files<P: AsRef<Path>, F: Fn(&str) -> bool>(
            dir: P,
            filter: Option<F>,
        ) -> Result<Vec<String>> {
            Self::check_path_is_dir(&dir)?;

            let fs = MemFs::global();
            let files = fs.files.lock().unwrap();
            let dir_path = dir.as_ref();

            let mut result = Vec::new();
            for file_path in files.keys() {
                if let Ok(relative) = file_path.strip_prefix(dir_path) {
                    if let Some(name_str) = relative.to_str() {
                        if !name_str.is_empty() {
                            if let Some(ref f) = filter {
                                if f(name_str) {
                                    result.push(name_str.to_string());
                                }
                            } else {
                                result.push(name_str.to_string());
                            }
                        }
                    }
                }
            }

            Ok(result)
        }

        fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
            let fs = MemFs::global();
            let mut files = fs.files.lock().unwrap();
            if files.remove(path.as_ref()).is_some() {
                Ok(())
            } else {
                Err(Error::IoError(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File not found",
                )))
            }
        }

        fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
            let fs = MemFs::global();
            let files = fs.files.lock().unwrap();

            if let Some(buf) = files.get(path.as_ref()) {
                let inner = buf.lock().unwrap();
                Ok(inner.get_ref().len() as u64)
            } else {
                Err(Error::IoError(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File not found",
                )))
            }
        }
    }

    #[test]
    fn test_mem_io_backend_basic() -> Result<()> {
        MemIoBackend::reset_mem_fs();
        let opts = OpenOptions::new()
            .with_create(true)
            .with_write(true)
            .with_read(true);

        let file_path = "test_dir/file1.txt";
        let mut writer = MemIoBackend::open_writer(file_path, &opts)?;
        writer.write_all(b"Hello, World!")?;
        writer.sync_all()?;

        assert_eq!(MemIoBackend::file_size(file_path)?, 13);

        let mut reader = MemIoBackend::open_reader(file_path, &opts)?;
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        assert_eq!(content, "Hello, World!");

        MemIoBackend::register_dir("test_dir");
        MemIoBackend::check_path_is_dir("test_dir")?;

        let mut writer2 = MemIoBackend::open_writer("test_dir/file2.log", &opts)?;
        writer2.write_all(b"Log data")?;

        let files = MemIoBackend::list_files("test_dir", Some(|f: &str| f.ends_with(".log")))?;
        assert_eq!(files, vec!["file2.log"]);

        MemIoBackend::remove_file(file_path)?;
        assert!(MemIoBackend::open_reader(file_path, &opts).is_err());

        Ok(())
    }
}
