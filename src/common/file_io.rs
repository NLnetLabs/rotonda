//! Support for dumping of BMP messages to files for diagnostic purposes.
//! 

use std::path::Path;

use async_trait::async_trait;
use tokio::io::AsyncWrite;

// --- FileIo trait ----------------------------------------------------------

#[async_trait]
/// A trait for abstracting filesystem operations so that they can be mocked.
pub trait FileIo: Default {
    async fn rename<P, Q>(&mut self, from: P, to: Q) -> std::io::Result<()>
    where
        P: AsRef<std::path::Path> + Send + Sync,
        Q: AsRef<std::path::Path> + Send + Sync;

    async fn remove_file<P>(&mut self, path: P) -> std::io::Result<()>
    where
        P: AsRef<std::path::Path> + Send + Sync;

    async fn write_all<W: AsyncWrite + Unpin + Send, T>(
        &mut self,
        writer: W,
        bytes: T,
    ) -> std::io::Result<()>
    where
        T: AsRef<[u8]> + Send + Sync;

    async fn flush<W: AsyncWrite + Unpin + Send>(
        &mut self,
        writer: W,
    ) -> std::io::Result<()>;

    fn read_to_string<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::io::Result<String>;

    fn read_dir<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::io::Result<fileio::ReadDir>;
}

// --- FileIo trait: real filesystem implementation --------------------------
#[cfg(not(test))]
mod fileio {
    //! Filesystem I/O.
    use std::{
        io::Error,
        path::Path,
    };

    use async_trait::async_trait;
    use tokio::io::AsyncWrite;

    // use crate::units::bmp_tcp_in::io::dump_bmp_msg;

    #[derive(Default)]
    pub struct RealFileIo;

    pub type ReadDir = std::fs::ReadDir;

    #[async_trait]
    impl super::FileIo for RealFileIo {
        async fn rename<P, Q>(
            &mut self,
            from: P,
            to: Q,
        ) -> std::io::Result<()>
        where
            P: AsRef<std::path::Path> + Send + Sync,
            Q: AsRef<std::path::Path> + Send + Sync,
        {
            tokio::fs::rename(from, to).await
        }

        async fn remove_file<P>(&mut self, path: P) -> std::io::Result<()>
        where
            P: AsRef<std::path::Path> + Send + Sync,
        {
            tokio::fs::remove_file(path).await
        }

        async fn write_all<W: AsyncWrite + Unpin + Send, T>(
            &mut self,
            mut writer: W,
            bytes: T,
        ) -> std::io::Result<()>
        where
            T: AsRef<[u8]> + Send + Sync,
        {
            use tokio::io::AsyncWriteExt;
            writer.write_all(bytes.as_ref()).await
        }

        async fn flush<W: AsyncWrite + Unpin + Send>(
            &mut self,
            mut writer: W,
        ) -> std::io::Result<()> {
            use tokio::io::AsyncWriteExt;
            writer.flush().await
        }

        fn read_to_string<P: AsRef<Path>>(
            &self,
            path: P,
        ) -> std::io::Result<String> {
            std::fs::read_to_string(path.as_ref()).map_err(|err| {
                Error::other(
                    format!("For path '{}': {err}", path.as_ref().display()),
                )
            })
        }

        fn read_dir<P: AsRef<Path>>(
            &self,
            path: P,
        ) -> std::io::Result<ReadDir> {
            std::fs::read_dir(path)
        }
    }
}

// --- FileIo trait: mock implementation ------------------------------------

#[cfg(test)]
mod fileio {
    use async_trait::async_trait;
    use std::{
        collections::{HashMap, VecDeque},
        io::{Error, ErrorKind},
        path::{Path, PathBuf},
    };
    use tokio::io::AsyncWrite;

    pub struct ReadDir(VecDeque<PathBuf>);

    pub struct DirEntry(PathBuf);

    impl DirEntry {
        pub fn path(&self) -> PathBuf {
            self.0.clone()
        }
    }

    impl Iterator for ReadDir {
        type Item = std::io::Result<DirEntry>;

        fn next(&mut self) -> Option<std::io::Result<DirEntry>> {
            self.0.pop_front().map(|p| Ok(DirEntry(p)))
        }
    }

    #[derive(Default)]
    pub struct MockFileIo {
        pub readable_paths: HashMap<PathBuf, String>,
        pub rename_calls: Vec<(PathBuf, PathBuf)>,
        pub remove_file_calls: Vec<PathBuf>,
        pub write_all_calls: usize,
    }

    impl MockFileIo {
        #[allow(dead_code)]
        pub fn new<T>(readable_paths: T) -> Self
        where
            T: Into<HashMap<PathBuf, String>>,
        {
            Self {
                readable_paths: readable_paths.into(),
                ..Default::default()
            }
        }

        pub fn _is_unchanged(&self) -> bool {
            self.rename_calls.is_empty()
                && self.remove_file_calls.is_empty()
                && self.write_all_calls == 0
        }
    }

    #[async_trait]
    impl super::FileIo for MockFileIo {
        async fn rename<P, Q>(
            &mut self,
            from: P,
            to: Q,
        ) -> std::io::Result<()>
        where
            P: AsRef<std::path::Path> + Send + Sync,
            Q: AsRef<std::path::Path> + Send + Sync,
        {
            self.rename_calls
                .push((from.as_ref().to_owned(), to.as_ref().to_owned()));
            Ok(())
        }

        async fn remove_file<P>(&mut self, path: P) -> std::io::Result<()>
        where
            P: AsRef<std::path::Path> + Send + Sync,
        {
            self.remove_file_calls.push(path.as_ref().to_owned());
            Ok(())
        }

        async fn write_all<W: AsyncWrite + Unpin + Send, T>(
            &mut self,
            _writer: W,
            _bytes: T,
        ) -> std::io::Result<()>
        where
            T: AsRef<[u8]> + Send + Sync,
        {
            self.write_all_calls += 1;
            Ok(())
        }

        async fn flush<W: AsyncWrite + Unpin + Send>(
            &mut self,
            _writer: W,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn read_to_string<P: AsRef<Path>>(
            &self,
            path: P,
        ) -> std::io::Result<String> {
            self.readable_paths
                .get(path.as_ref())
                .map(|v| v.to_string())
                .ok_or(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Test path {} not known",
                        path.as_ref().display()
                    ),
                ))
        }

        fn read_dir<P: AsRef<Path>>(
            &self,
            path: P,
        ) -> std::io::Result<ReadDir> {
            let paths = self
                .readable_paths
                .keys()
                .filter(|&readable_path| readable_path.starts_with(&path))
                .cloned()
                .collect::<VecDeque<_>>();
            Ok(ReadDir(paths))
        }
    }
}

#[cfg(not(test))]
pub use fileio::RealFileIo as TheFileIo;

#[cfg(test)]
pub use fileio::MockFileIo as TheFileIo;
