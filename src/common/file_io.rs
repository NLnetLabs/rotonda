//! Support for dumping of BMP messages to files for diagnostic purposes.

use async_trait::async_trait;
use tokio::io::AsyncWrite;

// --- FileIo trait ---------------------------------------------------------

#[async_trait]
/// A trait for abstracting filesystem operations so that they can be mocked.
pub(crate) trait FileIo: Default {
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

    async fn flush<W: AsyncWrite + Unpin + Send>(&mut self, writer: W) -> std::io::Result<()>;
}

// --- FileIo trait: real filesystem implementation -------------------------

#[cfg(not(test))]
mod fileio {
    //! Filesystem I/O.
    use async_trait::async_trait;
    use tokio::io::AsyncWrite;

    // use crate::units::bmp_tcp_in::io::dump_bmp_msg;

    #[derive(Default)]
    pub(crate) struct RealFileIo;

    #[async_trait]
    impl super::FileIo for RealFileIo {
        async fn rename<P, Q>(&mut self, from: P, to: Q) -> std::io::Result<()>
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
    }
}

// --- FileIo trait: mock implementation ------------------------------------

#[cfg(test)]
mod fileio {
    ///! Mock I/O.
    use async_trait::async_trait;
    use std::path::PathBuf;
    use tokio::io::AsyncWrite;

    #[derive(Default)]
    pub(crate) struct MockFileIo {
        pub rename_calls: Vec<(PathBuf, PathBuf)>,
        pub remove_file_calls: Vec<PathBuf>,
        pub write_all_calls: usize,
    }

    impl MockFileIo {
        pub fn _is_unchanged(&self) -> bool {
            self.rename_calls.is_empty()
                && self.remove_file_calls.is_empty()
                && self.write_all_calls == 0
        }
    }

    #[async_trait]
    impl super::FileIo for MockFileIo {
        async fn rename<P, Q>(&mut self, from: P, to: Q) -> std::io::Result<()>
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

        async fn flush<W: AsyncWrite + Unpin + Send>(&mut self, _writer: W) -> std::io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(not(test))]
pub(crate) use fileio::RealFileIo as TheFileIo;

#[cfg(test)]
pub(crate) use fileio::MockFileIo as TheFileIo;
