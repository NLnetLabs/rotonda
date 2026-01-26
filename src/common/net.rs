//--- TCP listener traits ----------------------------------------------------
//
// These traits enable us to swap out the real TCP listener for a mock when
// testing.

use std::net::SocketAddr;

use tokio::net::TcpStream;

#[async_trait::async_trait]
pub trait TcpListenerFactory<T> {
    async fn bind(&self, addr: String) -> std::io::Result<T>;
}

#[async_trait::async_trait]
pub trait TcpListener<T> {
    async fn accept(&self) -> std::io::Result<(T, SocketAddr)>;
}

#[async_trait::async_trait]
pub trait TcpStreamWrapper {
    fn into_inner(self) -> std::io::Result<TcpStream>;
}

/// A thin wrapper around the real Tokio TcpListener.
pub struct StandardTcpListenerFactory;

#[async_trait::async_trait]
impl TcpListenerFactory<StandardTcpListener> for StandardTcpListenerFactory {
    async fn bind(
        &self,
        addr: String,
    ) -> std::io::Result<StandardTcpListener> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(StandardTcpListener(listener))
    }
}

pub struct StandardTcpListener(pub(crate) ::tokio::net::TcpListener);

/// A thin wrapper around the real Tokio TcpListener bind call.
#[async_trait::async_trait]
impl TcpListener<StandardTcpStream> for StandardTcpListener {
    async fn accept(
        &self,
    ) -> std::io::Result<(StandardTcpStream, SocketAddr)> {
        let (stream, addr) = self.0.accept().await?;
        Ok((StandardTcpStream(stream), addr))
    }
}

pub struct StandardTcpStream(::tokio::net::TcpStream);

/// A thin wrapper around the Tokio TcpListener accept() call result.
impl TcpStreamWrapper for StandardTcpStream {
    fn into_inner(self) -> std::io::Result<TcpStream> {
        Ok(self.0)
    }
}
