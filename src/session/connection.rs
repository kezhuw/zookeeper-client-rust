use std::io::{Error, ErrorKind, IoSlice, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_io::Timer;
use async_net::TcpStream;
use asyncs::select;
use bytes::buf::BufMut;
use futures::io::BufReader;
use futures::prelude::*;
use futures_lite::AsyncReadExt;
use ignore_result::Ignore;
use tracing::{debug, trace};

#[cfg(feature = "tls")]
mod tls {
    pub use std::sync::Arc;

    pub use futures_rustls::client::TlsStream;
    pub use futures_rustls::TlsConnector;
    pub use rustls::pki_types::ServerName;
    pub use rustls::ClientConfig;
}
#[cfg(feature = "tls")]
use tls::*;

use crate::deadline::Deadline;
use crate::endpoint::{EndpointRef, IterableEndpoints};

#[derive(Debug)]
pub enum Connection {
    Raw(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<TlsStream<TcpStream>>),
}

pub trait AsyncReadToBuf: AsyncReadExt {
    async fn read_to_buf(&mut self, buf: &mut impl BufMut) -> Result<usize>
    where
        Self: Unpin, {
        let chunk = buf.chunk_mut();
        let read_to =
            unsafe { std::mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(chunk.as_uninit_slice_mut()) };
        let n = self.read(read_to).await?;
        if n != 0 {
            unsafe {
                buf.advance_mut(n);
            }
        }
        Ok(n)
    }
}

impl<T> AsyncReadToBuf for T where T: AsyncReadExt {}

impl AsyncRead for Connection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<Result<usize>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_close(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_close(cx),
        }
    }
}

pub struct ConnReader<'a> {
    conn: &'a mut Connection,
}

impl AsyncRead for ConnReader<'_> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.get_mut().conn).poll_read(cx, buf)
    }
}

pub struct ConnWriter<'a> {
    conn: &'a mut Connection,
}

impl AsyncWrite for ConnWriter<'_> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.get_mut().conn).poll_write(cx, buf)
    }

    fn poll_write_vectored(self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<Result<usize>> {
        Pin::new(&mut self.get_mut().conn).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_close(cx)
    }
}

impl Connection {
    pub fn new_raw(stream: TcpStream) -> Self {
        Self::Raw(stream)
    }

    pub fn split(&mut self) -> (ConnReader<'_>, ConnWriter<'_>) {
        let reader = ConnReader { conn: self };
        let writer = ConnWriter { conn: unsafe { std::ptr::read(&reader.conn) } };
        (reader, writer)
    }

    #[cfg(feature = "tls")]
    pub fn new_tls(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(stream.into())
    }

    pub async fn command(mut self, cmd: &str) -> Result<String> {
        // let mut stream = BufStream::new(self);
        self.write_all(cmd.as_bytes()).await?;
        self.flush().await?;
        let mut line = String::new();
        let mut reader = BufReader::new(self);
        reader.read_line(&mut line).await?;
        reader.close().await.ignore();
        Ok(line)
    }

    pub async fn command_isro(self) -> Result<bool> {
        let r = self.command("isro").await?;
        if r == "rw" {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[derive(Clone)]
pub struct Connector {
    #[cfg(feature = "tls")]
    tls: Option<TlsConnector>,
    timeout: Duration,
}

impl Connector {
    #[cfg(feature = "tls")]
    pub fn new() -> Self {
        Self { tls: None, timeout: Duration::from_secs(10) }
    }

    #[cfg(not(feature = "tls"))]
    pub fn new() -> Self {
        Self { timeout: Duration::from_secs(10) }
    }

    #[cfg(feature = "tls")]
    pub fn with_tls(config: ClientConfig) -> Self {
        Self { tls: Some(TlsConnector::from(Arc::new(config))), timeout: Duration::from_secs(10) }
    }

    #[cfg(feature = "tls")]
    async fn connect_tls(&self, stream: TcpStream, host: &str) -> Result<Connection> {
        let domain = ServerName::try_from(host).unwrap().to_owned();
        let stream = self.tls.as_ref().unwrap().connect(domain, stream).await?;
        Ok(Connection::new_tls(stream))
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub async fn connect(&self, endpoint: EndpointRef<'_>, deadline: &mut Deadline) -> Result<Connection> {
        if endpoint.tls {
            #[cfg(feature = "tls")]
            if self.tls.is_none() {
                return Err(Error::new(ErrorKind::Unsupported, "tls not configured"));
            }
            #[cfg(not(feature = "tls"))]
            return Err(Error::new(ErrorKind::Unsupported, "tls not supported"));
        }
        select! {
            _ = unsafe { Pin::new_unchecked(deadline) } => Err(Error::new(ErrorKind::TimedOut, "deadline exceed")),
            _ = Timer::after(self.timeout) => Err(Error::new(ErrorKind::TimedOut, format!("connection timeout{:?} exceed", self.timeout))),
            r = TcpStream::connect((endpoint.host, endpoint.port)) => {
                match r {
                    Err(err) => Err(err),
                    Ok(sock) => {
                        let connection = if endpoint.tls {
                            #[cfg(not(feature = "tls"))]
                            unreachable!("tls not supported");
                            #[cfg(feature = "tls")]
                            self.connect_tls(sock, endpoint.host).await?
                        } else {
                            Connection::new_raw(sock)
                        };
                        Ok(connection)
                    },
                }
            },
        }
    }

    pub async fn seek_for_writable(self, endpoints: &mut IterableEndpoints) -> Option<EndpointRef<'_>> {
        let n = endpoints.len();
        let max_timeout = Duration::from_secs(60);
        let mut i = 0;
        let mut timeout = Duration::from_millis(100);
        let mut deadline = Deadline::never();
        while let Some(endpoint) = endpoints.peek() {
            i += 1;
            match self.connect(endpoint, &mut deadline).await {
                Ok(conn) => match conn.command_isro().await {
                    // Safety: https://github.com/rust-lang/rust/issues/74068
                    Ok(true) => {
                        return Some(unsafe { std::mem::transmute::<EndpointRef<'_>, EndpointRef<'_>>(endpoint) })
                    },
                    Ok(false) => trace!("succeeds to contact readonly {}", endpoint),
                    Err(err) => trace!(%err, r#"fails to complete "isro" to {}"#, endpoint),
                },
                Err(err) => trace!(%err, "fails to contact {}", endpoint),
            }
            endpoints.step();
            if i % n == 0 {
                debug!(
                    sleep = timeout.as_millis(),
                    "fails to contact writable server from endpoints {:?}",
                    endpoints.endpoints()
                );
                Timer::after(timeout).await;
                timeout = max_timeout.min(timeout * 2);
            } else {
                Timer::after(Duration::from_millis(5)).await;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::Connector;
    use crate::deadline::Deadline;
    use crate::endpoint::EndpointRef;

    #[asyncs::test]
    async fn raw() {
        let connector = Connector::new();
        let endpoint = EndpointRef::new("host1", 2181, true);
        let err = connector.connect(endpoint, &mut Deadline::never()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }
}
