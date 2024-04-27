use std::io::{Error, ErrorKind, IoSlice, Result};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use bytes::buf::BufMut;
use ignore_result::Ignore;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tokio::{select, time};
use tracing::{debug, trace};

#[cfg(feature = "tls")]
mod tls {
    pub use std::sync::Arc;

    pub use rustls::pki_types::ServerName;
    pub use rustls::ClientConfig;
    pub use tokio_rustls::client::TlsStream;
    pub use tokio_rustls::TlsConnector;
}
#[cfg(feature = "tls")]
use tls::*;

use crate::deadline::Deadline;
use crate::endpoint::{EndpointRef, IterableEndpoints};

const NOOP_VTABLE: RawWakerVTable =
    RawWakerVTable::new(|_| RawWaker::new(ptr::null(), &NOOP_VTABLE), |_| {}, |_| {}, |_| {});
const NOOP_WAKER: RawWaker = RawWaker::new(ptr::null(), &NOOP_VTABLE);

#[derive(Debug)]
pub enum Connection {
    Raw(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for Connection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl Connection {
    pub fn new_raw(stream: TcpStream) -> Self {
        Self::Raw(stream)
    }

    #[cfg(feature = "tls")]
    pub fn new_tls(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(stream)
    }

    pub fn try_write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let waker = unsafe { Waker::from_raw(NOOP_WAKER) };
        let mut context = Context::from_waker(&waker);
        match Pin::new(self).poll_write_vectored(&mut context, bufs) {
            Poll::Pending => Err(ErrorKind::WouldBlock.into()),
            Poll::Ready(result) => result,
        }
    }

    pub fn try_read_buf(&mut self, buf: &mut impl BufMut) -> Result<usize> {
        let waker = unsafe { Waker::from_raw(NOOP_WAKER) };
        let mut context = Context::from_waker(&waker);
        let chunk = buf.chunk_mut();
        let mut read_buf = unsafe { ReadBuf::uninit(chunk.as_uninit_slice_mut()) };
        match Pin::new(self).poll_read(&mut context, &mut read_buf) {
            Poll::Pending => Err(ErrorKind::WouldBlock.into()),
            Poll::Ready(Err(err)) => Err(err),
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                unsafe { buf.advance_mut(n) };
                Ok(n)
            },
        }
    }

    pub async fn readable(&self) -> Result<()> {
        match self {
            Self::Raw(stream) => stream.readable().await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => {
                let (stream, session) = stream.get_ref();
                if session.wants_read() {
                    stream.readable().await
                } else {
                    // plaintext data are available for read
                    std::future::ready(Ok(())).await
                }
            },
        }
    }

    pub async fn writable(&self) -> Result<()> {
        match self {
            Self::Raw(stream) => stream.writable().await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => {
                let (stream, _session) = stream.get_ref();
                stream.writable().await
            },
        }
    }

    pub fn wants_write(&self) -> bool {
        match self {
            Self::Raw(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => {
                let (_stream, session) = stream.get_ref();
                session.wants_write()
            },
        }
    }

    pub fn try_flush(&mut self) -> Result<()> {
        let waker = unsafe { Waker::from_raw(NOOP_WAKER) };
        let mut context = Context::from_waker(&waker);
        match Pin::new(self).poll_flush(&mut context) {
            Poll::Pending => Err(ErrorKind::WouldBlock.into()),
            Poll::Ready(result) => result,
        }
    }

    pub async fn command(self, cmd: &str) -> Result<String> {
        let mut stream = BufStream::new(self);
        stream.write_all(cmd.as_bytes()).await?;
        stream.flush().await?;
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        stream.shutdown().await.ignore();
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
    #[allow(dead_code)]
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
                return Err(Error::new(ErrorKind::Unsupported, "tls not supported"));
            }
            #[cfg(not(feature = "tls"))]
            return Err(Error::new(ErrorKind::Unsupported, "tls not supported"));
        }
        select! {
            _ = unsafe { Pin::new_unchecked(deadline) } => Err(Error::new(ErrorKind::TimedOut, "deadline exceed")),
            _ = time::sleep(self.timeout) => Err(Error::new(ErrorKind::TimedOut, format!("connection timeout{:?} exceed", self.timeout))),
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
                    Ok(true) => return Some(unsafe { std::mem::transmute(endpoint) }),
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
                time::sleep(timeout).await;
                timeout = max_timeout.min(timeout * 2);
            } else {
                time::sleep(Duration::from_millis(5)).await;
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

    #[tokio::test]
    async fn raw() {
        let connector = Connector::new();
        let endpoint = EndpointRef::new("host1", 2181, true);
        let err = connector.connect(endpoint, &mut Deadline::never()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }
}
