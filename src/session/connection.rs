use std::io::{Error, ErrorKind, IoSlice, Result};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use bytes::buf::BufMut;
use ignore_result::Ignore;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tokio::{select, time};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::deadline::Deadline;
use crate::endpoint::{EndpointRef, IterableEndpoints};

const NOOP_VTABLE: RawWakerVTable =
    RawWakerVTable::new(|_| RawWaker::new(ptr::null(), &NOOP_VTABLE), |_| {}, |_| {}, |_| {});
const NOOP_WAKER: RawWaker = RawWaker::new(ptr::null(), &NOOP_VTABLE);

pub enum Connection {
    Tls(TlsStream<TcpStream>),
    Raw(TcpStream),
}

impl AsyncRead for Connection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Raw(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl Connection {
    pub fn new_raw(stream: TcpStream) -> Self {
        Self::Raw(stream)
    }

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
            Self::Tls(stream) => {
                let (stream, _session) = stream.get_ref();
                stream.writable().await
            },
        }
    }

    pub fn wants_write(&self) -> bool {
        match self {
            Self::Raw(_) => false,
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
    tls: TlsConnector,
    timeout: Duration,
}

impl Connector {
    pub fn new(config: impl Into<Arc<ClientConfig>>) -> Self {
        Self { tls: TlsConnector::from(config.into()), timeout: Duration::from_secs(10) }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub async fn connect(&self, endpoint: EndpointRef<'_>, deadline: &mut Deadline) -> Result<Connection> {
        select! {
            _ = unsafe { Pin::new_unchecked(deadline) } => Err(Error::new(ErrorKind::TimedOut, "deadline exceed")),
            _ = time::sleep(self.timeout) => Err(Error::new(ErrorKind::TimedOut, format!("connection timeout{:?} exceed", self.timeout))),
            r = TcpStream::connect((endpoint.host, endpoint.port)) => {
                match r {
                    Err(err) => Err(err),
                    Ok(sock) => {
                        let connection = if endpoint.tls {
                            let domain = ServerName::try_from(endpoint.host).unwrap().to_owned();
                            let stream = self.tls.connect(domain, sock).await?;
                            Connection::new_tls(stream)
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
            if let Ok(conn) = self.connect(endpoint, &mut deadline).await {
                if let Ok(true) = conn.command_isro().await {
                    return Some(unsafe { std::mem::transmute(endpoint) });
                }
            }
            endpoints.step();
            if i % n == 0 {
                log::debug!("ZooKeeper fails to contact writable server from endpoints {:?}", endpoints.endpoints());
                time::sleep(timeout).await;
                timeout = max_timeout.min(timeout * 2);
            } else {
                time::sleep(Duration::from_millis(5)).await;
            }
        }
        None
    }
}
