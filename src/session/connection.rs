use std::io::{ErrorKind, IoSlice, Result};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use bytes::buf::BufMut;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

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
}
