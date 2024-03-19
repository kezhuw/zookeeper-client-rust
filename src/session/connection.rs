use std::io::{self, Read, Write};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection};
use tokio::net::TcpStream;

use crate::error::Error;

pub struct Connection {
    tls: Option<ClientConnection>,
    stream: TcpStream,
}

struct WrappingStream<'a> {
    stream: &'a TcpStream,
}

impl io::Read for WrappingStream<'_> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        self.stream.try_read_buf(&mut buf)
    }
}

impl io::Write for WrappingStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.try_write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.stream.try_write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Connection {
    pub fn new_raw(stream: TcpStream) -> Self {
        Self { tls: None, stream }
    }

    pub fn new_tls(host: &str, config: Arc<ClientConfig>, stream: TcpStream) -> Result<Self, Error> {
        let name = match ServerName::try_from(host) {
            Err(_) => return Err(Error::BadArguments(&"invalid server dns name")),
            Ok(name) => name.to_owned(),
        };
        let client = match ClientConnection::new(config, name) {
            Err(err) => return Err(Error::other(format!("fail to create tls client for host({host}): {err}"), err)),
            Ok(client) => client,
        };
        Ok(Self { tls: Some(client), stream })
    }

    pub fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let Some(client) = self.tls.as_mut() else {
            return self.stream.try_write_vectored(bufs);
        };
        let n = client.writer().write_vectored(bufs)?;
        let mut stream = WrappingStream { stream: &self.stream };
        client.write_tls(&mut stream)?;
        Ok(n)
    }

    pub fn read_buf(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let Some(client) = self.tls.as_mut() else {
            return self.stream.try_read_buf(buf);
        };
        let mut stream = WrappingStream { stream: &self.stream };
        let mut read_bytes = 0;
        loop {
            match client.read_tls(&mut stream) {
                // We may have plaintext to return though tcp stream has been closed.
                // If not, read_bytes should be zero.
                Ok(0) => break,
                Ok(_) => {},
                Err(err) => match err.kind() {
                    // backpressure: tls buffer is full, let's process_new_packets.
                    io::ErrorKind::Other => {},
                    io::ErrorKind::WouldBlock if read_bytes == 0 => {
                        return Err(err);
                    },
                    _ => break,
                },
            }
            let state = client.process_new_packets().map_err(io::Error::other)?;
            let n = state.plaintext_bytes_to_read();
            buf.reserve(n);
            let slice = unsafe { &mut std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.len() + n)[buf.len()..] };
            client.reader().read_exact(slice).unwrap();
            unsafe { buf.set_len(buf.len() + n) };
            read_bytes += n;
        }
        Ok(read_bytes)
    }

    pub async fn readable(&self) -> io::Result<()> {
        let Some(client) = self.tls.as_ref() else {
            return self.stream.readable().await;
        };
        if client.wants_read() {
            self.stream.readable().await
        } else {
            // plaintext data are available for read
            std::future::ready(Ok(())).await
        }
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.stream.writable().await
    }

    pub fn wants_write(&self) -> bool {
        self.tls.as_ref().map(|tls| tls.wants_write()).unwrap_or(false)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let Some(client) = self.tls.as_mut() else {
            return Ok(());
        };
        let mut stream = WrappingStream { stream: &self.stream };
        while client.wants_write() {
            client.write_tls(&mut stream)?;
        }
        Ok(())
    }
}
