use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub(crate) type TcpStream = Compat<turmoil::net::TcpStream>;

pub(crate) async fn connect(host: &str, port: u16) -> std::io::Result<TcpStream> {
    turmoil::net::TcpStream::connect((host, port)).await.map(TokioAsyncReadCompatExt::compat)
}
