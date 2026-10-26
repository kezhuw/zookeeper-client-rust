pub(crate) type TcpStream = async_net::TcpStream;

pub(crate) async fn connect(host: &str, port: u16) -> std::io::Result<TcpStream> {
    TcpStream::connect((host, port)).await
}
