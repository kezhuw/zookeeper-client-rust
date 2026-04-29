#[cfg(not(feature = "turmoil"))]
mod async_net;
#[cfg(feature = "turmoil")]
mod turmoil;

#[cfg(not(feature = "turmoil"))]
pub(crate) use async_net::{connect, TcpStream};
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::{connect, TcpStream};
