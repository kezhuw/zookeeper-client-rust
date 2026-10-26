#[cfg(not(feature = "turmoil"))]
mod async_net;
#[cfg(feature = "turmoil")]
mod turmoil;

// Cargo features are additive, so `turmoil` can't drop the non-optional `async-net` it replaces.
// Mark it used to satisfy `unused-crate-dependencies`.
#[cfg(feature = "turmoil")]
use async_net as _;
#[cfg(not(feature = "turmoil"))]
pub(crate) use async_net::{connect, TcpStream};
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::{connect, TcpStream};
