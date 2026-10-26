#[cfg(not(feature = "turmoil"))]
mod async_io;
#[cfg(feature = "turmoil")]
mod turmoil;

// Same as `async-net` in `net`: `turmoil` can't drop the non-optional `async-io` it replaces.
#[cfg(feature = "turmoil")]
use async_io as _;
#[cfg(not(feature = "turmoil"))]
pub(crate) use async_io::{now, Timer};
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::{now, Timer};
