//! `zookeeper-client` is an async Rust client library for Apache ZooKeeper.
//!
//! ## Opinionated API
//! This library is written from scratch. Its API is pretty different from Java counterpart or even
//! other Rust clients. Some of them are listed here for a glance.
//!
//! * No callbacks.
//! * No catch-all watcher.
//! * [StateWatcher] tracks session state updates.
//! * [OneshotWatcher] tracks oneshot ZooKeeper node event.
//! * [PersistentWatcher] tracks persistent and recursive persistent ZooKeeper node events.
//! * No event type `XyzWatchRemoved` as Rust has [Drop].
//! * Most data operations are ordered at future creation time but not polling time.
//! * [Client::chroot] and [Client::clone] enable session sharing cross multiple different rooted clients.
//!
//! ## Feature flags
//! * `tls`: Toggle TLS support.
//! * `fips`: Toggle TLS FIPS mode support. See also [TlsOptions::with_fips].
//! * `fips-only`: Force TLS FIPS mode. This should be considered as an application only feature to
//!   fulfill security requirement at global level.
//! * `sasl`: Toggle SASL support.
//! * `sasl-gssapi`: Toggle only GSSAPI SASL support. This relies on binding package `libgssapi-sys`.
//! * `sasl-digest-md5`: Toggle only DIGEST-MD5 SASL support.
//!
//! ## Async runtime support
//! This library uses [asyncs](https://docs.rs/asyncs) and [spawns](https://docs.rs/spawns) to
//! spawn asynchronous tasks. It exposes runtime feature flags for handy. Please refer their docs
//! for references.
//! * `tokio`: Toggle support for [tokio](https://docs.rs/tokio).
//! * `smol`: Toggle support for [smol](https://docs.rs/smol) builtin global executor.
//! * `async-global-executor`: Toggle support for [async-global-executor](https://docs.rs/async-global-executor).
//!
//! ## Network simulation
//! * `turmoil`: Toggle support for [turmoil](https://docs.rs/turmoil) network simulation. This
//!   enables `tokio`.
//!
//! With `turmoil`, the client's sockets, DNS and timers run on turmoil's simulated network and
//! clock, so it works only inside a turmoil simulation; enable it only in simulation builds.
//! [Client] must be constructed on `tokio` tasks of a turmoil host, e.g. in `Sim::client`, for
//! `turmoil` to function, as its session task runs on the same host.
//!
//! Endpoint shuffle and reconnect jitter draw from [fastrand](https://docs.rs/fastrand)'s
//! thread-local RNG, which turmoil's seed does not cover. Call `fastrand::seed` on the thread
//! running the simulation for reproducible runs.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod acl;
mod chroot;
mod client;
mod deadline;
mod endpoint;
mod error;
mod net;
mod proto;
mod record;
#[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
mod sasl;
mod session;
mod time;
#[cfg(feature = "tls")]
mod tls;
mod util;

pub use self::acl::{Acl, Acls, AuthId, AuthUser, Permission};
pub use self::error::Error;
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use self::tls::{TlsCa, TlsCerts, TlsCertsBuilder, TlsCertsOptions, TlsDynamicCerts, TlsIdentity, TlsOptions};
pub use crate::client::*;
#[cfg(feature = "sasl-digest-md5")]
#[cfg_attr(docsrs, doc(cfg(any(feature = "sasl", feature = "sasl-digest-md5"))))]
pub use crate::sasl::DigestMd5SaslOptions;
#[cfg(feature = "sasl-gssapi")]
#[cfg_attr(docsrs, doc(cfg(any(feature = "sasl", feature = "sasl-gssapi"))))]
pub use crate::sasl::GssapiSaslOptions;
#[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "sasl", feature = "sasl-gssapi", feature = "sasl-digest-md5"))))]
pub use crate::sasl::SaslOptions;
