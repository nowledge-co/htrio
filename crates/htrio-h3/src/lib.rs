//! HTTP/3 client and server
//!
//! This is htrio's local fork of hyperium/h3. The Cargo package is `htrio-h3`;
//! its library name remains `h3` for source compatibility with protocol adapters.
//! See `UPSTREAM.md` for the pinned revision and local changes.
#![deny(missing_docs, clippy::self_named_module_files)]
#![allow(clippy::derive_partial_eq_without_eq)]

pub mod client;

mod config;

/// Remembered peer limits for accepted HTTP/3 early data.
pub use config::PeerSettings;
//pub mod error;
pub mod ext;
pub mod priority;
pub mod quic;

pub mod server;

//pub use error::Error;

mod buf;

mod shared_state;

#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
pub use shared_state::{ConnectionState, SharedState};

pub mod error;

#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
#[allow(missing_docs)]
pub mod connection;
#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
#[allow(missing_docs)]
pub mod frame;
#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
#[allow(missing_docs)]
pub mod proto;
#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
#[allow(missing_docs)]
pub mod stream;
#[cfg(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes")]
#[allow(missing_docs)]
pub mod webtransport;

#[cfg(not(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes"))]
mod connection;
#[cfg(not(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes"))]
mod frame;
#[cfg(not(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes"))]
mod proto;
#[cfg(not(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes"))]
mod stream;
#[cfg(not(feature = "i-implement-a-third-party-backend-and-opt-into-breaking-changes"))]
mod webtransport;

#[allow(dead_code)]
mod qpack;

/// Connection header compression counters.
pub use qpack::state::Stats as QpackStats;
#[cfg(test)]
mod tests;
#[cfg(test)]
extern crate self as h3;
