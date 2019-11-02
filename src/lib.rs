//! A simple key/value store.

use failure::Error;

#[macro_use] extern crate log;

/// Return type for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;

pub use kv::KvStore;

mod kv;
pub mod wal;
