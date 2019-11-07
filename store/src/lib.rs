//! A simple key/value store.
#[macro_use]
extern crate log;

pub mod error;
mod kv;

pub use error::Error;
pub use kv::KvStore;
pub use kv::KvsEngine;

/// Return type for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;
