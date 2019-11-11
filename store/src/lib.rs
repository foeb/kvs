//! A simple key/value store.
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;

use slog::Drain;

fn get_default_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("version" => "0.1"));
    logger
}

pub mod error;
mod kv;

pub use error::Error;
pub use kv::KvStore;
pub use kv::KvsEngine;

/// Return type for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;
