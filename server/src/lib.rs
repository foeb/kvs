#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod kv;

pub use kv::KvStore;
