#![feature(seek_convenience)]
//! A custom file format for KVStore's log-structured storage.
//! The idea is to make compaction easier by separating the values from the logs
//! into a separate data file. Every entry in the log has a fixed size, making
//! it easy to compact in place. The data files currently are just unstructured
//! blobs of bytes, but could be made smarter in the future.

#[macro_use]
extern crate log;

mod entry;
mod error;
mod reader;
mod writer;

pub mod page;
pub mod slotted;
pub mod index;

pub use entry::{file, mem};
pub use error::{Error, Result};
pub use reader::LogReader;
pub use writer::LogWriter;
