//! This crate holds the data types for the log-structured storage in the key-value store.
//! 
//! Records are split up into pages, each with a corresponding data file holding the byte-string
//! values. There's also a single index file which is used to quickly sort through the pages on
//! a `get` command.

pub mod page;
pub mod slotted;
pub mod index;

mod error;
pub use error::{Error, Result};
