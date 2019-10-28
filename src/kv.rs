use crate::log::{Log, LogEntry};
use failure::Error;
use std::path::Path;

/// Return type for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `Log` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    log: Log,
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn new() -> KvStore {
        KvStore {
            log: Log::default(),
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        Ok(self.log.push(LogEntry::Set(key, value)))
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.log.get_value(key))
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        Ok(self.log.push(LogEntry::Rm(key)))
    }

    /// Open file containing log for processing.
    pub fn open(_path: &Path) -> Result<Self> {
        Ok(KvStore::new())
    }
}
