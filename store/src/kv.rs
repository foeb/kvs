use crate::wal::{Log, LogEntry};
use crate::{Error, Result};
use std::fs::OpenOptions;
use std::path::Path;

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
    /// Creates a `KvStore` by opening the given path as a log.
    pub fn open(path: &Path) -> Result<KvStore> {
        let file_path = if path.is_dir() {
            path.join("wal")
        } else {
            path.to_owned()
        };
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path)?;
        let log = Log::new(file)?;
        Ok(KvStore { log })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.log.push(LogEntry::Set(key, value))
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.log.get_value(&key)?)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.log.contains_key(&key) {
            Err(Error::NonExistentKey(key))
        } else {
            self.log.push(LogEntry::Rm(key))
        }
    }
}
