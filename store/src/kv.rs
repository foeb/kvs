use crate::wal::Log;
use crate::{Error, Result};
use logformat::mem::{Entry, Value};
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
        let log_path = if path.is_dir() {
            path.join("log")
        } else {
            path.to_owned()
        };

        let data_path = log_path.with_extension("data");

        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path)?;

        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(data_path)?;

        Ok(KvStore {
            log: Log::new(log_file, data_file)?,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.log.push(Entry::Set {
            key: Value::String(key),
            value: Value::String(value),
        })
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.log.get_value(&Value::String(key))? {
            Ok(Some(format!("{}", value)))
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let key_ = Value::String(key);
        if !self.log.contains_key(&key_) {
            Err(Error::NonExistentKey(key_))
        } else {
            self.log.push(Entry::Remove { key: key_ })
        }
    }
}
