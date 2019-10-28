//! Methods for the log-structured storage.
use serde::{Deserialize, Serialize};

/// The type for the keys of our key-value store.
pub type Key = String;

/// The type for the values of our key-value store.
pub type Value = String;

/// Contains a single command registered by the key-value store.
#[derive(Serialize, Deserialize)]
pub enum LogEntry {
    /// Represents a command to set the key to the given value.
    Set(Key, Value),

    /// Represents a command to delete the value associated with the given key.
    Rm(Key),
}

/// Our WAL, to be stored in a file.
#[derive(Serialize, Deserialize, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    /// Append a log entry to the end of the log.
    pub fn push(&mut self, entry: LogEntry) {
        self.entries.push(entry)
    }

    /// Get the entry at the given index.
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.entries.get(index)
    }

    /// Look up the given key in the log.
    pub fn get_value(&self, key: Key) -> Option<Value> {
        for i in self.entries.len()..0 {
            let entry = unsafe { self.entries.get_unchecked(i) };
            match entry {
                LogEntry::Set(key_, value) => {
                    if key.eq(key_) {
                        return Some(value.clone());
                    }
                }
                LogEntry::Rm(key_) => {
                    if key.eq(key_) {
                        return None;
                    }
                }
            }
        }

        None
    }
}