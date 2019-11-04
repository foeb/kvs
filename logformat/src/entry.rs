/// Data types for serialized values in the log and data file.
pub mod file {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Value {
        String { start: u64, len: u64 },
        Integer { value: i128 },
    }

    /// Right now, keys can just be values.
    pub type Key = Value;

    /// Contains a single command registered by the key-value store.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum Entry {
        /// Represents a command to set the key to the given value.
        Set { key: Key, value: Value },

        /// Represents a command to delete the value associated with the given key.
        Remove { key: Key },
    }

    /// There's probably a better way to do it, but here we decide on a fixed size
    /// for our log entries.
    pub const SERIALIZED_ENTRY_SIZE: usize = 32;

    /// Here we pick an arbitrary size for when we should move onto another generation
    /// of log files.
    pub const MAX_ENTRIES_PER_FILE: u64 = 0x1000;

    #[cfg(test)]
    mod tests {
        use super::*;
        use bincode;

        #[test]
        fn check_serialized_entry_size() {
            let entry = Some(Entry::Set {
                key: Value::Integer { value: 0 },
                value: Value::Integer { value: 0 },
            });
            assert!(bincode::serialized_size(&entry).unwrap() <= SERIALIZED_ENTRY_SIZE as u64);
        }
    }
}

/// Datatypes for values deserialized from the log or data file.
pub mod mem {
    #[derive(Debug, PartialEq)]
    pub enum Value {
        String(String),
        Integer(i128),
    }

    pub type Key = Value;

    #[derive(Debug, PartialEq)]
    pub enum Entry {
        Set { key: Key, value: Value },
        Remove { key: Key },
    }
}
