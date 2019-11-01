//! Methods for the log-structured storage.
use serde::{Deserialize, Serialize};

use ron::de;
/// In a real key-value store, we'd probably want a fast, binary format with
/// some other nice properties. Since this is just a toy, we're instead using
/// RON to serialize, since it'll be easy to debug.
use ron::ser;

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use crate::Result;

/// The type for the keys of our key-value store.
pub type Key = String;

/// The type for the values of our key-value store.
pub type Value = String;

/// Contains a single command registered by the key-value store.
#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    /// Represents a command to set the key to the given value.
    Set(Key, Value),

    /// Represents a command to delete the value associated with the given key.
    Rm(Key),
}

/// Our WAL, to be stored in a file.
pub struct Log {
    entries: Vec<LogEntry>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

/// The default size of the entries vector (to reduce allocations).
const DEFAULT_LOG_CAPACITY: usize = 4000;

/// The size of the buffer used to read in the log.
const READ_BUFFER_SIZE: usize = 4000;

impl Drop for Log {
    fn drop(&mut self) {
        if let Err(e) = self.writer.flush() {
            println!("ERROR FLUSHING LOG TO DISK");
            println!("{:?}", e);
        }
    }
}

impl Log {
    /// Open the log contained in the given file.
    pub fn new(file: File) -> Result<Self> {
        let mut log = Log {
            entries: vec![],
            reader: BufReader::new(file.try_clone()?),
            writer: BufWriter::new(file.try_clone()?),
        };
        log.load()?;
        Ok(log)
    }

    /// Append a log entry to the end of the log.
    pub fn push(&mut self, entry: LogEntry) -> Result<()> {
        self.writer.write_all(ser::to_string(&entry)?.as_bytes())?;
        self.writer.write_all("\n".as_bytes())?;
        self.entries.push(entry);
        Ok(())
    }

    /// Look up the given key in the log.
    pub fn get_value(&self, key: Key) -> Option<Value> {
        for i in (0..self.entries.len()).rev() {
            let entry = self.entries.get(i).unwrap();
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

    /// Load the log from disk.
    pub fn load(&mut self) -> Result<()> {
        let mut entries = Vec::<LogEntry>::new();
        entries.reserve(DEFAULT_LOG_CAPACITY);
        let mut buf: String = String::with_capacity(READ_BUFFER_SIZE);
        while let Ok(len) = self.reader.read_line(&mut buf) {
            if len == 0 {
                break;
            }

            if let Some(line) = buf.get(0..len) {
                entries.push(de::from_str(line)?);
            } else {
                break;
            }

            buf.clear();
        }
        self.entries = entries;
        Ok(())
    }
}
