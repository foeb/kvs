//! Methods for the log-structured storage.
use ron::de;
/// In a real key-value store, we'd probably want a fast, binary format with
/// some other nice properties. Since this is just a toy, we're instead using
/// RON to serialize, since it'll be easy to debug.
use ron::ser;
use serde::{self, Deserialize, Serialize};

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};

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
    // entries: Vec<LogEntry>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    reader_pos: u64,
    index: HashMap<String, u64>,
}

/// The default capacity of the index (to reduce allocations).
const DEFAULT_INDEX_CAPACITY: usize = 4000;

/// The size of the buffer used to read in the log.
const READ_BUFFER_SIZE: usize = 4000;

impl Drop for Log {
    fn drop(&mut self) {
        if let Err(e) = self.writer.flush() {
            error!("ERROR FLUSHING LOG TO DISK {:?}", e);
        }
    }
}

impl Log {
    /// Open the log contained in the given file.
    pub fn new(file: File) -> Result<Self> {
        let mut log = Log {
            reader: BufReader::new(file.try_clone()?),
            writer: BufWriter::new(file.try_clone()?),
            reader_pos: 0,
            index: HashMap::with_capacity(DEFAULT_INDEX_CAPACITY),
        };
        log.load()?;
        Ok(log)
    }

    /// Put the entry into the index with the given position in the log file.
    fn index_entry(&mut self, entry: LogEntry, pos: u64) {
        match entry {
            LogEntry::Set(key, _) => {
                self.index.insert(key, pos);
            }
            LogEntry::Rm(key) => {
                self.index.remove(&key);
            }
        }
    }

    /// Append a log entry to the end of the log.
    pub fn push(&mut self, entry: LogEntry) -> Result<()> {
        let entry_str = ser::to_string(&entry)?;
        let bytes = entry_str.as_bytes();
        let pos = self.writer.seek(SeekFrom::Current(0))?;
        self.writer.write_all(bytes)?;
        self.writer.write_all("\n".as_bytes())?;
        self.index_entry(entry, pos);
        Ok(())
    }

    /// Look up the given key in the log.
    pub fn get_value(&mut self, key: &Key) -> io::Result<Option<Value>> {
        if let Some(pos) = self.index.get(key) {
            let offset = self.reader.seek(SeekFrom::Start(*pos))?;
            self.reader_pos = offset;
            let mut buf: String = String::with_capacity(READ_BUFFER_SIZE);

            self.writer.flush()?; // make sure to flush the write buffer before trying to read.
            let len = self.reader.read_line(&mut buf)?;
            if len == 0 {
                return Ok(None);
            }

            if let Some(line) = buf.get(0..len) {
                if let LogEntry::Set(_, value) = de::from_str(line).expect("parse") {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    /// Load the log from disk.
    pub fn load(&mut self) -> Result<()> {
        let offset = self.reader.seek(SeekFrom::Start(0))?;
        self.reader_pos = offset;
        let mut buf: String = String::with_capacity(READ_BUFFER_SIZE);

        self.writer.flush()?; // make sure to flush the write buffer before trying to read.
        while let Ok(len) = self.reader.read_line(&mut buf) {
            if len == 0 {
                break;
            }

            if let Some(line) = buf.get(0..len) {
                self.index_entry(de::from_str(line)?, self.reader_pos);
            } else {
                break;
            }

            self.reader_pos += len as u64;
            buf.clear();
        }
        Ok(())
    }
}
