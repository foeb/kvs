//! Methods for the log-structured storage.
use crate::Result;
use logformat::{mem::Entry, mem::Key, mem::Value, LogReader, LogWriter};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};

/// Our WAL, to be stored in a file.
#[derive(Debug)]
pub struct Log {
    reader: LogReader<BufReader<File>>,
    writer: LogWriter<BufWriter<File>>,
    writer_pos: u64,
    index: HashMap<Key, u64>,
}

/// The default capacity of the index (to reduce allocations).
const DEFAULT_INDEX_CAPACITY: usize = 4000;

impl Drop for Log {
    fn drop(&mut self) {
        if let Err(e) = self.writer.flush() {
            error!("ERROR FLUSHING LOG TO DISK {:?}", e);
        }
    }
}

impl Log {
    /// Open the log contained in the given file.
    pub fn new(log: File, data: File) -> Result<Self> {
        let mut log = Log {
            reader: LogReader::new(
                BufReader::new(log.try_clone()?),
                BufReader::new(data.try_clone()?),
            )?,
            writer: LogWriter::new(
                BufWriter::new(log.try_clone()?),
                BufWriter::new(data.try_clone()?),
            )?,
            writer_pos: 0,
            index: HashMap::with_capacity(DEFAULT_INDEX_CAPACITY),
        };
        log.load()?;
        Ok(log)
    }

    /// Put the entry into the index with the given position in the log file.
    fn index_entry(&mut self, entry: Entry) {
        match entry {
            Entry::Set { key, .. } => {
                self.index.insert(key, self.writer_pos);
            }
            Entry::Remove { key } => {
                self.index.remove(&key);
            }
        }

        self.writer_pos += 1;
    }

    pub fn contains_key(&mut self, key: &Key) -> bool {
        self.index.contains_key(key)
    }

    /// Append a log entry to the end of the log.
    pub fn push(&mut self, entry: Entry) -> Result<()> {
        self.writer.write_entry(&entry)?;
        self.index_entry(entry);
        Ok(())
    }

    /// Look up the given key in the log.
    pub fn get_value(&mut self, key: &Key) -> Result<Option<Value>> {
        self.writer.flush()?; // make sure to flush the write buffer before trying to read.
        if let Some(pos) = self.index.get(key) {
            if let Some(Entry::Set { value, .. }) = self.reader.entry_at(*pos)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Load the log from disk.
    pub fn load(&mut self) -> Result<()> {
        self.reader.seek(0)?;
        self.writer.flush()?; // make sure to flush the write buffer before trying to read.
        while let Some(entry) = self.reader.read_entry()? {
            self.index_entry(entry);
        }
        Ok(())
    }
}
