use crate::{Error, Result};
use logformat::{file, mem, LogReader, LogWriter};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

type Generation = u32;

pub struct KvStore {
    log_path: PathBuf,
    readers: HashMap<Generation, LogReader<BufReader<File>>>,
    writers: HashMap<Generation, LogWriter<BufWriter<File>>>,
    index: HashMap<mem::Key, u64>,
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        trace!("Setting {} <- {}", &key, &value);
        let entry = mem::Entry::Set {
            key: mem::Value::String(key),
            value: mem::Value::String(value),
        };
        let pos = self.push(&entry)?;
        self.index_entry(entry, pos);
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        trace!("Getting {}", &key);
        let key_ = mem::Value::String(key);
        let gen = 0;
        self.flush(gen)?;
        let reader = self
            .readers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        if let Some(pos) = self.index.get(&key_) {
            let entry = reader.entry_at(*pos)?;
            if let Some(mem::Entry::Set { value, .. }) = &entry {
                return Ok(Some(format!("{}", value)));
            } else {
                warn!(
                    "Did not find Entry::Set at {}, instead found {:?}",
                    *pos, &entry
                );
            }
        } else {
            warn!("Key not found in index: {:?}", &key_);
        }

        Ok(None)
    }

    /// Remove a key from the database.
    fn remove(&mut self, key: String) -> Result<()> {
        trace!("Removing {}", &key);
        let key_ = mem::Value::String(key);
        if !self.index.contains_key(&key_) {
            warn!("Trying to remove key not found in index: {}", &key_);
            Err(Error::NonExistentKey(key_))
        } else {
            let entry = mem::Entry::Remove { key: key_ };
            let pos = self.push(&entry)?;
            self.index_entry(entry, pos);
            Ok(())
        }
    }
}

/// The default capacity of the index (to reduce allocations).
const DEFAULT_INDEX_CAPACITY: usize = 4000;

impl Drop for KvStore {
    fn drop(&mut self) {
        self.compact(0).unwrap();
        for (_, writer) in self.writers.iter_mut() {
            if let Err(e) = writer.flush() {
                error!("ERROR FLUSHING LOG TO DISK {:?}", e);
            }
        }
    }
}

impl KvStore {
    /// Creates a `KvStore` by opening the given path as a log.
    pub fn open(path: &Path) -> Result<KvStore> {
        let log_path = if path.is_dir() {
            path.join("log")
        } else {
            path.to_owned()
        };

        trace!("Opening KvStore at {:?}", &log_path);

        let mut kvs = KvStore {
            log_path,
            readers: HashMap::new(),
            writers: HashMap::new(),
            index: HashMap::with_capacity(DEFAULT_INDEX_CAPACITY),
        };
        kvs.open_log_file(0)?;
        kvs.compact(0)?;
        kvs.load(0)?;
        Ok(kvs)
    }

    fn open_log_file(&mut self, gen: Generation) -> Result<()> {
        let log_path = self.log_path.with_extension(format!("{}", gen));
        let data_path = self.log_path.with_extension(format!("{}.data", gen));

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_path)?;

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&data_path)?;

        self.writers.insert(
            gen,
            LogWriter::new(
                BufWriter::new(log_file.try_clone()?),
                BufWriter::new(data_file.try_clone()?),
            )?,
        );

        self.readers.insert(
            gen,
            LogReader::new(
                BufReader::new(log_file.try_clone()?),
                BufReader::new(data_file.try_clone()?),
            )?,
        );

        Ok(())
    }

    /// Put the entry into the index with the given position in the log file.
    fn index_entry(&mut self, entry: mem::Entry, pos: u64) {
        trace!("Indexing entry {:?} at {}", &entry, pos);
        match entry {
            mem::Entry::Set { key, .. } => {
                self.index.insert(key, pos);
            }
            mem::Entry::Remove { key } => {
                self.index.remove(&key);
            }
        }
    }

    fn flush(&mut self, gen: Generation) -> Result<()> {
        let writer = self
            .writers
            .get_mut(&gen)
            .expect("Writer not found for generation");
        writer.flush()?;
        trace!("Flushed writer for gen {}", gen);
        Ok(())
    }

    /// Append a log entry to the end of the log.
    fn push(&mut self, entry: &mem::Entry) -> Result<u64> {
        trace!("Pushing entry {:?}", entry);
        let writer = self
            .writers
            .get_mut(&0)
            .expect("Writer not found for generation");
        Ok(writer.write_entry(entry)?)
    }

    /// Load the log from disk.
    fn load(&mut self, gen: Generation) -> Result<()> {
        trace!("Loading log from disk for gen {}", gen);
        self.flush(gen)?;
        let reader = self
            .readers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        reader.seek(0)?;

        let mut entries: Vec<(mem::Entry, u64)> = Vec::new();
        let mut pos = 0;
        while let Some(entry) = reader.read_entry()? {
            trace!("{}: {:?}", &pos, &entry);
            entries.push((entry, pos));
            pos += 1;
        }

        for (entry, pos) in entries {
            self.index_entry(entry, pos);
        }

        let writer = self
            .writers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        writer.set_pos(pos);

        Ok(())
    }

    fn compact(&mut self, gen: Generation) -> Result<()> {
        assert!(gen % 2 == 0);

        self.open_log_file(gen + 1)?;

        self.flush(gen)?;
        let reader = self
            .readers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        reader.seek(0)?;

        let writer = self
            .writers
            .get_mut(&(gen + 1))
            .expect("Writer not found for generation");

        trace!("Starting compaction");

        let mut entries_deleted = 0;
        let mut reader_pos = 0;
        while let Some(entry) = reader.read_file_entry()? {
            trace!("Read {:?}", &entry);
            match &entry {
                file::Entry::Set { key, value } => {
                    let mem_key = reader.lookup_file_value(&key)?;
                    if let Some(pos) = self.index.get(&mem_key) {
                        trace!(
                            "Found pos in index: {}, comparing to reader_pos {}",
                            pos,
                            reader_pos
                        );

                        if *pos == reader_pos {
                            let mem_value = reader.lookup_file_value(&value)?;
                            writer.write_entry(&mem::Entry::Set {
                                key: mem_key,
                                value: mem_value,
                            })?;
                        } else {
                            entries_deleted += 1;
                        }
                    }
                }
                file::Entry::Remove { key } => {
                    let mem_key = reader.lookup_file_value(&key)?;
                    if !self.index.contains_key(&mem_key) {
                        writer.write_entry(&mem::Entry::Remove { key: mem_key })?;
                    } else {
                        entries_deleted += 1;
                    }
                }
            }
            reader_pos += 1;
        }

        info!("Replacing {:?} with {:?}",
            self.log_path.with_extension(format!("{}", gen)),
            self.log_path.with_extension(format!("{}", gen + 1)),
        );

        std::fs::rename(
            self.log_path.with_extension(format!("{}", gen + 1)),
            self.log_path.with_extension(format!("{}", gen)),
        )?;

        std::fs::rename(
            self.log_path.with_extension(format!("{}.data", gen + 1)),
            self.log_path.with_extension(format!("{}.data", gen)),
        )?;

        info!(
            "Finished compaction: removed {} out of {} entries",
            entries_deleted,
            reader_pos
        );

        Ok(())
    }
}
