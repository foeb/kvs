use crate::{Error, Result};
use logformat::{file, mem, LogReader, LogWriter};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub struct KvStore {
    log_path: PathBuf,
    log_len: HashMap<Generation, u64>,
    readers: HashMap<Generation, LogReader<BufReader<File>>>,
    writers: HashMap<Generation, LogWriter<BufWriter<File>>>,
    current_gen: Generation,
    index: HashMap<mem::Key, IndexEntry>,
}

type Generation = u32;

struct IndexEntry {
    pub gen: Generation,
    pub location: u64,
}

impl IndexEntry {
    pub fn new(gen: Generation, location: u64) -> Self {
        IndexEntry { gen, location }
    }
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
        let current_gen = self.current_gen;
        self.push(&current_gen, entry)?;
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        trace!("Getting {}", &key);
        let key_ = mem::Value::String(key);
        let current_gen = self.current_gen;
        self.flush(&current_gen)?;

        if let Some(IndexEntry { gen, location }) = self.index.get(&key_) {
            let reader = self
                .readers
                .get_mut(&gen)
                .expect("Reader not found for generation");

            let entry = reader.entry_at(*location)?;
            if let Some(mem::Entry::Set { value, .. }) = &entry {
                return Ok(Some(format!("{}", value)));
            } else {
                warn!(
                    "Did not find Entry::Set at ({}, {}), instead found {:?}",
                    *gen, *location, &entry
                );
            }
        } else {
            warn!("Key not found in index: {:?}", &key_);
        }

        Ok(None)
    }

    /// Remove a given key.
    fn remove(&mut self, key: String) -> Result<()> {
        trace!("Removing {}", &key);
        let key_ = mem::Value::String(key);
        if !self.index.contains_key(&key_) {
            warn!("Trying to remove key not found in index: {}", &key_);
            Err(Error::NonExistentKey(key_))
        } else {
            let entry = mem::Entry::Remove { key: key_ };
            let current_gen = self.current_gen;
            self.push(&current_gen, entry)?;
            Ok(())
        }
    }
}

/// The default capacity of the index (to reduce allocations).
const DEFAULT_INDEX_CAPACITY: usize = 4000;

impl Drop for KvStore {
    fn drop(&mut self) {
        for (_, writer) in self.writers.iter_mut() {
            if let Err(e) = writer.flush() {
                error!("ERROR FLUSHING LOG TO DISK {:?}", e);
            }
        }
    }
}

const COMPACT_THRESHOLD: u64 = 4000;

impl KvStore {
    /// Creates a `KvStore` by opening the given path as a log.
    pub fn open(path: &Path) -> Result<KvStore> {
        let log_path = path.to_owned();

        if !log_path.is_dir() {
            return Err(Error::Message("Path is not a directory".to_owned()));
        }

        trace!("Opening KvStore at {:?}", &log_path);

        let generations = fs::read_dir(&log_path)?
            .map(|e| {
                e.map(|x| {
                    x.file_name()
                        .as_os_str()
                        .to_str()
                        .expect("file name is utf-8")
                        .parse::<Generation>()
                })
            })
            .filter_map(|x| if let Ok(Ok(gen)) = x { Some(gen) } else { None });

        let mut kvs = KvStore {
            log_path,
            log_len: HashMap::new(),
            readers: HashMap::new(),
            writers: HashMap::new(),
            current_gen: generations.max().unwrap_or(0),
            index: HashMap::with_capacity(DEFAULT_INDEX_CAPACITY),
        };

        trace!("Current generation {}", &kvs.current_gen);

        for gen in 0..kvs.current_gen + 1 {
            kvs.open_log_file(gen)?;
            let len = kvs.load(gen)?;
            debug!("Loaded gen {} with {} entries", gen, len);
        }

        Ok(kvs)
    }

    fn get_log_path(&self, gen: Generation, is_data: bool, is_temp: bool) -> PathBuf {
        let temp_part = if is_temp { "-temp" } else { "" };
        let data_part = if is_data { ".data" } else { "" };
        let name = format!("{}{}{}", gen, temp_part, data_part);
        self.log_path.join(Path::new(name.as_str()))
    }

    fn open_temp_writer(&self, gen: Generation) -> Result<LogWriter<BufWriter<File>>> {
        let log_path = self.get_log_path(gen, false, true);
        let data_path = self.get_log_path(gen, true, true);

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

        let writer = LogWriter::new(
            BufWriter::new(log_file.try_clone()?),
            BufWriter::new(data_file.try_clone()?),
        )?;

        Ok(writer)
    }

    fn open_log_file(&mut self, gen: Generation) -> Result<()> {
        let log_path = self.get_log_path(gen, false, false);
        let data_path = self.get_log_path(gen, true, false);

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

        self.readers.insert(
            gen,
            LogReader::new(
                BufReader::new(log_file.try_clone()?),
                BufReader::new(data_file.try_clone()?),
            )?,
        );

        let old_writer = self.writers.insert(
            gen,
            LogWriter::new(
                BufWriter::new(log_file.try_clone()?),
                BufWriter::new(data_file.try_clone()?),
            )?,
        );

        if let Some(mut writer) = old_writer {
            writer.flush()?;
        }

        Ok(())
    }

    /// Put the entry into the index with the given position in the log file.
    fn index_entry(&mut self, entry: mem::Entry, gen: Generation, pos: u64) {
        debug!("Indexing entry {:?} at gen {}, {}", &entry, gen, pos);
        match entry {
            mem::Entry::Set { key, .. } => {
                self.index.insert(key, IndexEntry::new(gen, pos));
            }
            mem::Entry::Remove { key } => {
                self.index.remove(&key);
            }
        }
    }

    fn flush(&mut self, gen: &Generation) -> Result<()> {
        let writer = self
            .writers
            .get_mut(gen)
            .expect("Writer not found for generation");
        writer.flush()?;
        trace!("Flushed writer for gen {}", gen);
        Ok(())
    }

    /// Append a log entry to the end of the log.
    fn push(&mut self, gen: &Generation, entry: mem::Entry) -> Result<u64> {
        debug!("Pushing entry {:?}", entry);
        let writer = self
            .writers
            .get_mut(&gen)
            .expect("Writer not found for generation");

        let pos = writer.write_entry(&entry)?;
        self.index_entry(entry, *gen, pos);

        if pos > COMPACT_THRESHOLD {
            self.compact_all()?;
        }

        Ok(pos)
    }

    /// Load the log from disk.
    fn load(&mut self, gen: Generation) -> Result<u64> {
        trace!("Loading log from disk for gen {}", gen);
        self.flush(&gen)?;
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
            self.index_entry(entry, gen, pos);
        }

        self.log_len.insert(gen, pos);

        let writer = self
            .writers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        writer.set_pos(pos);

        Ok(pos)
    }

    fn compact_all(&mut self) -> Result<()> {
        let mut new_len = 0;
        for gen in 0..self.current_gen + 1 {
            new_len = self.compact(gen)?;
        }

        if new_len > COMPACT_THRESHOLD {
            self.current_gen += 1;
            self.open_log_file(self.current_gen)?;
        };

        Ok(())
    }

    fn compact(&mut self, gen: Generation) -> Result<u64> {
        let mut writer = self.open_temp_writer(gen)?;

        self.flush(&gen)?;
        let reader = self
            .readers
            .get_mut(&gen)
            .expect("Reader not found for generation");
        reader.seek(0)?;

        trace!("Starting compaction");

        let mut entries_deleted = 0;
        let mut reader_pos = 0;
        while let Some(entry) = reader.read_file_entry()? {
            trace!("Read {:?}", &entry);
            match &entry {
                file::Entry::Set { key, value } => {
                    let mem_key = reader.lookup_file_value(&key)?;
                    if let Some(index_entry) = self.index.get(&mem_key) {
                        trace!(
                            "Found location in index for gen {}: {}, comparing to reader_pos {} in gen {}",
                            index_entry.gen,
                            index_entry.location,
                            reader_pos,
                            gen,
                        );

                        if index_entry.gen == gen && index_entry.location == reader_pos {
                            let mem_value = reader.lookup_file_value(&value)?;
                            writer.write_entry(&mem::Entry::Set {
                                key: mem_key,
                                value: mem_value,
                            })?;
                        } else {
                            debug!("Deleting Entry::Set at gen {}, {}", gen, reader_pos);
                            entries_deleted += 1;
                        }
                    }
                }
                file::Entry::Remove { key } => {
                    let mem_key = reader.lookup_file_value(&key)?;
                    if !self.index.contains_key(&mem_key) {
                        writer.write_entry(&mem::Entry::Remove { key: mem_key })?;
                    } else {
                        debug!("Deleting Entry::Remove at gen {}, {}", gen, reader_pos);
                        entries_deleted += 1;
                    }
                }
            }
            reader_pos += 1;
        }

        writer.flush()?;

        std::fs::rename(
            self.get_log_path(gen, false, true),
            self.get_log_path(gen, false, false),
        )?;

        std::fs::rename(
            self.get_log_path(gen, true, true),
            self.get_log_path(gen, true, false),
        )?;

        self.open_log_file(gen)?;

        info!(
            "Finished compaction: removed {} out of {} entries",
            entries_deleted, reader_pos
        );

        Ok(reader_pos)
    }
}
