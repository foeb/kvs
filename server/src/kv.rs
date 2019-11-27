use bincode;
use kvs::{self, Error, Result};
use logformat::index::Index;
use logformat::page::{Page, PageBody, PageBuffer, PageHeader, BUF_SIZE, COMMANDS_PER_PAGE};
use logformat::slotted::Slotted;
use metrohash::MetroHash64;
use sled::Db;
use slog::Logger;
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use uuid::{v1, Uuid};

pub struct SledEngine {
    pub db: Db,
}

impl Drop for SledEngine {
    fn drop(&mut self) {}
}

impl kvs::Engine for SledEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let result = self
            .db
            .get(key)
            .map(|x| x.map(|y| String::from_utf8_lossy(&y).into_owned()))?;
        self.db.flush()?;
        Ok(result)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let result = if let None = self.db.remove(key)? {
            Err(Error::KeyNotFound)
        } else {
            Ok(())
        };
        self.db.flush()?;
        result
    }
}

pub struct KvStore {
    log_path: PathBuf,
    index: Index,
    page_readers: HashMap<Uuid, BufReader<File>>,
    data_readers: HashMap<Uuid, BufReader<File>>,
    in_memory: BTreeMap<InMemoryKey, Option<String>>,
    page_buffer: PageBuffer,
    node_id: [u8; 6],
    context: v1::Context,
    slog: Logger,
}

/// Holds the key with its hash, ordered by the hash.
#[derive(Eq, PartialEq)]
pub struct InMemoryKey {
    pub hash: u64,
    pub key: String,
}

impl InMemoryKey {
    pub fn new(key: String) -> Self {
        let mut hasher = MetroHash64::with_seed(METROHASH_SEED);
        key.hash(&mut hasher);
        let hash = hasher.finish();
        InMemoryKey { key, hash }
    }
}

impl Ord for InMemoryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialOrd for InMemoryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const METROHASH_SEED: u64 = 0x385f_829f_0031_3111;

impl kvs::Engine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> kvs::Result<()> {
        if let Err(e) = self.push(key, Some(value)) {
            Err(kvs::Error::Message(format!("{}", e)))
        } else {
            Ok(())
        }
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> kvs::Result<Option<String>> {
        trace!(self.slog, "Getting {}", &key);
        let key_with_hash = InMemoryKey::new(key);
        if let Some(maybe_value) = self.in_memory.get(&key_with_hash) {
            if let Some(value) = maybe_value {
                trace!(self.slog, "Found {} in memory", value);
                return Ok(Some(value.to_string()));
            } else {
                trace!(self.slog, "Found None in memory");
                return Ok(None);
            }
        }

        let key_hash = key_with_hash.hash;
        let len = self.index.len();
        for i in 0..len {
            let header = self.index.get(len - i - 1).unwrap();
            let uuid = header.uuid;
            if header.min_key_hash <= key_hash && key_hash <= header.max_key_hash {
                let page = self.read_page(&uuid);
                if let Err(e) = page {
                    return Err(kvs::Error::Message(format!("{}", e)));
                }
                let page = page.unwrap();

                trace!(self.slog, "Reading page {:?}", &page.header);
                for (index, hash) in page.body.key_hash[..].iter().enumerate() {
                    // FIXME: use binary search
                    if hash != &key_hash {
                        continue;
                    }

                    let value_index = page.body.value_index[index];
                    if value_index < 0 {
                        return Ok(None);
                    }

                    let data = self.read_data(&uuid);
                    if let Err(e) = data {
                        return Err(kvs::Error::Message(format!("{}", e)));
                    }
                    let mut data = data.unwrap();
                    let bytes = data.get(value_index as usize).expect("bad index");
                    let value = String::from_utf8_lossy(bytes).into_owned();
                    trace!(self.slog, "Found {} on disk", value);
                    return Ok(Some(value));
                }
            }
        }

        trace!(self.slog, "Key not found");
        Ok(None)
    }

    /// Remove a given key.
    fn remove(&mut self, key: String) -> kvs::Result<()> {
        if let Ok(Some(_)) = self.get(key.clone()) {
            if let Err(e) = self.push(key, None) {
                Err(kvs::Error::Message(format!("{}", e)))
            } else {
                Ok(())
            }
        } else {
            Err(kvs::Error::KeyNotFound)
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.save().unwrap();
    }
}

impl KvStore {
    pub fn open(path: &Path) -> Result<KvStore> {
        let logger = kvs::get_default_logger();
        KvStore::open_with_logger(path, &logger)
    }

    /// Creates a `KvStore` by opening all of the log files in the given path.
    pub fn open_with_logger(path: &Path, logger: &Logger) -> Result<KvStore> {
        let log_path = path.to_owned();

        let slog = logger.new(o!("path" => format!("{:?}", &log_path)));

        if !log_path.is_dir() {
            return Err(Error::Message("Path is not a directory".to_owned()));
        }

        let mut kvs = KvStore {
            slog,
            log_path,
            page_readers: HashMap::new(),
            data_readers: HashMap::new(),
            index: Index::default(),
            in_memory: BTreeMap::default(),
            page_buffer: PageBuffer { buf: [0; BUF_SIZE] },
            node_id: [b'g', b'o', b'o', b'd', b'!', b'!'],
            context: v1::Context::new(0),
        };

        kvs.read_index()?;

        Ok(kvs)
    }

    pub fn save(&mut self) -> Result<()> {
        if !self.in_memory.is_empty() {
            self.write_page()?;
            self.write_index()?;
        }
        Ok(())
    }

    /// Write the index to the index file, truncating the previous one.
    // FIXME: this could cause us to lose all of the data
    fn write_index(&self) -> Result<()> {
        let path = self.log_path.join(Index::path());
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        trace!(self.slog, "Writing {:?}", &self.index);
        bincode::serialize_into(file, &self.index)?;
        Ok(())
    }

    /// Read the index from the index file.
    fn read_index(&mut self) -> Result<()> {
        let path = self.log_path.join(Index::path());
        trace!(self.slog, "Reading index at {:?}", &path);
        match OpenOptions::new().read(true).open(path) {
            Ok(file) => {
                trace!(self.slog, "Deserializing index");
                self.index = bincode::deserialize_from(file)?;
                trace!(self.slog, "Index has {:?} entries", self.index.len());
                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    trace!(self.slog, "Index not found");
                    self.index = Index::default();
                    Ok(())
                }
                _ => Err(Error::IoError(e)),
            },
        }
    }

    /// Take the in-memory store, and write it out as a page in order of key-hash, along with
    /// the data file.
    fn write_page(&mut self) -> Result<()> {
        let mut min = std::u64::MAX;
        let mut max = std::u64::MIN;
        let mut body = PageBody::default();
        let mut data = Slotted::new();

        let mut i = 0;
        for (key, value) in self.in_memory.iter() {
            if i >= COMMANDS_PER_PAGE {
                panic!("Writing page with more than COMMANDS_PER_PAGE commands");
            }

            min = cmp::min(min, key.hash);
            max = cmp::max(max, key.hash);
            let value_index = value.as_ref().map(|s| data.push(s.as_bytes()) as i16);
            body.key_hash[i] = key.hash;
            body.value_index[i] = value_index.unwrap_or(-1);

            i += 1;
        }

        let header = PageHeader::new(&self.node_id, &self.context, min, max, i as u16)?;
        self.index.push(header.clone());
        let page = Page { body, header };
        trace!(self.slog, "{}", &page.body.key_hash[0]);

        let page_path = self.log_path.join(Page::path(&page.header.uuid));
        let mut page_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(page_path)?;
        self.page_buffer.serialize(&page);
        self.page_buffer.write_to(&mut page_file)?;

        let data_path = self.log_path.join(Slotted::path(&page.header.uuid));
        let data_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(data_path)?;
        bincode::serialize_into(data_file, &data)?;

        info!(self.slog, "Wrote {} commands to disk", i);

        Ok(())
    }

    /// Read the page with the UUID from disk.
    fn read_page(&mut self, uuid: &Uuid) -> Result<Page> {
        if !self.page_readers.contains_key(&uuid) {
            let path = self.log_path.join(Page::path(uuid));
            let file = OpenOptions::new().read(true).open(path)?;
            self.page_readers.insert(*uuid, BufReader::new(file));
        }

        if let Some(reader) = self.page_readers.get_mut(uuid) {
            reader.seek(SeekFrom::Start(0))?;
            let mut page = Page::default();
            self.page_buffer.read_from(reader)?;
            self.page_buffer.deserialize(&mut page)?;
            Ok(page)
        } else {
            panic!("Error retrieving cached reader")
        }
    }

    /// Read the data file with the UUID from disk.
    fn read_data(&mut self, uuid: &Uuid) -> Result<Slotted> {
        if !self.data_readers.contains_key(&uuid) {
            let path = self.log_path.join(Slotted::path(uuid));
            let file = OpenOptions::new().read(true).open(path)?;
            self.data_readers.insert(*uuid, BufReader::new(file));
        }

        if let Some(reader) = self.data_readers.get_mut(uuid) {
            reader.seek(SeekFrom::Start(0))?;
            let data = bincode::deserialize_from(reader)?;
            Ok(data)
        } else {
            panic!("Error retrieving cached reader")
        }
    }

    /// Append a log entry to the end of the log.
    fn push(&mut self, key: String, value: Option<String>) -> Result<()> {
        trace!(self.slog, "Pushing ({:?}, {:?})", &key, &value);
        self.in_memory.insert(InMemoryKey::new(key), value);
        if self.in_memory.len() >= COMMANDS_PER_PAGE {
            self.write_page()?;
            self.in_memory = BTreeMap::new();
        }
        self.save().unwrap();
        Ok(())
    }
}
