use crate::{Error, Result};
use bincode;
use logformat::index::Index;
use logformat::page::{Page, PageBody, PageBuffer, PageHeader, BUF_SIZE, COMMANDS_PER_PAGE};
use logformat::slotted::Slotted;
use metrohash::MetroHash64;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use uuid::{v1, Uuid};

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub struct KvStore {
    log_path: PathBuf,
    index: Index,
    page_readers: HashMap<Uuid, BufReader<File>>,
    data_readers: HashMap<Uuid, BufReader<File>>,
    in_memory: BTreeMap<String, Option<String>>,
    page_buffer: PageBuffer,
    node_id: [u8; 6],
    context: v1::Context,
}

const METROHASH_SEED: u64 = 0x385f_829f_0031_3111;

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.push(key, Some(value))?;
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        trace!("Getting {}", &key);
        if let Some(maybe_value) = self.in_memory.get(&key) {
            if let Some(value) = maybe_value {
                debug!("Found {} in memory", value);
                return Ok(Some(value.to_string()));
            } else {
                debug!("Found None in memory");
                return Ok(None);
            }
        }

        let mut hasher = MetroHash64::with_seed(METROHASH_SEED);
        key.hash(&mut hasher);
        let key_hash = hasher.finish();

        let len = self.index.len();
        for i in 0..len {
            let header = self.index.get(len - i - 1).unwrap();
            let uuid = header.uuid;
            if header.min_key_hash <= key_hash && key_hash <= header.max_key_hash {
                let page = self.read_page(&uuid)?;
                trace!("Reading page {:?}", &page.header);

                trace!("{}", &page.body.key_hash[0]);
                for (index, hash) in page.body.key_hash[..].iter().enumerate() {
                    // FIXME: use binary search
                    if hash != &key_hash {
                        continue;
                    }

                    let value_index = page.body.value_index[index];
                    if value_index < 0 {
                        return Ok(None);
                    }

                    let mut data = self.read_data(&uuid)?;
                    let bytes = data.get(value_index as usize).expect("bad index");
                    let value = String::from_utf8_lossy(bytes).into_owned();
                    debug!("Found {} on disk", value);
                    return Ok(Some(value));
                }
            }
        }

        debug!("Key not found");
        Ok(None)
    }

    /// Remove a given key.
    fn remove(&mut self, key: String) -> Result<()> {
        self.push(key, None)?;
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if !self.in_memory.is_empty() {
            self.write_page().unwrap();
            self.write_index().unwrap();
        }
    }
}

impl KvStore {
    /// Creates a `KvStore` by opening the given path as a log.
    pub fn open(path: &Path) -> Result<KvStore> {
        let log_path = path.to_owned();

        if !log_path.is_dir() {
            return Err(Error::Message("Path is not a directory".to_owned()));
        }

        trace!("Opening KvStore at {:?}", &log_path);

        let mut kvs = KvStore {
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

    fn write_index(&self) -> Result<()> {
        let path = self.log_path.join(Index::path());
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        trace!("Writing {:?}", &self.index);
        bincode::serialize_into(file, &self.index)?;
        Ok(())
    }

    fn read_index(&mut self) -> Result<()> {
        let path = self.log_path.join(Index::path());
        trace!("Reading index at {:?}", &path);
        match OpenOptions::new().read(true).open(path) {
            Ok(file) => {
                trace!("Deserializing index");
                self.index = bincode::deserialize_from(file)?;
                trace!("Index has {:?} entries", self.index.len());
                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    trace!("Index not found");
                    self.index = Index::default();
                    Ok(())
                }
                _ => Err(Error::IoError(e)),
            },
        }
    }

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

            let mut hasher = MetroHash64::with_seed(METROHASH_SEED);
            key.hash(&mut hasher);
            let key_hash = hasher.finish();

            min = cmp::min(min, key_hash);
            max = cmp::max(max, key_hash);
            let value_index = value.as_ref().map(|s| data.push(s.as_bytes()) as i16);
            body.key_hash[i] = key_hash;
            body.value_index[i] = value_index.unwrap_or(-1);

            i += 1;
        }

        let header = PageHeader::new(&self.node_id, &self.context, min, max, i as u16)?;
        self.index.push(header.clone());
        let page = Page { body, header };
        trace!("{}", &page.body.key_hash[0]);

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

        info!("Wrote {} commands to disk", i);

        Ok(())
    }

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
        debug!("Pushing ({:?}, {:?})", &key, &value);
        self.in_memory.insert(key, value);
        if self.in_memory.len() >= COMMANDS_PER_PAGE {
            self.write_page()?;
            self.in_memory = BTreeMap::new();
        }
        Ok(())
    }
}
