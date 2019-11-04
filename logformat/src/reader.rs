use crate::entry::{file, mem};
use crate::{Error, Result};
use bincode;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub struct LogReader<F: Read + Seek> {
    entry_reader: F,
    entry_pos: u64,
    entry_buf: Vec<u8>,
    data_reader: F,
}

impl<F: Read + Seek> LogReader<F> {
    pub fn new(entry_reader: F, data_reader: F) -> Result<Self> {
        let mut reader = LogReader {
            entry_reader,
            entry_pos: 0,
            entry_buf: vec![0; file::SERIALIZED_ENTRY_SIZE],
            data_reader,
        };
        reader.seek(0)?;
        Ok(reader)
    }

    fn fill_buf(&mut self) -> Result<()> {
        assert!(self.entry_reader.stream_position()? % file::SERIALIZED_ENTRY_SIZE as u64 == 0);
        if let Err(e) = self.entry_reader.read_exact(&mut self.entry_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Err(Error::UnexpectedEof)
            } else {
                Err(Error::IoError(e))
            }
        } else {
            self.entry_pos += 1;
            Ok(())
        }
    }

    pub fn seek(&mut self, pos: u64) -> Result<()> {
        let actual_pos = self
            .entry_reader
            .seek(SeekFrom::Start(pos * file::SERIALIZED_ENTRY_SIZE as u64))?;
        self.entry_pos = pos;

        if actual_pos != pos * file::SERIALIZED_ENTRY_SIZE as u64 {
            return Err(Error::SeekError());
        }

        Ok(())
    }

    fn lookup_value(&mut self, value: &file::Value) -> Result<mem::Value> {
        let out = match value {
            file::Value::String { start, len } => {
                self.data_reader.seek(SeekFrom::Start(*start))?;
                let mut buf = vec![0u8; *len as usize];
                self.data_reader.read_exact(buf.as_mut())?;
                mem::Value::String(String::from_utf8(buf)?)
            }
            file::Value::Integer { value: i } => mem::Value::Integer(*i),
        };

        Ok(out)
    }

    fn lookup_entry(&mut self, entry: &file::Entry) -> Result<mem::Entry> {
        let out = match entry {
            file::Entry::Set { key, value } => mem::Entry::Set {
                key: self.lookup_value(key)?,
                value: self.lookup_value(value)?,
            },
            file::Entry::Remove { key } => mem::Entry::Remove {
                key: self.lookup_value(key)?,
            },
        };

        Ok(out)
    }

    pub fn read_entry(&mut self) -> Result<Option<mem::Entry>> {
        if let Err(e) = self.fill_buf() {
            return match e {
                Error::UnexpectedEof => Ok(None),
                _ => Err(e),
            };
        }

        if let Some(entry) = bincode::deserialize::<Option<file::Entry>>(&self.entry_buf)? {
            Ok(Some(self.lookup_entry(&entry)?))
        } else {
            Ok(None)
        }
    }

    pub fn entry_at(&mut self, pos: u64) -> Result<Option<mem::Entry>> {
        if pos > self.entry_pos {
            return Ok(None);
        }

        self.seek(pos)?;
        self.read_entry()
    }
}
