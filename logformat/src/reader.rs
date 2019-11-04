use crate::entry::{file, mem};
use crate::{Error, Result};
use bincode;
use std::io::{BufRead, Read, Seek, SeekFrom};

pub struct LogReader<F: BufRead + Read + Seek> {
    entry_reader: F,
    entry_pos: u64,
    data_reader: F,
}

impl<F: BufRead + Read + Seek> LogReader<F> {
    pub fn new(entry_reader: F, data_reader: F) -> Result<Self> {
        let mut reader = LogReader {
            entry_reader,
            entry_pos: 0,
            data_reader,
        };
        reader.from_most_recent()?;
        Ok(reader)
    }

    fn fill_buf(&mut self) -> Result<&[u8]> {
        assert!(self.entry_reader.stream_position()? % file::SERIALIZED_ENTRY_SIZE as u64 == 0);
        let buf = self.entry_reader.fill_buf()?;
        if buf.len() < file::SERIALIZED_ENTRY_SIZE {
            return Err(Error::BufferFillError());
        }
        Ok(buf)
    }

    fn consume(&mut self) -> Result<()> {
        if self.entry_pos >= file::MAX_ENTRIES_PER_FILE - 1 {
            return Err(Error::SeekError());
        }

        self.entry_reader.consume(file::SERIALIZED_ENTRY_SIZE);
        self.entry_pos += 1;

        Ok(())
    }

    fn seek_prev(&mut self) -> Result<()> {
        if self.entry_pos == 0 {
            return Err(Error::SeekError());
        }

        let offset = self
            .entry_reader
            .seek(SeekFrom::Current(-(file::SERIALIZED_ENTRY_SIZE as i64)))?;
        self.entry_pos -= 1;

        if offset % file::SERIALIZED_ENTRY_SIZE as u64 != 0 {
            return Err(Error::SeekError());
        }

        Ok(())
    }

    fn from_most_recent(&mut self) -> Result<()> {
        let mut buf = self.fill_buf()?;
        while bincode::deserialize::<Option<file::Entry>>(&buf)?.is_some() {
            self.consume()?;
            buf = self.fill_buf()?;
        }
        if self.entry_pos > 0 {
            self.seek_prev()?;
        }
        Ok(())
    }

    pub fn last_entry(&mut self) -> Result<Option<file::Entry>> {
        self.from_most_recent()?;
        let buf = self.fill_buf()?;
        let result = bincode::deserialize(&buf)?;
        self.consume()?;
        Ok(result)
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

    pub fn lookup_entry(&mut self, entry: &file::Entry) -> Result<mem::Entry> {
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
}
