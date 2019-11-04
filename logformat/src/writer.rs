use crate::entry::{file, mem};
use crate::{Error, Result};
use bincode;
use std::convert::TryInto;
use std::io::{Seek, SeekFrom, Write};

pub struct LogWriter<F: Write + Seek> {
    entry_writer: F,
    entry_pos: u64,
    data_writer: F,
}

impl<F: Write + Seek> LogWriter<F> {
    pub fn new(entry_writer: F, data_writer: F) -> Result<Self> {
        let mut writer = LogWriter {
            entry_writer,
            entry_pos: 0,
            data_writer,
        };
        writer.from_beginning()?;
        Ok(writer)
    }

    fn from_beginning(&mut self) -> Result<()> {
        self.entry_writer.seek(SeekFrom::Start(0))?;
        self.data_writer.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.data_writer.flush()?;
        self.entry_writer.flush()?;
        Ok(())
    }

    fn write_value(&mut self, value: &mem::Value) -> Result<file::Value> {
        let out = match value {
            mem::Value::String(s) => {
                let start = self.data_writer.stream_position()?;
                let len = self.data_writer.write(s.as_bytes())?.try_into()?;
                file::Value::String { start, len }
            }
            mem::Value::Integer(i) => file::Value::Integer { value: *i },
        };

        Ok(out)
    }

    pub fn write_entry(&mut self, entry: &mem::Entry) -> Result<()> {
        if self.entry_pos >= file::MAX_ENTRIES_PER_FILE {
            return Err(Error::FileOutOfSpaceError());
        }

        let out: Option<file::Entry> = Some(match entry {
            mem::Entry::Set { key, value } => file::Entry::Set {
                key: self.write_value(key)?,
                value: self.write_value(value)?,
            },
            mem::Entry::Remove { key } => file::Entry::Remove {
                key: self.write_value(key)?,
            },
        });

        let bytes_written = self
            .entry_writer
            .write(bincode::serialize(&out)?.as_slice())?;
        self.entry_pos += 1;

        self.entry_writer.seek(SeekFrom::Current(
            file::SERIALIZED_ENTRY_SIZE as i64 - bytes_written as i64,
        ))?;

        Ok(())
    }
}
