use crate::entry::{file, mem};
use crate::{Error, Result};
use bincode;
use std::convert::TryInto;
use std::io::{Seek, SeekFrom, Write};

#[derive(Debug)]
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
        writer.seek(0)?;
        Ok(writer)
    }

    pub fn seek(&mut self, pos: u64) -> Result<()> {
        let actual_pos = self
            .entry_writer
            .seek(SeekFrom::Start(pos * file::SERIALIZED_ENTRY_SIZE as u64))?;
        self.entry_pos = pos;

        if actual_pos != pos * file::SERIALIZED_ENTRY_SIZE as u64 {
            return Err(Error::SeekError());
        }

        Ok(())
    }

    pub fn get_pos(&self) -> u64 {
        trace!("Current pos: {}", self.entry_pos);
        self.entry_pos
    }

    pub fn set_pos(&mut self, pos: u64) {
        trace!("Setting pos to {}", pos);
        self.entry_pos = pos;
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

    pub fn write_file_entry(&mut self, entry: &file::Entry) -> Result<u64> {
        let mut bytes = bincode::serialize(&Some(entry))?;
        // Pad the output to the max size of an entry
        bytes.resize(file::SERIALIZED_ENTRY_SIZE, 0);

        self.entry_writer.write_all(bytes.as_slice())?;
        let pos = self.get_pos();
        self.set_pos(pos + 1);

        Ok(pos)
    }

    pub fn write_entry(&mut self, entry: &mem::Entry) -> Result<u64> {
        let out: file::Entry = match entry {
            mem::Entry::Set { key, value } => file::Entry::Set {
                key: self.write_value(key)?,
                value: self.write_value(value)?,
            },
            mem::Entry::Remove { key } => file::Entry::Remove {
                key: self.write_value(key)?,
            },
        };

        self.write_file_entry(&out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn entry_pos_remains_updated() {
        let mut entry_buf = vec![0u8; 500];
        let mut data_buf = vec![0u8; 500];

        let entry_cursor = Cursor::new(&mut entry_buf);
        let data_cursor = Cursor::new(&mut data_buf);

        let mut writer = LogWriter::new(entry_cursor, data_cursor).expect("writer");
        assert_eq!(0, writer.get_pos());

        let entry = mem::Entry::Set {
            key: mem::Value::Integer(42),
            value: mem::Value::String("hey there".to_owned()),
        };

        for i in 0..10 {
            let pos = writer.write_entry(&entry);
            assert!(pos.is_ok());
            assert_eq!(i, pos.unwrap());
        }
    }
}
