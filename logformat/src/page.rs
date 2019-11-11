use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::v1::{ClockSequence, Timestamp};
use uuid::Uuid;

#[derive(Default)]
pub struct Page {
    pub header: PageHeader,
    pub body: PageBody,
}

impl Page {
    pub fn path(uuid: &Uuid) -> PathBuf {
        Path::new(format!("{}.log", uuid.to_hyphenated_ref()).as_str()).to_owned()
    }
}

pub const MAGIC: u64 = 0x7873_6769;

pub const RESERVE_BYTES_FOR_HEADER: usize = 384;

/// Each entry is 10 bytes (a u64 + u16)
pub const COMMANDS_PER_PAGE: usize = 1600;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub uuid: Uuid,
    pub ticks: u64,
    pub min_key_hash: u64,
    pub max_key_hash: u64,
    pub count: u16,
}

impl Default for PageHeader {
    fn default() -> Self {
        PageHeader {
            uuid: Uuid::default(),
            ticks: 0,
            min_key_hash: 0,
            max_key_hash: 0,
            count: 0,
        }
    }
}

impl PageHeader {
    pub fn new(
        node_id: &[u8],
        context: &impl ClockSequence,
        min_key_hash: u64,
        max_key_hash: u64,
        count: u16,
    ) -> Result<Self> {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH)?;
        let timestamp =
            Timestamp::from_unix(context, since_epoch.as_secs(), since_epoch.subsec_nanos());
        let uuid = Uuid::new_v1(timestamp, node_id)?;
        Ok(PageHeader {
            uuid,
            ticks: timestamp.to_rfc4122().0,
            min_key_hash,
            max_key_hash,
            count,
        })
    }

    pub fn is_partial(&self) -> bool {
        self.count != COMMANDS_PER_PAGE as u16
    }
}

pub struct PageBody {
    pub key_hash: [u64; COMMANDS_PER_PAGE],
    pub value_index: [i16; COMMANDS_PER_PAGE],
}

impl Default for PageBody {
    fn default() -> Self {
        PageBody {
            key_hash: [0; COMMANDS_PER_PAGE],
            value_index: [0; COMMANDS_PER_PAGE],
        }
    }
}

/// Each page is 16KiB.
pub const BUF_SIZE: usize = 16384;

/// PageBuffer is used to quickly read and write pages to disk in one go.
pub struct PageBuffer {
    pub buf: [u8; BUF_SIZE],
}

impl PageBuffer {
    pub fn read_from(&mut self, reader: &mut impl Read) -> Result<()> {
        let mut attempts = 0;
        let mut remaining = BUF_SIZE;
        while remaining > 0 {
            if attempts > 1000 {
                return Err(Error::Message("Failed to load buffer after many attempts".to_owned()))
            }

            let n = reader.read(&mut self.buf[BUF_SIZE - remaining..])?;
            remaining -= n;
            attempts += 1;
        }
        Ok(())
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&self.buf[..])?;
        Ok(())
    }
}

macro_rules! write_bytes {
    ($buf:expr, $index:expr, $bytes:expr) => {
        for byte in $bytes {
            $buf[$index] = *byte;
            $index += 1;
        }
    };
}

macro_rules! write_int {
    ($buf:expr, $index:expr, $x:expr) => {
        let bytes = $x.to_le_bytes();
        write_bytes!($buf, $index, &bytes);
    };
}

impl PageBuffer {
    pub fn serialize(&mut self, page: &Page) {
        self.serialize_header(&page.header);
        self.serialize_body(&page.body, page.header.count as usize);
    }

    fn serialize_header(&mut self, header: &PageHeader) {
        let mut index = 0;

        // Magic number
        write_int!(self.buf, index, MAGIC);

        // UUID
        write_bytes!(self.buf, index, header.uuid.as_bytes());

        // Ticks
        write_int!(self.buf, index, header.ticks);

        // Min/max key hashes
        write_int!(self.buf, index, header.min_key_hash);
        write_int!(self.buf, index, header.max_key_hash);

        // Count
        write_int!(self.buf, index, header.count);
    }

    // FIXME: broken on platforms that don't use little endianness
    fn serialize_body(&mut self, body: &PageBody, count: usize) {
        let offset = RESERVE_BYTES_FOR_HEADER;
        let key_hash_bytes = &body.key_hash as *const _ as *const u8;
        for i in 0..count as usize * 8 {
            self.buf[offset + i] = unsafe { *key_hash_bytes.add(i) };
        }

        let offset = RESERVE_BYTES_FOR_HEADER + COMMANDS_PER_PAGE * 8;
        let value_index_bytes = &body.value_index as *const _ as *const u8;
        for i in 0..count as usize * 2 {
            self.buf[offset + i] = unsafe { *value_index_bytes.add(i) };
        }
    }
}

impl PageBuffer {
    pub fn deserialize(&self, page: &mut Page) -> Result<()> {
        self.deserialize_header(&mut page.header)?;
        let count = page.header.count;
        self.deserialize_body(&mut page.body, count as usize);
        Ok(())
    }

    fn deserialize_header(&self, header: &mut PageHeader) -> Result<()> {
        let mut index = 0;

        let mut u128_buf = [0u8; 16];
        let mut u64_buf = [0u8; 8];
        let mut u16_buf = [0u8; 2];

        // Magic number
        for (i, byte) in u64_buf.iter_mut().enumerate() {
            *byte = self.buf[i + index];
        }
        index += 8;
        assert_eq!(MAGIC, u64::from_le_bytes(u64_buf));

        // UUID
        for (i, byte) in u128_buf.iter_mut().enumerate() {
            *byte = self.buf[i + index];
        }
        index += 16;
        header.uuid = Uuid::from_slice(&u128_buf)?;

        // Timestamp
        for (i, byte) in u64_buf.iter_mut().enumerate() {
            *byte = self.buf[i + index];
        }
        index += 8;
        let ticks = u64::from_le_bytes(u64_buf);
        header.ticks = ticks;

        // Min/max key hashes
        for (i, byte) in u64_buf.iter_mut().enumerate() {
            *byte = self.buf[i + index];
        }
        index += 8;
        header.min_key_hash = u64::from_le_bytes(u64_buf);

        for (i, byte) in u64_buf.iter_mut().enumerate() {
            *byte = self.buf[i + index];
        }
        index += 8;
        header.max_key_hash = u64::from_le_bytes(u64_buf);

        // Count
        u16_buf[0] = self.buf[index];
        u16_buf[1] = self.buf[index + 1];
        header.count = u16::from_le_bytes(u16_buf);

        Ok(())
    }

    fn deserialize_body(&self, body: &mut PageBody, count: usize) {
        let offset = RESERVE_BYTES_FOR_HEADER;
        for i in 0..count {
            let key_hash_bytes: *const [u8; 8] =
                (&self.buf[offset + i * 8..] as &[u8]).as_ptr() as *const [u8; 8];
            body.key_hash[i] = unsafe { u64::from_le_bytes(*key_hash_bytes) };
        }

        let offset = RESERVE_BYTES_FOR_HEADER + COMMANDS_PER_PAGE * 8;
        for i in 0..count {
            let value_index_bytes: *const [u8; 2] =
                (&self.buf[offset + i * 2..] as &[u8]).as_ptr() as *const [u8; 2];
            body.value_index[i] = unsafe { i16::from_le_bytes(*value_index_bytes) };
        }
    }
}
