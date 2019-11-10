use crate::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::v1::{ClockSequence, Timestamp};
use uuid::Uuid;

pub struct Page {
    pub header: PageHeader,
    pub body: PageBody,
}

impl Default for Page {
    fn default() -> Self {
        Page {
            header: PageHeader::default(),
            body: PageBody {
                key_hash: [0; COMMANDS_PER_PAGE],
                value_index: [0; COMMANDS_PER_PAGE],
            },
        }
    }
}

impl Page {
    pub fn new(header: PageHeader) -> Self {
        Page {
            header,
            body: PageBody {
                key_hash: [0; COMMANDS_PER_PAGE],
                value_index: [0; COMMANDS_PER_PAGE],
            },
        }
    }
}

pub const MAGIC: u64 = 0x78736769;

pub const RESERVE_BYTES_FOR_HEADER: usize = 384;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub uuid: Uuid,
    pub ticks: u64,
    pub min_key_hash: u64,
    pub max_key_hash: u64,
}

impl Default for PageHeader {
    fn default() -> Self {
        PageHeader {
            uuid: Uuid::default(),
            ticks: 0,
            min_key_hash: 0,
            max_key_hash: 0,
        }
    }
}

impl PageHeader {
    pub fn new(
        node_id: &[u8],
        context: &impl ClockSequence,
        min_key_hash: u64,
        max_key_hash: u64,
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
        })
    }

    pub fn path(&self) -> PathBuf {
        Path::new(format!("{}.log", self.uuid.to_hyphenated_ref()).as_str()).to_owned()
    }
}

pub const COMMANDS_PER_PAGE: usize = 1600;

pub struct PageBody {
    pub key_hash: [u64; COMMANDS_PER_PAGE],
    pub value_index: [i16; COMMANDS_PER_PAGE],
}

pub const BUF_SIZE: usize = 16384;

pub struct PageBuffer<'a> {
    pub buf: &'a mut [u8; BUF_SIZE],
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

impl<'a> PageBuffer<'a> {
    pub fn serialize(&mut self, page: &Page) {
        self.serialize_header(&page.header);
        self.serialize_body(&page.body);
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
    }

    // FIXME: broken on platforms that don't use little endianness
    fn serialize_body(&mut self, body: &PageBody) {
        let offset = RESERVE_BYTES_FOR_HEADER;
        let key_hash_bytes = &body.key_hash as *const _ as *const u8;
        for i in 0..COMMANDS_PER_PAGE * 8 {
            self.buf[offset + i] = unsafe { *key_hash_bytes.offset(i as isize) };
        }

        let offset = RESERVE_BYTES_FOR_HEADER + COMMANDS_PER_PAGE * 8;
        let value_index_bytes = &body.value_index as *const _ as *const u8;
        for i in 0..COMMANDS_PER_PAGE * 2 {
            self.buf[offset + i] = unsafe { *value_index_bytes.offset(i as isize) };
        }
    }
}

impl<'a> PageBuffer<'a> {
    pub fn deserialize(&self, page: &mut Page) -> Result<()> {
        self.deserialize_header(&mut page.header)?;
        self.deserialize_body(&mut page.body);
        Ok(())
    }

    fn deserialize_header(&self, header: &mut PageHeader) -> Result<()> {
        let mut index = 0;

        let mut u128_buf = [0u8; 16];
        let mut u64_buf = [0u8; 8];

        // Magic number
        for i in 0..8 {
            u64_buf[i] = self.buf[i + index];
        }
        index += 8;
        assert_eq!(MAGIC, u64::from_le_bytes(u64_buf));

        // UUID
        for i in 0..16 {
            u128_buf[i] = self.buf[i + index];
        }
        index += 16;
        header.uuid = Uuid::from_slice(&u128_buf)?;

        // Timestamp
        for i in 0..8 {
            u64_buf[i] = self.buf[i + index];
        }
        index += 8;
        let ticks = u64::from_le_bytes(u64_buf);
        header.ticks = ticks;

        // Min/max key hashes
        for i in 0..8 {
            u64_buf[i] = self.buf[i + index];
        }
        index += 8;
        header.min_key_hash = u64::from_le_bytes(u64_buf);

        for i in 0..8 {
            u64_buf[i] = self.buf[i + index];
        }
        header.max_key_hash = u64::from_le_bytes(u64_buf);

        Ok(())
    }

    fn deserialize_body(&self, body: &mut PageBody) {
        let offset = RESERVE_BYTES_FOR_HEADER;
        for i in 0..COMMANDS_PER_PAGE {
            let key_hash_bytes: *const [u8; 8] =
                (&self.buf[offset + i * 8..] as &[u8]).as_ptr() as *const [u8; 8];
            body.key_hash[i] = unsafe { u64::from_le_bytes(*key_hash_bytes) };
        }

        let offset = RESERVE_BYTES_FOR_HEADER + COMMANDS_PER_PAGE * 8;
        for i in 0..COMMANDS_PER_PAGE {
            let value_index_bytes: *const [u8; 2] =
                (&self.buf[offset + i * 2..] as &[u8]).as_ptr() as *const [u8; 2];
            body.value_index[i] = unsafe { i16::from_le_bytes(*value_index_bytes) };
        }
    }
}
