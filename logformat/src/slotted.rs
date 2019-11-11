use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Slotted is our data file type. We keep a list of (pointer, length) pairs at the beginning,
/// followed by the heap of data as bytes.
#[derive(Default, Serialize, Deserialize)]
pub struct Slotted {
    header: SlottedHeader,
    body: SlottedBody,
}

#[derive(Default, Serialize, Deserialize)]
struct SlottedHeader {
    offsets: Vec<u16>,
    lens: Vec<u16>,
}

#[derive(Default, Serialize, Deserialize)]
struct SlottedBody {
    bin: Vec<u8>,
}

impl Slotted {
    pub fn new() -> Self {
        Slotted {
            header: SlottedHeader {
                offsets: Vec::default(),
                lens: Vec::default(),
            },
            body: SlottedBody::default(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> usize {
        let index = self.header.offsets.len();
        let offset = self.body.bin.len() as u16;
        self.header.offsets.push(offset);
        self.header.lens.push(bytes.len() as u16);
        for byte in bytes {
            self.body.bin.push(*byte);
        }
        index
    }

    pub fn get(&mut self, index: usize) -> Option<&[u8]> {
        if let Some(offset) = self.header.offsets.get(index) {
            if let Some(len) = self.header.lens.get(index) {
                return Some(&self.body.bin[*offset as usize..*offset as usize + *len as usize]);
            } else {
                panic!("offset and len are different lengths")
            }
        }
        None
    }

    pub fn path(uuid: &Uuid) -> PathBuf {
        Path::new(format!("{}.data", uuid.to_hyphenated_ref()).as_str()).to_owned()
    }
}