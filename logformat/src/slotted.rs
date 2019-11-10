use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Default, Serialize, Deserialize)]
struct Slotted {
    header: SlottedHeader,
    body: SlottedBody,
}

#[derive(Default, Serialize, Deserialize)]
struct SlottedHeader {
    pub uuid: Uuid,
    offsets: Vec<u16>,
    lens: Vec<u16>,
}

#[derive(Default, Serialize, Deserialize)]
struct SlottedBody {
    bin: Vec<u8>,
}

impl Slotted {
    pub fn new(uuid: Uuid) -> Self {
        Slotted {
            header: SlottedHeader {
                uuid,
                offsets: Vec::default(),
                lens: Vec::default(),
            },
            body: SlottedBody::default(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        let offset = self.body.bin.len() as u16;
        self.header.offsets.push(offset);
        self.header.lens.push(bytes.len() as u16);
        for byte in bytes {
            self.body.bin.push(*byte);
        }
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

    pub fn path(&self) -> PathBuf {
        Path::new(format!("{}.data", self.header.uuid.to_hyphenated_ref()).as_str()).to_owned()
    }
}