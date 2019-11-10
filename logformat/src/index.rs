use crate::page::PageHeader;
use serde::{Serialize, Deserialize};
use std::path::{Path, PathBuf};

#[derive(Default, Serialize, Deserialize)]
struct Index {
    headers: Vec<PageHeader>,
}

impl Index {
    pub fn push(&mut self, header: PageHeader) {
        self.headers.push(header);
    }

    pub fn get(&self, i: usize) -> Option<&PageHeader> {
        self.headers.get(i)
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    pub fn path(&self) -> PathBuf {
        Path::new("index").to_owned()
    }
}
