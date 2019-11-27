use crate::page::PageHeader;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// FIXME: make this into a B-tree (or something like it) with pages as leaves
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Index {
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

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    pub fn path() -> PathBuf {
        Path::new("index").to_owned()
    }
}
