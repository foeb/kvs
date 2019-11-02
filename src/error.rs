use crate::wal::Key;

use ron::{ser, de};

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    NonExistentKey(Key),
    IoError(io::Error),
    SerError(ser::Error),
    DeError(de::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NonExistentKey(key) => write!(f, "Key not found: {}", key),
            Error::IoError(err) => fmt::Display::fmt(err, f),
            Error::SerError(err) => fmt::Display::fmt(err, f),
            Error::DeError(err) => fmt::Display::fmt(err, f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<ser::Error> for Error {
    fn from(error: ser::Error) -> Self {
        Error::SerError(error)
    }
}

impl From<de::Error> for Error {
    fn from(error: de::Error) -> Self {
        Error::DeError(error)
    }
}