use logformat;
use logformat::mem::Key;

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Message(String),
    NonExistentKey(Key),
    IoError(io::Error),
    LogFormatError(logformat::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(err) => fmt::Display::fmt(err, f),
            Error::NonExistentKey(key) => write!(f, "Key not found: {}", key),
            Error::IoError(err) => fmt::Display::fmt(err, f),
            Error::LogFormatError(err) => fmt::Display::fmt(err, f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<logformat::Error> for Error {
    fn from(error: logformat::Error) -> Self {
        Error::LogFormatError(error)
    }
}
