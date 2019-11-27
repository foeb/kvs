use bincode;
use logformat;
use sled;
use std::fmt::{self, Display};
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    KeyNotFound,
    IoError(io::Error),
    LogFormatError(logformat::Error),
    BincodeError(bincode::Error),
    SledError(sled::Error),
}

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Self {
        Error::SledError(error)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::KeyNotFound => write!(f, "Key not found"),
            _ => write!(f, "{:?}", self),
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

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Self {
        Error::BincodeError(error)
    }
}
