use logformat;
use std::fmt::{Display, self};
use std::io;
use bincode;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    NonExistentKey,
    IoError(io::Error),
    LogFormatError(logformat::Error),
    BincodeError(bincode::Error)
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NonExistentKey => write!(f, "Key not found"),
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