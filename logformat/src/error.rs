use serde::{de, ser};
use std::fmt;
use std::io;
use uuid;
use std::time::SystemTimeError;

/// The result type for everything in the logformat crate.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    IoError(io::Error),
    UuidError(uuid::Error),
    SystemTimeError(SystemTimeError),
    UnexpectedEof,
}

impl From<uuid::Error> for Error {
    fn from(error: uuid::Error) -> Self {
        Error::UuidError(error)
    }
}

impl From<SystemTimeError> for Error {
    fn from(error: SystemTimeError) -> Self {
        Error::SystemTimeError(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Message(msg) => write!(f, "{}", msg),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl std::error::Error for Error {}
