use bincode;
use serde::{de, ser};
use std::fmt;
use std::io;
use std::num::TryFromIntError;
use uuid;
use std::time::SystemTimeError;

/// The result type for everything in the logformat crate.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for everything in the logformat crate.
#[derive(Debug)]
pub enum Error {
    /// Returned by serde when deserializing log entries.
    Message(String),

    /// Bincode (the format we use for log entries) has its own error type.
    BincodeError(bincode::Error),

    /// Reading/writing/seeking
    IoError(io::Error),

    /// We need to convert usize to i64, which technically might fail.
    TryFromIntError(TryFromIntError),

    /// This is returned if we try to write to a full log file.
    FileOutOfSpaceError(),

    /// We rely on the read/write buffers being filled to at least the size of
    /// a log entry. This is thrown if that doesn't happen.
    BufferFillError(usize),

    /// Similarly, if somehow we end up inbetween the start of two log entries we
    /// have to give up.
    SeekError(),

    /// We store UTF-8 strings as binary in the data files. This handles the case
    /// when we read something we weren't supposed to or the data is corrupted.
    FromUtf8Error(std::string::FromUtf8Error),

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

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Self {
        Error::BincodeError(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<TryFromIntError> for Error {
    fn from(error: TryFromIntError) -> Self {
        Error::TryFromIntError(error)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8Error(error)
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
