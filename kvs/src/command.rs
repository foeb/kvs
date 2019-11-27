use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Debug, Deserialize, Serialize)]
pub enum CommandRequest {
    Get { key: String },
    Set { key: String, value: Option<String> },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CommandResponse {
    Message(String),
    KeyNotFound,
}

impl Display for CommandResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandResponse::Message(s) => write!(f, "{}", s),
            CommandResponse::KeyNotFound => write!(f, "Key not found"),
        }
    }
}
