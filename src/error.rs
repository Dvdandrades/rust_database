use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("I/O error: {0}")]
    Io(io::Error),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Unsupported format version: found {found}, expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },

    #[error("Unexpected end of file")]
    UnexpectedEof,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Data corruption detected: {0}")]
    Corruption(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::PermissionDenied => DbError::PermissionDenied(error.to_string()),
            io::ErrorKind::UnexpectedEof => DbError::UnexpectedEof,
            _ => DbError::Io(error),
        }
    }
}

pub type Result<T> = std::result::Result<T, DbError>;
