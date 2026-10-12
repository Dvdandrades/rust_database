use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Data corruption detected: {0}")]
    Corruption(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, DbError>;
