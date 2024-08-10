use std::{io, ops::Deref};

use thiserror::Error;
#[derive(Error,Debug)]
pub enum Error {
    #[error("Unexpected: {0}, {1}")]
    UnexpectIO(String, io::Error),
    #[error("Unexpected: {0}")]
    Unexpected(String),
    #[error("db open fail: {0}")]
    DBOpenFail(io::Error),
    #[error("invalid database")]
    ErrInvalid,
    #[error("version mismatch")]
    ErrVersionMismatch,
    #[error("checksum error")]
    ErrChecksum,
    #[error("incompatible value")]
    ErrIncompatibleValue,
    #[error("key required")]
    ErrKeyRequired,
    #[error("key too large")]
    ErrKeyTooLarge,
    #[error("value too large")]
    ErrValueTooLarge,
    #[error("IncompatibleValue")]
    IncompatibleValue,
}



pub type Result<T>  = std::result::Result<T,Error>;


impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::Unexpected(value.to_string())
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Unexpected(value)
    }
}

impl From<(&str,io::Error)> for Error {
    fn from(value: (&str,io::Error)) -> Self {
        Self::UnexpectIO(value.0.to_string(), value.1) 
    } 
}



impl From<Error> for String {
   fn from(value: Error) -> Self {
       format!("{}",value)
   } 
}
