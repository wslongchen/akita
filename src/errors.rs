//! 
//! Common Errors.
//! 
use std::fmt;

#[derive(Debug)]
pub enum AkitaError {
    InvalidSQL(String),
    InvalidField(String),
    MissingIdent(String),
    MissingTable(String),
    MissingField(String),
    MySQLError(String),
    R2D2Error(String),
    RedundantField(String),
    Unknown,
}

impl fmt::Display for AkitaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AkitaError::Unknown => write!(f, "Unknown Error"),
            AkitaError::InvalidSQL(ref err) => err.fmt(f),
            AkitaError::InvalidField(ref err) => err.fmt(f),
            AkitaError::MissingIdent(ref err) => err.fmt(f),
            AkitaError::MissingTable(ref err) => err.fmt(f),
            AkitaError::MissingField(ref err) => err.fmt(f),
            AkitaError::RedundantField(ref err) => err.fmt(f),
            AkitaError::MySQLError(ref err) => err.fmt(f),
            AkitaError::R2D2Error(ref err) => err.fmt(f),
        }
    }
}

#[allow(deprecated, deprecated_in_future)]
impl std::error::Error for AkitaError {
    fn description(&self) -> &str {
        match *self {
            AkitaError::Unknown => "Unknown Error",
            AkitaError::InvalidSQL(ref err) => err,
            AkitaError::InvalidField(ref err) => err,
            AkitaError::MissingIdent(ref err) => err,
            AkitaError::MissingTable(ref err) => err,
            AkitaError::MissingField(ref err) => err,
            AkitaError::RedundantField(ref err) => err,
            AkitaError::MySQLError(ref err) => err,
            AkitaError::R2D2Error(ref err) => err,
        }
    }
}


impl From<mysql::Error> for AkitaError {
    fn from(err: mysql::Error) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}

impl From<r2d2::Error> for AkitaError {
    fn from(err: r2d2::Error) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}