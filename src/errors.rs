/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */

//!
//! Common Errors.
//! 
use std::{fmt, str::Utf8Error, string::ParseError};
use akita_core::AkitaDataError;

use crate::ConvertError;

pub type Result<T> = std::result::Result<T, AkitaError>;


#[derive(Debug)]
pub enum AkitaError {
    InvalidSQL(String),
    InvalidField(String),
    MissingIdent(String),
    MissingTable(String),
    MissingField(String),
    MySQLError(String),
    SQLiteError(String),
    ExecuteSqlError(String, String),
    DataError(String),
    R2D2Error(String),
    UrlParseError(String),
    RedundantField(String),
    UnknownDatabase(String),
    UnsupportedOperation(String),
    Unknown,
}

impl fmt::Display for AkitaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AkitaError::Unknown => write!(f, "Unknown Error"),
            AkitaError::InvalidSQL(ref err) => err.fmt(f),
            AkitaError::InvalidField(ref err) => err.fmt(f),
            AkitaError::ExecuteSqlError(ref err, ref sql) => write!(f, "SQL Execute Error: {}, SQL: {}", err, sql),
            AkitaError::UnsupportedOperation(ref err) => write!(f, "Unsupported operation: {}", err),
            AkitaError::UnknownDatabase(ref schema) => write!(f, "Unknown Database URL :{} (Just Support MySQL)", schema),
            AkitaError::MissingIdent(ref err) => err.fmt(f),
            AkitaError::UrlParseError(ref err) => err.fmt(f),
            AkitaError::DataError(ref err) => err.fmt(f),
            AkitaError::MissingTable(ref err) => err.fmt(f),
            AkitaError::MissingField(ref err) => err.fmt(f),
            AkitaError::RedundantField(ref err) => err.fmt(f),
            AkitaError::MySQLError(ref err) => err.fmt(f),
            AkitaError::SQLiteError(ref err) => err.fmt(f),
            AkitaError::R2D2Error(ref err) => err.fmt(f),
        }
    }
}

#[allow(deprecated, deprecated_in_future)]
impl std::error::Error for AkitaError {
    fn description(&self) -> &str {
        match *self {
            AkitaError::Unknown => "Unknown Error",
            AkitaError::UnknownDatabase(ref err) => err,
            AkitaError::InvalidSQL(ref err) => err,
            AkitaError::ExecuteSqlError(ref err, ref _sql) => err,
            AkitaError::InvalidField(ref err) => err,
            AkitaError::UnsupportedOperation(ref err) => err,
            AkitaError::UrlParseError(ref err) => err,
            AkitaError::MissingIdent(ref err) => err,
            AkitaError::DataError(ref err) => err,
            AkitaError::MissingTable(ref err) => err,
            AkitaError::MissingField(ref err) => err,
            AkitaError::RedundantField(ref err) => err,
            AkitaError::MySQLError(ref err) => err,
            AkitaError::SQLiteError(ref err) => err,
            AkitaError::R2D2Error(ref err) => err,
        }
    }
}



impl From<Utf8Error> for AkitaError {
    fn from(err: Utf8Error) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}
impl From<AkitaDataError> for AkitaError {
    fn from(err: AkitaDataError) -> Self {
        AkitaError::DataError(err.to_string())
    }
}

impl From<ParseError> for AkitaError {
    fn from(err: ParseError) -> Self {
        AkitaError::UrlParseError(err.to_string())
    }
}
impl From<ConvertError> for AkitaError {
    fn from(err: ConvertError) -> Self {
        match err {
            ConvertError::NotSupported(v, ty) => {
                AkitaError::DataError(format!("[{}]:{}", ty, v))
            }
        }
    }
}

#[cfg(feature = "akita-mysql")]
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

#[cfg(feature = "akita-mysql")]
impl From<mysql::UrlError> for AkitaError {
    fn from(err: mysql::UrlError) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}

#[cfg(feature = "akita-sqlite")]
impl From<rusqlite::Error> for AkitaError {
    fn from(err: rusqlite::Error) -> Self {
        AkitaError::SQLiteError(err.to_string())
    }
}

#[cfg(feature = "akita-mysql")]
impl From<mysql::FromValueError> for AkitaError {
    fn from(err: mysql::FromValueError) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}

#[cfg(feature = "akita-mysql")]
impl From<mysql::FromRowError> for AkitaError {
    fn from(err: mysql::FromRowError) -> Self {
        AkitaError::MySQLError(err.to_string())
    }
}