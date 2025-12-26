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
use akita_core::{AkitaDataError, ConversionError, SqlInjectionError};
use std::error::Error;
use std::{fmt, str::Utf8Error};
use std::ops::Deref;

pub(crate) type Result<T> = std::result::Result<T, AkitaError>;
#[derive(Debug)]
pub enum AkitaError {
    InvalidSQL(String),
    InterceptorError(String),
    SecurityError(String),
    InvalidField(String),
    MissingIdent(String),
    MissingTable(String),
    MissingField(String),

    /// Keep original MySQL error inside
    #[cfg(feature = "mysql-sync")]
    MySQLError(mysql::Error),

    /// MySQL async error
    #[cfg(feature = "mysql-async")]
    MySQLAsyncError(mysql_async::Error),

    #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
    OracleError(oracle::Error),

    #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
    MssqlError(tiberius::error::Error),

    #[cfg(feature = "postgres-sync")]
    PostgresError(postgres::error::Error),

    #[cfg(feature = "postgres-async")]
    TokioPostgresError(tokio_postgres::error::Error),

    /// Keep original SQLite error
    #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
    SQLiteError(rusqlite::Error),

    TokioError(String),
    ExecuteSqlError {
        message: String,
        sql: String,
    },
    AkitaDataError(AkitaDataError),
    DataError(String),

    /// Keep original r2d2 error
    #[cfg(any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ))]
    R2D2Error(r2d2::Error),

    /// Keep original deadpool error
    #[cfg(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "oracle-async",
        feature = "mssql-async"
    ))]
    DeadPoolError(String),

    /// Keep original URL parse error
    UrlParseError(url::ParseError),
    RedundantField(String),
    DatabaseError(String),
    UnsupportedOperation(String),
    ConnectionValidError,
    SqlLoaderError(SqlLoaderError),
    EmptyData,
    Unknown,
}

// 错误类型定义
#[derive(Debug)]
pub enum SqlLoaderError {
    FileReadError(String),
    XmlParseError(String),
    SqlNotFound(String),
    ParameterError(String),
    SqlSyntaxError(String),
    CacheError(String),
}


impl fmt::Display for SqlLoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlLoaderError::FileReadError(e) => write!(f, "File reading error: {e}"),
            SqlLoaderError::XmlParseError(e) => write!(f, "XML Parsing error: {e}"),
            SqlLoaderError::SqlNotFound(e) => write!(f, "SQL ID '{e}' NotFound"),
            SqlLoaderError::ParameterError(e) => write!(f, "Parameter parsing error: {e}"),
            SqlLoaderError::SqlSyntaxError(e) => write!(f, "SQL Grammatical errors: {e}"),
            SqlLoaderError::CacheError(e) => write!(f, "Caching error: {e}"),
        }
    }
}


impl fmt::Display for AkitaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AkitaError::InvalidSQL(e) => write!(f, "Invalid SQL: {e}"),
            AkitaError::InterceptorError(e) => write!(f, "Interceptor error: {e}"),
            AkitaError::TokioError(e) => write!(f, "Tokio error: {e}"),
            AkitaError::InvalidField(e) => write!(f, "Invalid field: {e}"),
            AkitaError::MissingIdent(e) => write!(f, "Missing identifier: {e}"),
            AkitaError::SecurityError(e) => write!(f, "Dangerous SQL operation: {e}"),
            AkitaError::MissingTable(e) => write!(f, "Missing table: {e}"),
            AkitaError::MissingField(e) => write!(f, "Missing field: {e}"),

            #[cfg(any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "oracle-async",
                feature = "mssql-async"
            ))]
            AkitaError::DeadPoolError(e) => write!(f, "DeadPool Error: {e}"),

            #[cfg(feature = "mysql-sync")]
            AkitaError::MySQLError(e) => write!(f, "MySQL error: {e}"),

            #[cfg(feature = "mysql-async")]
            AkitaError::MySQLAsyncError(e) => write!(f, "MySQLAsync error: {e}"),

            #[cfg(feature = "postgres-sync")]
            AkitaError::PostgresError(e) => write!(f, "Postgres error: {e}"),

            #[cfg(feature = "postgres-async")]
            AkitaError::TokioPostgresError(e) => write!(f, "TokioPostgres error: {e}"),

            #[cfg(feature = "oracle-sync")]
            AkitaError::OracleError(e) => write!(f, "Oracle error: {e}"),

            #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
            AkitaError::SQLiteError(e) => write!(f, "SQLite error: {e}"),

            AkitaError::ExecuteSqlError { message, sql } => {
                write!(f, "SQL Execute Error: {message}, SQL: {sql}")
            }
            AkitaError::DataError(e) => write!(f, "Data error: {e}"),
            AkitaError::AkitaDataError(e) => write!(f, "AkitaData error: {e}"),

            #[cfg(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ))]
            AkitaError::R2D2Error(e) => write!(f, "Pool error: {e}"),

            AkitaError::UrlParseError(e) => write!(f, "URL parse error: {e}"),
            AkitaError::RedundantField(e) => write!(f, "Redundant field: {e}"),
            AkitaError::DatabaseError(e) => {
                write!(f, "Database Error : {e}")
            }
            AkitaError::UnsupportedOperation(e) => write!(f, "Unsupported operation: {e}"),
            AkitaError::ConnectionValidError => write!(f, "Connection is no longer valid"),
            AkitaError::EmptyData => write!(f, "No entities to insert"),
            AkitaError::Unknown => write!(f, "Unknown error"),
            AkitaError::SqlLoaderError(e) => write!(f, "SqlLoader Error: {e}"),
            #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
            AkitaError::MssqlError(e) => write!(f, "MssqlError Error: {e}"),
            _ => {
                write!(f, "Unknown Error:{:?}", self)
            }
        }
    }
}

impl Error for AkitaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(feature = "mysql-sync")]
            AkitaError::MySQLError(err) => Some(err),

            #[cfg(feature = "postgres-sync")]
            AkitaError::PostgresError(err) => Some(err),

            #[cfg(feature = "postgres-async")]
            AkitaError::TokioPostgresError(err) => Some(err),

            #[cfg(feature = "sqlite-sync")]
            AkitaError::SQLiteError(err) => Some(err),

            #[cfg(feature = "oracle-sync")]
            AkitaError::OracleError(err) => Some(err),

            #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
            AkitaError::MssqlError(err) => Some(err),

            #[cfg(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ))]
            AkitaError::R2D2Error(err) => Some(err),

            AkitaError::UrlParseError(err) => Some(err),
            _ => None,
        }
    }
}

//
// ───────────────────────────────────────────────
//   CONVERSIONS (From<T> → AkitaError)
// ───────────────────────────────────────────────
//

impl From<Utf8Error> for AkitaError {
    fn from(err: Utf8Error) -> Self {
        AkitaError::DataError(err.to_string())
    }
}

impl From<AkitaDataError> for AkitaError {
    fn from(err: AkitaDataError) -> Self {
        AkitaError::AkitaDataError(err)
    }
}

impl From<url::ParseError> for AkitaError {
    fn from(err: url::ParseError) -> Self {
        AkitaError::UrlParseError(err)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl From<r2d2::Error> for AkitaError {
    fn from(err: r2d2::Error) -> Self {
        AkitaError::R2D2Error(err)
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::Error> for AkitaError {
    fn from(err: mysql::Error) -> Self {
        AkitaError::MySQLError(err)
    }
}

#[cfg(feature = "mysql-async")]
impl From<mysql_async::Error> for AkitaError {
    fn from(err: mysql_async::Error) -> Self {
        AkitaError::MySQLAsyncError(err)
    }
}

#[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))]
impl From<deadpool::managed::BuildError> for AkitaError {
    fn from(err: deadpool::managed::BuildError) -> Self {
        AkitaError::DeadPoolError(err.to_string())
    }
}

#[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))]
impl From<deadpool_sync::InteractError> for AkitaError {
    fn from(err: deadpool_sync::InteractError) -> Self {
        AkitaError::DeadPoolError(err.to_string())
    }
}

#[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
impl From<oracle::Error> for AkitaError {
    fn from(err: oracle::Error) -> Self {
        AkitaError::OracleError(err)
    }
}

#[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
impl From<tiberius::error::Error> for AkitaError {
    fn from(err: tiberius::error::Error) -> Self {
        AkitaError::MssqlError(err)
    }
}

#[cfg(feature = "postgres-sync")]
impl From<postgres::error::Error> for AkitaError {
    fn from(err: postgres::error::Error) -> Self {
        AkitaError::PostgresError(err)
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::UrlError> for AkitaError {
    fn from(err: mysql::UrlError) -> Self {
        AkitaError::MySQLError(err.into())
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::FromValueError> for AkitaError {
    fn from(err: mysql::FromValueError) -> Self {
        AkitaError::MySQLError(err.into())
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::FromRowError> for AkitaError {
    fn from(err: mysql::FromRowError) -> Self {
        AkitaError::MySQLError(err.into())
    }
}

#[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
impl From<rusqlite::Error> for AkitaError {
    fn from(err: rusqlite::Error) -> Self {
        AkitaError::SQLiteError(err)
    }
}

impl From<ConversionError> for AkitaError {
    fn from(err: ConversionError) -> Self {
        AkitaError::AkitaDataError(AkitaDataError::ConversionError(err))
    }
}
impl From<SqlLoaderError> for AkitaError {
    fn from(err: SqlLoaderError) -> Self {
        AkitaError::SqlLoaderError(err)
    }
}

impl From<SqlInjectionError> for AkitaError {
    fn from(err: SqlInjectionError) -> Self {
        AkitaError::AkitaDataError(AkitaDataError::SqlInjectionError(err))
    }
}