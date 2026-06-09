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

mod backtrace;
mod macros;

pub use backtrace::*;
pub use macros::*;

use akita_core::{AkitaDataError, ConversionError, SqlInjectionError};
use std::error::Error;
use std::{fmt, str::Utf8Error};

pub(crate) type Result<T> = std::result::Result<T, AkitaError>;

#[derive(Debug)]
pub enum AkitaError {
    InvalidSQL(String, SmartBacktrace),
    InterceptorError(String, SmartBacktrace),
    SecurityError(String, SmartBacktrace),
    InvalidField(String, SmartBacktrace),
    MissingIdent(String, SmartBacktrace),
    MissingTable(String, SmartBacktrace),
    MissingField(String, SmartBacktrace),

    /// Keep original MySQL error inside
    #[cfg(feature = "mysql-sync")]
    MySQLError(mysql::Error, SmartBacktrace),

    /// MySQL async error
    #[cfg(feature = "mysql-async")]
    MySQLAsyncError(mysql_async::Error, SmartBacktrace),

    #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
    OracleError(oracle::Error, SmartBacktrace),

    #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
    MssqlError(tiberius::error::Error, SmartBacktrace),

    #[cfg(feature = "postgres-sync")]
    PostgresError(postgres::error::Error, SmartBacktrace),

    #[cfg(feature = "postgres-async")]
    TokioPostgresError(tokio_postgres::error::Error, SmartBacktrace),

    /// Keep original SQLite error
    #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
    SQLiteError(rusqlite::Error, SmartBacktrace),

    TokioError(String, SmartBacktrace),

    ExecuteSqlError {
        message: String,
        sql: String,
        backtrace: SmartBacktrace,
    },

    AkitaDataError(AkitaDataError, SmartBacktrace),
    DataError(String, SmartBacktrace),

    /// Keep original r2d2 error
    #[cfg(any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ))]
    R2D2Error(r2d2::Error, SmartBacktrace),

    /// Keep original deadpool error
    #[cfg(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "oracle-async",
        feature = "mssql-async"
    ))]
    DeadPoolError(String, SmartBacktrace),

    /// Keep original URL parse error
    UrlParseError(url::ParseError, SmartBacktrace),

    RedundantField(String, SmartBacktrace),
    DatabaseError(String, SmartBacktrace),
    UnsupportedOperation(String, SmartBacktrace),
    ConnectionValidError(SmartBacktrace),
    SqlLoaderError(SqlLoaderError, SmartBacktrace),
    EmptyData(SmartBacktrace),
    Unknown(SmartBacktrace),
}

// SqlLoaderError definition (unchanged)
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

impl Error for SqlLoaderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

// ===== Display implementation for AkitaError =====
impl fmt::Display for AkitaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Basic error messages
        let base_msg = match self {
            AkitaError::InvalidSQL(e, _) => format!("Invalid SQL: {e}"),
            AkitaError::InterceptorError(e, _) => format!("Interceptor error: {e}"),
            AkitaError::TokioError(e, _) => format!("Tokio error: {e}"),
            AkitaError::InvalidField(e, _) => format!("Invalid field: {e}"),
            AkitaError::MissingIdent(e, _) => format!("Missing identifier: {e}"),
            AkitaError::SecurityError(e, _) => format!("Dangerous SQL operation: {e}"),
            AkitaError::MissingTable(e, _) => format!("Missing table: {e}"),
            AkitaError::MissingField(e, _) => format!("Missing field: {e}"),

            #[cfg(any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "oracle-async",
                feature = "mssql-async"
            ))]
            AkitaError::DeadPoolError(e, _) => format!("DeadPool Error: {e}"),

            #[cfg(feature = "mysql-sync")]
            AkitaError::MySQLError(e, _) => format!("MySQL error: {e}"),

            #[cfg(feature = "mysql-async")]
            AkitaError::MySQLAsyncError(e, _) => format!("MySQLAsync error: {e}"),

            #[cfg(feature = "postgres-sync")]
            AkitaError::PostgresError(e, _) => format!("Postgres error: {e}"),

            #[cfg(feature = "postgres-async")]
            AkitaError::TokioPostgresError(e, _) => format!("TokioPostgres error: {e}"),

            #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
            AkitaError::OracleError(e, _) => format!("Oracle error: {e}"),

            #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
            AkitaError::SQLiteError(e, _) => format!("SQLite error: {e}"),

            AkitaError::ExecuteSqlError { message, sql, .. } => {
                format!("SQL Execute Error: {message}, SQL: {sql}")
            }
            AkitaError::DataError(e, _) => format!("Data error: {e}"),
            AkitaError::AkitaDataError(e, _) => format!("AkitaData error: {e}"),

            #[cfg(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ))]
            AkitaError::R2D2Error(e, _) => format!("Pool error: {e}"),

            AkitaError::UrlParseError(e, _) => format!("URL parse error: {e}"),
            AkitaError::RedundantField(e, _) => format!("Redundant field: {e}"),
            AkitaError::DatabaseError(e, _) => format!("Database Error : {e}"),
            AkitaError::UnsupportedOperation(e, _) => format!("Unsupported operation: {e}"),
            AkitaError::ConnectionValidError(_) => "Connection is no longer valid".to_string(),
            AkitaError::EmptyData(_) => "No entities to insert".to_string(),
            AkitaError::Unknown(_) => "Unknown error".to_string(),
            AkitaError::SqlLoaderError(e, _) => format!("SqlLoader Error: {e}"),

            #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
            AkitaError::MssqlError(e, _) => format!("MssqlError Error: {e}"),
        };

        // Get backtrace string
        let backtrace_str = match self {
            AkitaError::InvalidSQL(_, bt)
            | AkitaError::InterceptorError(_, bt)
            | AkitaError::SecurityError(_, bt)
            | AkitaError::InvalidField(_, bt)
            | AkitaError::MissingIdent(_, bt)
            | AkitaError::MissingTable(_, bt)
            | AkitaError::MissingField(_, bt)
            | AkitaError::TokioError(_, bt)
            | AkitaError::DatabaseError(_, bt)
            | AkitaError::RedundantField(_, bt)
            | AkitaError::DataError(_, bt)
            | AkitaError::UnsupportedOperation(_, bt)
            | AkitaError::EmptyData(bt)
            | AkitaError::Unknown(bt) => bt.to_display_string(),

            AkitaError::ExecuteSqlError { backtrace, .. } => backtrace.to_display_string(),

            #[cfg(feature = "mysql-sync")]
            AkitaError::MySQLError(_, bt) => bt.to_display_string(),

            #[cfg(feature = "mysql-async")]
            AkitaError::MySQLAsyncError(_, bt) => bt.to_display_string(),

            #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
            AkitaError::OracleError(_, bt) => bt.to_display_string(),

            #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
            AkitaError::MssqlError(_, bt) => bt.to_display_string(),

            #[cfg(feature = "postgres-sync")]
            AkitaError::PostgresError(_, bt) => bt.to_display_string(),

            #[cfg(feature = "postgres-async")]
            AkitaError::TokioPostgresError(_, bt) => bt.to_display_string(),

            #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
            AkitaError::SQLiteError(_, bt) => bt.to_display_string(),

            #[cfg(any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "oracle-async",
                feature = "mssql-async"
            ))]
            AkitaError::DeadPoolError(_, bt) => bt.to_display_string(),

            AkitaError::AkitaDataError(_, bt) => bt.to_display_string(),

            #[cfg(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ))]
            AkitaError::R2D2Error(_, bt) => bt.to_display_string(),

            AkitaError::UrlParseError(_, bt) => bt.to_display_string(),
            AkitaError::ConnectionValidError(bt) => bt.to_display_string(),
            AkitaError::SqlLoaderError(_, bt) => bt.to_display_string(),
        };

        match backtrace_str {
            Some(bt) => write!(f, "{}{}", base_msg, bt),
            None => write!(f, "{}", base_msg),
        }
    }
}

// ===== Error trait implementation =====
impl Error for AkitaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(feature = "mysql-sync")]
            AkitaError::MySQLError(err, _) => Some(err),

            #[cfg(feature = "mysql-async")]
            AkitaError::MySQLAsyncError(err, _) => Some(err),

            #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
            AkitaError::OracleError(err, _) => Some(err),

            #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
            AkitaError::MssqlError(err, _) => Some(err),

            #[cfg(feature = "postgres-sync")]
            AkitaError::PostgresError(err, _) => Some(err),

            #[cfg(feature = "postgres-async")]
            AkitaError::TokioPostgresError(err, _) => Some(err),

            #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
            AkitaError::SQLiteError(err, _) => Some(err),

            #[cfg(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ))]
            AkitaError::R2D2Error(err, _) => Some(err),

            AkitaError::UrlParseError(err, _) => Some(err),

            AkitaError::SqlLoaderError(err, _) => Some(err),

            _ => None,
        }
    }
}

// ===== From implementations (using from_current_level) =====
impl From<Utf8Error> for AkitaError {
    #[track_caller]
    fn from(err: Utf8Error) -> Self {
        AkitaError::data_error(err.to_string())
    }
}
impl From<std::string::FromUtf8Error> for AkitaError {
    #[track_caller]
    fn from(err: std::string::FromUtf8Error) -> Self {
        AkitaError::data_error(err.to_string())
    }
}

impl From<AkitaDataError> for AkitaError {
    #[track_caller]
    fn from(err: AkitaDataError) -> Self {
        AkitaError::akita_data_error(err)
    }
}

impl From<url::ParseError> for AkitaError {
    #[track_caller]
    fn from(err: url::ParseError) -> Self {
        AkitaError::UrlParseError(err, SmartBacktrace::from_current_level())
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
    #[track_caller]
    fn from(err: r2d2::Error) -> Self {
        AkitaError::r2d2_error(err)
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::Error> for AkitaError {
    #[track_caller]
    fn from(err: mysql::Error) -> Self {
        AkitaError::mysql_error(err)
    }
}

#[cfg(feature = "mysql-async")]
impl From<mysql_async::Error> for AkitaError {
    #[track_caller]
    fn from(err: mysql_async::Error) -> Self {
        AkitaError::mysql_async_error(err)
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
    #[track_caller]
    fn from(err: deadpool::managed::BuildError) -> Self {
        AkitaError::deadpool_error(err.to_string())
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
    #[track_caller]
    fn from(err: deadpool_sync::InteractError) -> Self {
        AkitaError::deadpool_error(err.to_string())
    }
}

#[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
impl From<oracle::Error> for AkitaError {
    #[track_caller]
    fn from(err: oracle::Error) -> Self {
        AkitaError::oracle_error(err)
    }
}

#[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
impl From<tiberius::error::Error> for AkitaError {
    #[track_caller]
    fn from(err: tiberius::error::Error) -> Self {
        AkitaError::mssql_error(err)
    }
}

#[cfg(feature = "postgres-sync")]
impl From<postgres::error::Error> for AkitaError {
    #[track_caller]
    fn from(err: postgres::error::Error) -> Self {
        AkitaError::tokio_postgres_error(err)
    }
}

// #[cfg(feature = "postgres-async")]
// impl From<tokio_postgres::error::Error> for AkitaError {
//     fn from(err: tokio_postgres::error::Error) -> Self {
//         AkitaError::TokioPostgresError(err, SmartBacktrace::from_current_level())
//     }
// }

#[cfg(feature = "mysql-sync")]
impl From<mysql::UrlError> for AkitaError {
    #[track_caller]
    fn from(err: mysql::UrlError) -> Self {
        AkitaError::mysql_error(err)
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::FromValueError> for AkitaError {
    #[track_caller]
    fn from(err: mysql::FromValueError) -> Self {
        AkitaError::mysql_error(err)
    }
}

#[cfg(feature = "mysql-sync")]
impl From<mysql::FromRowError> for AkitaError {
    #[track_caller]
    fn from(err: mysql::FromRowError) -> Self {
        AkitaError::mysql_error(err)
    }
}

#[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
impl From<rusqlite::Error> for AkitaError {
    #[track_caller]
    fn from(err: rusqlite::Error) -> Self {
        AkitaError::sqlite_error(err)
    }
}

impl From<ConversionError> for AkitaError {
    #[track_caller]
    fn from(err: ConversionError) -> Self {
        AkitaError::akita_data_error(AkitaDataError::ConversionError(err))
    }
}

impl From<SqlLoaderError> for AkitaError {
    #[track_caller]
    fn from(err: SqlLoaderError) -> Self {
        AkitaError::sql_loader_error(err)
    }
}

impl From<SqlInjectionError> for AkitaError {
    #[track_caller]
    fn from(err: SqlInjectionError) -> Self {
        AkitaError::akita_data_error(AkitaDataError::SqlInjectionError(err))
    }
}

// ===== #[track_caller] constructors for AkitaError =====
impl AkitaError {
    #[track_caller]
    pub fn invalid_sql(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::InvalidSQL(msg.into(), bt)
    }

    #[track_caller]
    pub fn interceptor_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::InterceptorError(msg.into(), bt)
    }

    #[track_caller]
    pub fn security_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::SecurityError(msg.into(), bt)
    }

    #[track_caller]
    pub fn invalid_field(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::InvalidField(msg.into(), bt)
    }

    #[track_caller]
    pub fn missing_ident(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MissingIdent(msg.into(), bt)
    }

    #[track_caller]
    pub fn missing_table(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MissingTable(msg.into(), bt)
    }

    #[track_caller]
    pub fn missing_field(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MissingField(msg.into(), bt)
    }

    #[track_caller]
    pub fn tokio_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::TokioError(msg.into(), bt)
    }

    #[track_caller]
    pub fn data_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::DataError(msg.into(), bt)
    }

    #[track_caller]
    pub fn database_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::DatabaseError(msg.into(), bt)
    }

    #[track_caller]
    pub fn redundant_field(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::RedundantField(msg.into(), bt)
    }

    #[track_caller]
    pub fn unsupported_operation(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::UnsupportedOperation(msg.into(), bt)
    }

    #[track_caller]
    pub fn connection_valid_error() -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::ConnectionValidError(bt)
    }

    #[track_caller]
    pub fn empty_data_error() -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::EmptyData(bt)
    }

    #[track_caller]
    pub fn unknown_error() -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::Unknown(bt)
    }

    #[track_caller]
    pub fn execute_sql_error(sql: impl Into<String>, msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::ExecuteSqlError {
            message: msg.into(),
            sql: sql.into(),
            backtrace: bt,
        }
    }

    #[track_caller]
    pub fn akita_data_error<E: Into<AkitaDataError>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::AkitaDataError(err.into(), bt)
    }

    #[track_caller]
    pub fn sql_loader_error(err: SqlLoaderError) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::SqlLoaderError(err, bt)
    }
}

// Feature-gated constructors for database errors
#[cfg(feature = "mysql-sync")]
impl AkitaError {
    #[track_caller]
    pub fn mysql_error<E: Into<mysql::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MySQLError(err.into(), bt)
    }
}

#[cfg(feature = "mysql-async")]
impl AkitaError {
    #[track_caller]
    pub fn mysql_async_error<E: Into<mysql_async::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MySQLAsyncError(err.into(), bt)
    }
}

#[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
impl AkitaError {
    #[track_caller]
    pub fn oracle_error<E: Into<oracle::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::OracleError(err.into(), bt)
    }
}

#[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
impl AkitaError {
    #[track_caller]
    pub fn mssql_error<E: Into<tiberius::error::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::MssqlError(err.into(), bt)
    }
}

#[cfg(feature = "postgres-sync")]
impl AkitaError {
    #[track_caller]
    pub fn postgres_error<E: Into<postgres::error::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::PostgresError(err.into(), bt)
    }
}

#[cfg(feature = "postgres-async")]
impl AkitaError {
    #[track_caller]
    pub fn tokio_postgres_error<E: Into<tokio_postgres::error::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::TokioPostgresError(err.into(), bt)
    }
}

#[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
impl AkitaError {
    #[track_caller]
    pub fn sqlite_error<E: Into<rusqlite::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::SQLiteError(err.into(), bt)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl AkitaError {
    #[track_caller]
    pub fn r2d2_error<E: Into<r2d2::Error>>(err: E) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::R2D2Error(err.into(), bt)
    }
}

#[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))]
impl AkitaError {
    #[track_caller]
    pub fn deadpool_error(msg: impl Into<String>) -> Self {
        let loc = std::panic::Location::caller();
        let bt = match current_backtrace_mode() {
            BacktraceMode::Full => SmartBacktrace::full(),
            BacktraceMode::Light => SmartBacktrace::light(loc.file(), loc.line(), loc.column()),
            BacktraceMode::None => SmartBacktrace::none(),
        };
        AkitaError::DeadPoolError(msg.into(), bt)
    }
}
