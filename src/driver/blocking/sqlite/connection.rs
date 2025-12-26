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
use std::fmt;
use std::path::{Path, PathBuf};
use r2d2::{Pool};
use rusqlite::{Connection, Error, OpenFlags};
use crate::config::AkitaConfig;
use crate::driver::DriverType;
use crate::errors::AkitaError;

pub type SqlitePool = Pool<SqliteConnectionManager>;
pub type SqliteConnection = r2d2::PooledConnection<SqliteConnectionManager>;


#[derive(Debug)]
enum Source {
    File(PathBuf),
    Memory,
}

type InitFn = dyn Fn(&mut Connection) -> std::result::Result<(), rusqlite::Error> + Send + Sync + 'static;

pub struct SqliteConnectionManager {
    source: Source,
    flags: OpenFlags,
    init: Option<Box<InitFn>>,
}

impl fmt::Debug for SqliteConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("SqliteConnectionManager");
        let _ = builder.field("source", &self.source);
        let _ = builder.field("flags", &self.source);
        let _ = builder.field("init", &self.init.as_ref().map(|_| "InitFn"));
        builder.finish()
    }
}

impl SqliteConnectionManager {
    
    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        if cfg.get_platform()? != DriverType::Sqlite {
            return Err(AkitaError::DatabaseError(
                "Database type mismatch: expected SQLite".to_string()
            ));
        }

        let file_path = cfg.get_connection_string()?;

        // SQLite special handling: file path or :memory:
        if file_path == ":memory:" {
            Ok(Self::memory())
        } else {
            Ok(Self::file(file_path))
        }
    }
    
    /// Creates a new `SqliteConnectionManager` from file.
    ///
    /// See `rusqlite::Connection::open`
    pub fn file<P: AsRef<Path>>(path: P) -> Self {
        Self {
            source: Source::File(path.as_ref().to_path_buf()),
            flags: OpenFlags::default(),
            init: None,
        }
    }

    /// Creates a new `SqliteConnectionManager` from memory.
    pub fn memory() -> Self {
        Self {
            source: Source::Memory,
            flags: OpenFlags::default(),
            init: None,
        }
    }

    /// Converts `SqliteConnectionManager` into one that sets OpenFlags upon
    /// connection creation.
    ///
    /// See `rustqlite::OpenFlags` for a list of available flags.
    pub fn with_flags(self, flags: OpenFlags) -> Self {
        Self { flags, ..self }
    }

    /// Converts `SqliteConnectionManager` into one that calls an initialization
    /// function upon connection creation. Could be used to set PRAGMAs, for
    /// example.
    ///
    /// ### Example
    ///
    /// Make a `SqliteConnectionManager` that sets the `foreign_keys` pragma to
    /// true for every connection.
    ///
    /// ```rust,no_run
    /// ```
    pub fn with_init<F>(self, init: F) -> Self
    where
        F: Fn(&mut Connection) -> std::result::Result<(), rusqlite::Error> + Send + Sync + 'static,
    {
        let init: Option<Box<InitFn>> = Some(Box::new(init));
        Self { init, ..self }
    }
}

impl r2d2::ManageConnection for SqliteConnectionManager {
    type Connection = Connection;
    type Error = rusqlite::Error;

    fn connect(&self) -> std::result::Result<Connection, Error> {
        match self.source {
            Source::File(ref path) => Connection::open_with_flags(path, self.flags),
            Source::Memory => Connection::open_in_memory_with_flags(self.flags),
        }
            .map_err(Into::into)
            .and_then(|mut c| match self.init {
                None => Ok(c),
                Some(ref init) => init(&mut c).map(|_| c),
            })
    }

    fn is_valid(&self, conn: &mut Connection) -> std::result::Result<(), Error> {
        conn.execute_batch("").map_err(Into::into)
    }

    fn has_broken(&self, conn: &mut Connection) -> bool {
        self.is_valid(conn).is_err()
    }
}

///
/// Create a connection pool
/// cfg Configuration information
///
pub fn init_sqlite_pool(cfg: AkitaConfig) -> Result<SqlitePool, AkitaError> {
    let manager = SqliteConnectionManager::new(&cfg)?;

    let pool = r2d2::Pool::builder()
        .connection_timeout(cfg.get_connection_timeout())
        .min_idle(cfg.get_min_idle().into())
        .max_size(cfg.get_max_size())
        .idle_timeout(Some(cfg.get_idle_timeout()))
        .max_lifetime(Some(cfg.get_max_lifetime()))
        .test_on_check_out(cfg.get_test_on_check_out())
        .build(manager)
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to create SQLite connection pool: {}", e))
        })?;

    // Testing connections
    let conn = pool.get().map_err(|e| {
        AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e))
    })?;

    conn.execute_batch("SELECT 1").map_err(|e| {
        AkitaError::DatabaseError(format!("SQLite connection test failed: {}", e))
    })?;

    Ok(pool)
}