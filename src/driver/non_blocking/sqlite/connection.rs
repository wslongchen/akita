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
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;
use deadpool::managed::{Metrics, Object, Pool, RecycleError, RecycleResult};
use deadpool::Runtime;
use deadpool_sync::SyncWrapper;
use rusqlite::{Connection, Error, OpenFlags};
use tokio::runtime::Handle;
use tokio::task;
use crate::config::AkitaConfig;
use crate::driver::DriverType;
use crate::driver::non_blocking::get_tokio_context;
use crate::errors::AkitaError;

/// SQLite Asynchronous connection pool type
pub type SqliteAsyncPool = Pool<SqliteAsyncConnectionManager>;
/// SQLite Asynchronous connection type
pub type SqliteAsyncConnection = Object<SqliteAsyncConnectionManager>;

#[derive(Debug)]
enum Source {
    File(PathBuf),
    Memory,
}
type InitFn = dyn Fn(&mut Connection) -> std::result::Result<(), rusqlite::Error> + Send + Sync + 'static;

/// SQLite Asynchronous connection manager
pub struct SqliteAsyncConnectionManager {
    source: Source,
    flags: OpenFlags,
    init: Option<Box<InitFn>>,
    runtime: Runtime,
    recycle_count: AtomicUsize,
}

impl fmt::Debug for SqliteAsyncConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("SqliteAsyncConnectionManager");
        let _ = builder.field("source", &self.source);
        let _ = builder.field("flags", &self.source);
        let _ = builder.field("init", &self.init.as_ref().map(|_| "InitFn"));
        let _ = builder.field("runtime", &self.runtime);
        let _ = builder.field("recycle_count", &self.recycle_count);
        builder.finish()
    }
}

impl SqliteAsyncConnectionManager {
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
            recycle_count: AtomicUsize::new(0),
            runtime: Runtime::Tokio1,
        }
    }

    /// Creates a new `SqliteConnectionManager` from memory.
    pub fn memory() -> Self {
        Self {
            source: Source::Memory,
            flags: OpenFlags::default(),
            init: None,
            recycle_count: AtomicUsize::new(0),
            runtime: Runtime::Tokio1,
        }
    }
}



#[async_trait]
impl deadpool::managed::Manager for SqliteAsyncConnectionManager {
    type Type = SyncWrapper<Connection>;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = match self.source {
            Source::File(ref path) => Connection::open_with_flags(path, self.flags),
            Source::Memory => Connection::open_in_memory_with_flags(self.flags),
        }
            .map_err(Into::into)
            .and_then(|mut c| match self.init {
                None => Ok(c),
                Some(ref init) => init(&mut c).map(|_| c),
            });
        SyncWrapper::new(self.runtime, move|| conn).await
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        // Perform a simple query to check the connection
        if conn.is_mutex_poisoned() {
            return Err(RecycleError::Message(
                "Mutex is poisoned. Connection is considered unusable.".into(),
            ));
        }
        let recycle_count = self.recycle_count.fetch_add(1, Ordering::Relaxed);
        let n: usize = conn
            .interact(move |conn| conn.query_row("SELECT $1", [recycle_count], |row| row.get(0)))
            .await
            .map_err(|e| RecycleError::Message(format!("{}", e)))??;
        if n == recycle_count {
            Ok(())
        } else {
            Err(RecycleError::Message("Recycle count mismatch".to_string()))
        }
    }
}


/// Initialize the SQLite asynchronous connection pool
pub async fn init_sqlite_async_pool(config: crate::config::AkitaConfig) -> Result<SqliteAsyncPool, AkitaError> {
    let manager = SqliteAsyncConnectionManager::new(&config)?;
    // Check the Tokio context
    let _handle = get_tokio_context()?;
    let pool_config = deadpool::managed::PoolConfig {
        max_size: config.get_max_size() as usize,
        timeouts: deadpool::managed::Timeouts {
            wait: Some(config.get_connection_timeout()),
            create: Some(config.get_connection_timeout()),
            recycle: Some(config.get_idle_timeout()),
        },
        ..Default::default()
    };

    let pool = Pool::builder(manager).runtime(Runtime::Tokio1).config(pool_config).build()?;

    // Testing connections
    let conn: SqliteAsyncConnection = pool.get().await
        .map_err(|e| AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e)))?;
    conn
        .interact(|conn| {
            conn.query_row("SELECT 1", [], |_| Ok(())).unwrap_or_default();
        })
        .await?;

    tracing::info!("SQLite async connection pool initialized successfully");

    Ok(pool)
}