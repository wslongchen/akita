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
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;
use deadpool::managed::{Metrics, Object, Pool, RecycleError, RecycleResult};
use deadpool::Runtime;
use deadpool_sync::SyncWrapper;
use oracle::{Connection, Connector, ErrorKind};
use tokio::runtime::Handle;
use tokio::task;
use crate::config::AkitaConfig;
use crate::driver::DriverType;
use crate::driver::non_blocking::get_tokio_context;
use crate::errors::AkitaError;

/// Oracle Asynchronous connection pool type
pub type OracleAsyncPool = Pool<OracleAsyncConnectionManager>;
/// Oracle Asynchronous connection type
pub type OracleAsyncConnection = Object<OracleAsyncConnectionManager>;

/// Oracle Asynchronous connection manager
pub struct OracleAsyncConnectionManager {
    connector: Connector,
    runtime: Runtime,
    recycle_count: AtomicUsize,
}

impl fmt::Debug for OracleAsyncConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("OracleAsyncConnectionManager");
        let _ = builder.field("connector", &self.connector);
        let _ = builder.field("runtime", &self.runtime);
        let _ = builder.field("recycle_count", &self.recycle_count);
        builder.finish()
    }
}

impl OracleAsyncConnectionManager {
    pub fn new(cfg: &crate::config::AkitaConfig) -> Result<Self, AkitaError> {
        let connector = cfg.try_into()?;
        Ok(Self {
            connector,
            runtime: Runtime::Tokio1,
            recycle_count: AtomicUsize::new(0),
        })
    }
}


#[async_trait]
impl deadpool::managed::Manager for OracleAsyncConnectionManager {
    type Type = SyncWrapper<Connection>;
    type Error = oracle::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        // A connection is created in a blocked thread
        let conn = self.connector.connect();
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
        let n: Option<usize> = conn
            .interact(move |conn| conn.query_row("SELECT 1 FROM DUAL", &[&recycle_count]))
            .await.map(|res| res.iter().next().map(|row| row.get(0).unwrap_or(0)))
            .map_err(|e| RecycleError::Message(format!("{}", e)))?;
        if n.unwrap_or_default() == recycle_count {
            Ok(())
        } else {
            Err(RecycleError::Message("Recycle count mismatch".to_string()))
        }
    }
}

/// Initialize the Oracle asynchronous connection pool
pub async fn init_oracle_async_pool(config: crate::config::AkitaConfig) -> Result<OracleAsyncPool, AkitaError> {
    let manager = OracleAsyncConnectionManager::new(&config)?;
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
    let conn: OracleAsyncConnection = pool.get().await
        .map_err(|e| AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e)))?;
    conn
        .interact(|conn| {
            conn.query_row_as::<i32>("SELECT 1 FROM DUAL", &[]).unwrap_or_default();
        })
        .await?;
    
    tracing::info!("Oracle async connection pool initialized successfully");

    Ok(pool)
}


#[cfg(all(
    any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "oracle-async",
        feature = "mssql-async"
    ),
    not(any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "mssql-sync",
        feature = "oracle-sync"
    ))
))]
impl TryFrom<AkitaConfig> for Connector {
    type Error = AkitaError;

    fn try_from(v: AkitaConfig) -> Result<Self, Self::Error> {
        Connector::try_from(&v)
    }
}

#[cfg(all(
    any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "oracle-async",
        feature = "mssql-async"
    ),
    not(any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "mssql-sync",
        feature = "oracle-sync"
    ))
))]
impl TryFrom<&AkitaConfig> for Connector {
    type Error = AkitaError;

    fn try_from(cfg: &AkitaConfig) -> Result<Self, Self::Error> {
        if cfg.get_platform()? != DriverType::Oracle {
            return Err(AkitaError::DatabaseError(
                "Database type mismatch: expected Oracle".to_string()
            ));
        }

        // Use smart acquisition methods
        let username = cfg.get_username()?.ok_or_else(|| {
            AkitaError::DatabaseError("Oracle username is required".to_string())
        })?;

        let password = cfg.get_password()?.ok_or_else(|| {
            AkitaError::DatabaseError("Oracle password is required".to_string())
        })?;

        // Building connection strings
        let mut connect_string = String::new();

        if let Some(host) = cfg.get_hostname()? {
            connect_string.push_str(&host);
            if let Some(port) = cfg.get_port()? {
                connect_string.push_str(&format!(":{}", port));
            }
            if let Some(service) = cfg.get_database()? {
                connect_string.push_str(&format!("/{}", service));
            }
        } else {
            // If the host is empty, it may be the full Easy Connect format
            if let Some(url) = cfg.get_url() {
                if let Some(at_index) = url.find('@') {
                    connect_string = url[at_index + 1..].to_string();
                } else {
                    return Err(AkitaError::DatabaseError(
                        "Oracle connection string is required".to_string()
                    ));
                }
            } else {
                return Err(AkitaError::DatabaseError(
                    "Oracle connection string is required".to_string()
                ));
            }
        }


        Ok(Connector::new(username, password, connect_string))
    }
}

