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
use async_trait::async_trait;
use deadpool::managed::{Metrics, Object, Pool, RecycleResult};
use deadpool::Runtime;
use tiberius::error::Error;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use tracing::log::trace;
use crate::config::AkitaConfig;
use crate::driver::DriverType;
use crate::driver::non_blocking::{get_tokio_context};
use crate::errors::AkitaError;

/// SQL Server Asynchronous connection pool type
pub type MssqlAsyncPool = Pool<MssqlAsyncConnectionManager>;
/// SQL Server Asynchronous connection type
pub type MssqlAsyncConnection = Object<MssqlAsyncConnectionManager>;

/// SQL Server Asynchronous connection manager
#[derive(Clone)]
pub struct MssqlAsyncConnectionManager {
    config: tiberius::Config,
}

impl MssqlAsyncConnectionManager {
    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        if cfg.get_platform()? != DriverType::Mssql {
            return Err(AkitaError::DatabaseError(
                "Database type mismatch: expected SQL Server".to_string()
            ));
        }

        let connection_string = cfg.get_connection_string()?;

        let config = tiberius::Config::from_ado_string(&connection_string)
            .map_err(|e| AkitaError::DatabaseError(format!("Invalid SQL Server connection string: {}", e)))?;

        Ok(Self { config })
    }
}

#[async_trait]
impl deadpool::managed::Manager for MssqlAsyncConnectionManager {
    type Type = tiberius::Client<Compat<tokio::net::TcpStream>>;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        // Establishing a TCP connection
        let tcp = tokio::net::TcpStream::connect(self.config.get_addr())
            .await?;

        tcp.set_nodelay(true)?;

        // Convert tokio::net::TcpStream to a futures-compatible type
        let compat_tcp = tcp.compat();

        tiberius::Client::connect(self.config.clone(), compat_tcp)
            .await
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        // Perform a simple query to check the connection
        conn.simple_query("SELECT 1").await?;
        Ok(())
    }
}

/// Initialize the SQL Server asynchronous connection pool
pub async fn init_mssql_async_pool(config: crate::config::AkitaConfig) -> Result<MssqlAsyncPool, AkitaError> {
    use tokio::runtime::Handle;

    // Check the Tokio context
    let _handle = get_tokio_context()?;
    let manager = MssqlAsyncConnectionManager::new(&config)?;

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
    let mut client: MssqlAsyncConnection = pool.get().await
        .map_err(|e| AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e)))?;

    client.simple_query("SELECT 1").await
        .map_err(|e| AkitaError::DatabaseError(format!("SQL Server async connection test failed: {}", e)))?;

    tracing::info!("SQL Server async connection pool initialized successfully");

    Ok(pool)
}