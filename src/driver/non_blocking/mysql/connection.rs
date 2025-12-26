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
use deadpool::managed::{Pool, Object, Metrics, RecycleResult};
use deadpool::Runtime;
use mysql_async::{Conn, Opts};
use mysql_async::prelude::Queryable;
use tokio::runtime::Handle;
use crate::driver::non_blocking::get_tokio_context;
use crate::errors::AkitaError;

/// MySQL Asynchronous connection pool type
pub type MysqlAsyncPool = Pool<MysqlAsyncConnectionManager>;
/// MySQL Asynchronous connection type
pub type MysqlAsyncConnection = Object<MysqlAsyncConnectionManager>;

/// MySQL Asynchronous connection manager
#[derive(Clone)]
pub struct MysqlAsyncConnectionManager {
    opts: Opts,
    config: crate::config::AkitaConfig,
}

impl MysqlAsyncConnectionManager {
    /// Create a new connection manager
    pub fn new(config: &crate::config::AkitaConfig) -> Result<Self, AkitaError> {
        let connection_string = config.get_connection_string()?;

        let opts = Opts::from_url(&connection_string)
            .map_err(|e| AkitaError::DatabaseError(format!("Invalid MySQL URL: {}", e)))?;

        Ok(Self {
            opts,
            config: config.clone(),
        })
    }
}

#[async_trait]
impl deadpool::managed::Manager for MysqlAsyncConnectionManager {
    type Type = Conn;
    type Error = mysql_async::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let mut conn = Conn::new(self.opts.clone()).await?;

        // Setting connection options
        let _ = conn.query_drop(format!("SET SESSION max_execution_time = {}", self.config.get_connection_timeout().as_millis())).await;
        Ok(conn)
    }

    async fn recycle(&self, obj: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        // Ping Check that the connection is valid
        obj.ping().await?;
        Ok(())
    }
}

/// Initialize MySQL asynchronous connection pool
pub async fn init_mysql_async_pool(config: crate::config::AkitaConfig) -> Result<MysqlAsyncPool, AkitaError> {
    let manager = MysqlAsyncConnectionManager::new(&config)?;
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
    let mut conn: MysqlAsyncConnection = pool.get().await
        .map_err(|e| AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e)))?;
    conn.query_drop("SELECT 1").await
        .map_err(|e| AkitaError::DatabaseError(format!("MySQL async connection test failed: {}", e)))?;

    tracing::info!("MySQL async connection pool initialized successfully");

    Ok(pool)
}