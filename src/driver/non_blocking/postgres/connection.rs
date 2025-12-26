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
use tokio::runtime::Handle;
use tokio_postgres::{Client, Config, NoTls};
use crate::config::AkitaConfig;
use crate::driver::non_blocking::get_tokio_context;
use crate::errors::AkitaError;

/// PostgreSQL Asynchronous connection type
pub type PostgresAsyncConnection = Object<PostgresAsyncConnectionManager>;
/// PostgreSQL Asynchronous connection pool type
pub type PostgresAsyncPool = Pool<PostgresAsyncConnectionManager>;

/// PostgreSQL Asynchronous connection manager
#[derive(Clone, Debug)]
pub struct PostgresAsyncConnectionManager {
    config: Config,
}

impl PostgresAsyncConnectionManager {
    pub fn new(config: &AkitaConfig) -> Result<Self, AkitaError> {
        let pg_config = config.into();
        Ok(Self {
            config: pg_config,
        })
    }
}

#[async_trait]
impl deadpool::managed::Manager for PostgresAsyncConnectionManager {
    type Type = Client;
    type Error = tokio_postgres::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let (client, connection) = self.config.connect(NoTls).await?;

        // Starting the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(client)
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        // Perform a simple query to check the connection
        conn.simple_query("SELECT 1").await?;
        Ok(())
    }
}


/// Initialize the PostgreSQL asynchronous connection pool
pub async fn init_postgres_async_pool(config: AkitaConfig) -> Result<PostgresAsyncPool, AkitaError> {
    let manager = PostgresAsyncConnectionManager::new(&config)?;
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
    let conn: PostgresAsyncConnection = pool.get().await
        .map_err(|e| AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e)))?;
    conn.simple_query("SELECT 1").await
        .map_err(|e| AkitaError::DatabaseError(format!("PostgreSQL async connection test failed: {}", e)))?;

    tracing::info!("PostgreSQL async connection pool initialized successfully");

    Ok(pool)
}


impl From<AkitaConfig> for tokio_postgres::Config {
    fn from(v: AkitaConfig) -> Self {
        tokio_postgres::Config::from(&v)
    }
}
impl From<&AkitaConfig> for tokio_postgres::Config {
    fn from(config: &AkitaConfig) -> Self {
        let mut cfg = tokio_postgres::Config::new();
        if let Ok(Some(host)) = config.get_hostname() {
            cfg.host(&host);
        }
        if let Ok(Some(port)) = config.get_port() {
            cfg.port(port);
        }
        if let Ok(Some(db)) = config.get_database() {
            cfg.dbname(&db);
        }
        if let Ok(Some(username)) = config.get_username() {
            cfg.user(&username);
        }
        if let Ok(Some(password)) = config.get_password() {
            cfg.password(&password);
        }
        cfg.connect_timeout(config.get_connection_timeout());
        cfg
    }
}