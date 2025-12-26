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
use tiberius::ToSql;
use crate::errors::{AkitaError, Result};
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

/// Synchronous SQL Server client wrapper
pub struct SyncMssqlClient {
    inner: tokio::sync::Mutex<tiberius::Client<Compat<tokio::net::TcpStream>>>,
    runtime: Runtime,
}

impl SyncMssqlClient {
    /// Create a new sync client
    pub fn connect(config: tiberius::Config) -> Result<Self> {
        // Create the Tokio runtime
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| AkitaError::TokioError(format!("Failed to create runtime: {}", e)))?;

        // Connect to the database asynchronously
        let client = runtime.block_on(async {
            // Establishing a TCP connection
            let tcp = tokio::net::TcpStream::connect(config.get_addr())
                .await
                .map_err(|e| AkitaError::TokioError(format!("TCP connection failed: {}", e)))?;

            tcp.set_nodelay(true).map_err(|e| {
                AkitaError::TokioError(format!("Failed to set TCP no delay: {}", e))
            })?;

            // Convert tokio::net::TcpStream to a futures-compatible type
            let compat_tcp = tcp.compat();

            tiberius::Client::connect(config, compat_tcp)
                .await
                .map_err(|e| AkitaError::DatabaseError(format!("Database connection failed: {}", e)))
        })?;

        Ok(Self {
            inner: tokio::sync::Mutex::new(client),
            runtime,
        })
    }

    /// Executing queries
    pub fn query(&self, sql: &str, params: &[& dyn ToSql]) -> Result<Vec<tiberius::Row>> {
        let mut client = self.runtime.block_on(async {
            self.inner.lock().await
        });

        self.runtime.block_on(async {
            let stream = client.query(sql, params).await.map_err(|e| {
                AkitaError::DatabaseError(format!("Query failed: {}", e))
            })?;
            let rows: Vec<tiberius::Row> = stream.into_first_result().await.map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get result: {}", e))
            })?;

            Ok(rows)
        })
    }
    
    pub fn simple_query(&self, sql: &str) -> Result<Vec<tiberius::Row>> {
        let mut client = self.runtime.block_on(async {
            self.inner.lock().await
        });

        self.runtime.block_on(async {
            let stream = client.simple_query(sql).await.map_err(|e| {
                AkitaError::DatabaseError(format!("Simple Query failed: {}", e))
            })?;

            let rows: Vec<tiberius::Row> = stream.into_first_result().await.map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get result: {}", e))
            })?;

            Ok(rows)
        })
    }

    /// Perform updates (synchronization)
    pub fn execute(&self, sql: &str, params: &[& dyn ToSql]) -> Result<u64> {
        let mut client = self.runtime.block_on(async {
            self.inner.lock().await
        });

        self.runtime.block_on(async {
            let result = client.execute(sql, params).await.map_err(|e| {
                AkitaError::DatabaseError(format!("Execute failed: {}", e))
            })?;

            Ok(result.total())
        })
    }
}