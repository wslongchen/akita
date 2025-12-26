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

use crate::driver::non_blocking::AsyncDbDriver;
use crate::prelude::*;
use async_trait::async_trait;
use crate::driver::DriverType;
use crate::pool::{PoolStatus};

cfg_if! {
    if #[cfg(feature = "mysql-async")] {
        use crate::driver::non_blocking::mysql::*;
    }
}

cfg_if! {
    if #[cfg(feature = "postgres-async")] {
        use crate::driver::non_blocking::postgres::*;
    }
}

cfg_if! {
    if #[cfg(feature = "oracle-async")] {
        use crate::driver::non_blocking::oracle::*;
    }
}

cfg_if! {
    if #[cfg(feature = "sqlite-async")] {
        use crate::driver::non_blocking::sqlite::*;
    }
}

cfg_if! {
    if #[cfg(feature = "mssql-async")] {
        use crate::driver::non_blocking::mssql::*;
    }
}

#[async_trait]
pub trait AsyncPool {
    /// Get connections from the connection pool
    async fn acquire(&self) -> crate::errors::Result<AsyncPooledConnection>;

    /// Get the database driver
    async fn database(&self) -> crate::errors::Result<AsyncDbDriver>;

    /// Get the connection pool status
    async fn status(&self) -> PoolStatus;

    /// Close connection pooling
    async fn close(&self);
}



#[allow(unused)]
#[derive(Clone)]
pub struct AsyncDBPoolWrapper {
    _inner: AsyncDBPool,
}

/// 异步数据库连接池枚举
#[derive(Clone)]
pub enum AsyncDBPool {
    #[cfg(feature = "mysql-async")]
    MysqlAsyncPool(MysqlAsyncPool),
    #[cfg(feature = "postgres-async")]
    PostgresAsyncPool(PostgresAsyncPool),
    #[cfg(feature = "oracle-async")]
    OracleAsyncPool(OracleAsyncPool),
    #[cfg(feature = "sqlite-async")]
    SqliteAsyncPool(SqliteAsyncPool),
    #[cfg(feature = "mssql-async")]
    MssqlAsyncPool(MssqlAsyncPool),
}

/// 异步数据库连接枚举
pub enum AsyncPooledConnection {
    #[cfg(feature = "mysql-async")]
    PooledMysqlAsync(MysqlAsyncConnection),
    #[cfg(feature = "postgres-async")]
    PooledPostgresAsync(PostgresAsyncConnection),
    #[cfg(feature = "oracle-async")]
    PooledOracleAsync(OracleAsyncConnection),
    #[cfg(feature = "sqlite-async")]
    PooledSqliteAsync(SqliteAsyncConnection),
    #[cfg(feature = "mssql-async")]
    PooledMssqlAsync(MssqlAsyncConnection),
}


#[async_trait]
impl AsyncPool for AsyncDBPool {
    async fn acquire(&self) -> Result<AsyncPooledConnection> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDBPool::MysqlAsyncPool(ref pool) => {
                let conn = pool.get().await
                    .map_err(|e| AkitaError::DatabaseError(format!("Failed to get async MySQL connection: {}", e)))?;
                Ok(AsyncPooledConnection::PooledMysqlAsync(conn))
            }
            #[cfg(feature = "postgres-async")]
            AsyncDBPool::PostgresAsyncPool(ref pool) => {
                let conn = pool.get().await
                    .map_err(|e| AkitaError::DatabaseError(format!("Failed to get async PostgreSQL connection: {}", e)))?;
                Ok(AsyncPooledConnection::PooledPostgresAsync(conn))
            }
            #[cfg(feature = "oracle-async")]
            AsyncDBPool::OracleAsyncPool(ref pool) => {
                let conn = pool.get().await
                    .map_err(|e| AkitaError::DatabaseError(format!("Failed to get async Oracle connection: {}", e)))?;
                Ok(AsyncPooledConnection::PooledOracleAsync(conn))
            }
            #[cfg(feature = "sqlite-async")]
            AsyncDBPool::SqliteAsyncPool(ref pool) => {
                let conn = pool.get().await
                    .map_err(|e| AkitaError::DatabaseError(format!("Failed to get async SQLite connection: {}", e)))?;
                Ok(AsyncPooledConnection::PooledSqliteAsync(conn))
            }
            #[cfg(feature = "mssql-async")]
            AsyncDBPool::MssqlAsyncPool(ref pool) => {
                let conn = pool.get().await
                    .map_err(|e| AkitaError::DatabaseError(format!("Failed to get async SQL Server connection: {}", e)))?;
                Ok(AsyncPooledConnection::PooledMssqlAsync(conn))
            }
        }
    }

    async fn database(&self) -> Result<AsyncDbDriver> {
        let conn = self.acquire().await?;
        match conn {
            #[cfg(feature = "mysql-async")]
            AsyncPooledConnection::PooledMysqlAsync(conn) => {
                Ok(AsyncDbDriver::MysqlAsyncDriver(Box::new(MySQLAsync::new(conn))))
            }
            #[cfg(feature = "postgres-async")]
            AsyncPooledConnection::PooledPostgresAsync(conn) => {
                Ok(AsyncDbDriver::PostgresAsyncDriver(Box::new(PostgresAsync::new(conn))))
            }
            #[cfg(feature = "oracle-async")]
            AsyncPooledConnection::PooledOracleAsync(conn) => {
                Ok(AsyncDbDriver::OracleAsyncDriver(Box::new(OracleAsync::new(conn))))
            }
            #[cfg(feature = "sqlite-async")]
            AsyncPooledConnection::PooledSqliteAsync(conn) => {
                Ok(AsyncDbDriver::SqliteAsyncDriver(Box::new(SqliteAsync::new(conn))))
            }
            #[cfg(feature = "mssql-async")]
            AsyncPooledConnection::PooledMssqlAsync(conn) => {
                Ok(AsyncDbDriver::MssqlAsyncDriver(Box::new(MssqlAsync::new(conn))))
            }
        }
    }

    async fn status(&self) -> PoolStatus {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDBPool::MysqlAsyncPool(pool) => {
                let size = pool.status().size;
                let available = pool.status().available;
                PoolStatus { size, available }
            }
            #[cfg(feature = "postgres-async")]
            AsyncDBPool::PostgresAsyncPool(pool) => {
                let size = pool.status().size;
                let available = pool.status().available;
                PoolStatus { size, available }
            }
            #[cfg(feature = "oracle-async")]
            AsyncDBPool::OracleAsyncPool(pool) => {
                let size = pool.status().size;
                let available = pool.status().available;
                PoolStatus { size, available }
            }
            #[cfg(feature = "sqlite-async")]
            AsyncDBPool::SqliteAsyncPool(pool) => {
                let size = pool.status().size;
                let available = pool.status().available;
                PoolStatus { size, available }
            }
            #[cfg(feature = "mssql-async")]
            AsyncDBPool::MssqlAsyncPool(pool) => {
                let size = pool.status().size;
                let available = pool.status().available;
                PoolStatus { size, available }
            }
        }
    }

    async fn close(&self) {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDBPool::MysqlAsyncPool(pool) => {
                pool.close();
            }
            #[cfg(feature = "postgres-async")]
            AsyncDBPool::PostgresAsyncPool(pool) => {
                pool.close();
            }
            #[cfg(feature = "oracle-async")]
            AsyncDBPool::OracleAsyncPool(pool) => {
                pool.close();
            }
            #[cfg(feature = "sqlite-async")]
            AsyncDBPool::SqliteAsyncPool(pool) => {
                pool.close();
            }
            #[cfg(feature = "mssql-async")]
            AsyncDBPool::MssqlAsyncPool(pool) => {
                pool.close();
            }
        }
    }
}



#[allow(unused)]
impl AsyncDBPoolWrapper {
    pub async fn new(mut cfg: AkitaConfig) -> Result<Self>  {
        let driver_type = cfg.get_platform()?;
        match driver_type {
            #[cfg(feature = "mysql-async")]
            DriverType::MySQL => {
                let pool_mysql = init_mysql_async_pool(cfg).await?;
                Ok(AsyncDBPoolWrapper { _inner: AsyncDBPool::MysqlAsyncPool(pool_mysql) })
            }
            #[cfg(feature = "sqlite-async")]
            DriverType::Sqlite => {
                let pool_sqlite = init_sqlite_async_pool(cfg).await?;
                Ok(AsyncDBPoolWrapper { _inner: AsyncDBPool::SqliteAsyncPool(pool_sqlite)})
            }
            #[cfg(feature = "oracle-async")]
            DriverType::Oracle => {
                let pool_oracle = init_oracle_async_pool(cfg).await?;
                Ok(AsyncDBPoolWrapper { _inner: AsyncDBPool::OracleAsyncPool(pool_oracle) })
            }
            #[cfg(feature = "postgres-async")]
            DriverType::Postgres => {
                let pool_postgres = init_postgres_async_pool(cfg).await?;
                Ok(AsyncDBPoolWrapper { _inner: AsyncDBPool::PostgresAsyncPool(pool_postgres) })
            }
            #[cfg(feature = "mssql-async")]
            DriverType::Mssql => {
                let pool_mssql = init_mssql_async_pool(cfg).await?;
                Ok(AsyncDBPoolWrapper { _inner: AsyncDBPool::MssqlAsyncPool(pool_mssql) })
            }
            _ => {
                Err(AkitaError::DatabaseError("Unknown".to_string()))
            }

        }
    }

    /// get a usable database connection from
    pub async fn acquire(&self) -> Result<AsyncPooledConnection> {
        self._inner.acquire().await
    }

    pub async fn database(&self) -> Result<AsyncDbDriver> {
        self._inner.database().await
    }

    pub fn pool(self) -> AsyncDBPool {
        self._inner
    }

    /// get a usable database connection from
    pub async fn connect(&self) -> Result<AsyncPooledConnection> {
        match self._inner {
            #[cfg(feature = "mysql-async")]
            AsyncDBPool::MysqlAsyncPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get().await;
                match pooled_conn {
                    Ok(pooled_conn) => Ok(AsyncPooledConnection::PooledMysqlAsync(pooled_conn)),
                    Err(e) => Err(AkitaError::DeadPoolError(e.to_string())),
                }
            }
            #[cfg(feature = "sqlite-async")]
            AsyncDBPool::SqliteAsyncPool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get().await;
                match pooled_conn {
                    Ok(pooled_conn) => Ok(AsyncPooledConnection::PooledSqliteAsync(pooled_conn)),
                    Err(e) => Err(AkitaError::DeadPoolError(e.to_string())),
                }
            }
            #[cfg(feature = "oracle-async")]
            AsyncDBPool::OracleAsyncPool(ref pool_oracle) => {
                let pooled_conn = pool_oracle.get().await;
                match pooled_conn {
                    Ok(pooled_conn) => Ok(AsyncPooledConnection::PooledOracleAsync(pooled_conn)),
                    Err(e) => Err(AkitaError::DeadPoolError(e.to_string())),
                }
            }
            #[cfg(feature = "mssql-async")]
            AsyncDBPool::MssqlAsyncPool(ref pool_mssql) => {
                let pooled_conn = pool_mssql.get().await;
                match pooled_conn {
                    Ok(pooled_conn) => Ok(AsyncPooledConnection::PooledMssqlAsync(pooled_conn)),
                    Err(e) => Err(AkitaError::DeadPoolError(e.to_string())),
                }
            }
            #[cfg(feature = "postgres-async")]
            AsyncDBPool::PostgresAsyncPool(ref pool_postgres) => {
                let pooled_conn = pool_postgres.get().await;
                match pooled_conn {
                    Ok(pooled_conn) => Ok(AsyncPooledConnection::PooledPostgresAsync(pooled_conn)),
                    Err(e) => Err(AkitaError::DeadPoolError(e.to_string())),
                }
            }

        }
    }
}