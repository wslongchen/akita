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

use crate::config::AkitaConfig;
use crate::driver;
use crate::driver::blocking::{DbDriver};
use crate::driver::DriverType;
use crate::errors::{AkitaError, Result};
use crate::pool::{PoolStatus};
use akita_core::cfg_if;

cfg_if! {if #[cfg(feature = "mysql-sync")]{
    use crate::driver::blocking::{mysql::{self as mmysql, MySQL}};
    use crate::driver::blocking::mysql::{MysqlPool, MysqlConnection};
}}

cfg_if! {if #[cfg(feature = "sqlite-sync")]{
    use crate::driver::blocking::sqlite::{self as sqlite, Sqlite};
    use crate::driver::blocking::sqlite::{SqlitePool, SqliteConnection};
}}


cfg_if! {if #[cfg(feature = "oracle-sync")]{
    use crate::driver::blocking::{oracle::{Oracle, OraclePool}};
    use crate::driver::blocking::oracle::OracleConnection;
}}

cfg_if! {if #[cfg(feature = "postgres-sync")]{
    use crate::driver::blocking::postgres::{Postgres, PostgresPool};
    use crate::driver::blocking::postgres::PostgresConnection;
}}

cfg_if! {if #[cfg(feature = "mssql-sync")]{
    use crate::driver::blocking::mssql::{Mssql, MssqlPool};
    use crate::driver::blocking::mssql::MssqlConnection;
}}

pub trait SyncPool {
    /// Get connections from the connection pool
    fn acquire(&self) -> crate::errors::Result<PooledConnection>;

    /// Get the database driver
    fn database(&self) -> crate::errors::Result<DbDriver>;

    /// Get the connection pool status
    fn status(&self) -> PoolStatus;

    /// Close connection pooling
    fn close(&self);
}

#[allow(unused)]
#[derive(Clone)]
pub struct DBPoolWrapper {
    _inner: DBPool,
    _cfg: AkitaConfig,
}


#[allow(unused)]
#[derive(Clone)]
pub enum DBPool {
    #[cfg(feature = "mysql-sync")]
    MysqlPool(MysqlPool),
    #[cfg(feature = "sqlite-sync")]
    SqlitePool(SqlitePool),
    #[cfg(feature = "oracle-sync")]
    OraclePool(OraclePool),
    #[cfg(feature = "mssql-sync")]
    MssqlPool(MssqlPool),
    #[cfg(feature = "postgres-sync")]
    PostgresPool(PostgresPool),
}

#[allow(unused)]
pub enum PooledConnection {
    #[cfg(feature = "mysql-sync")]
    PooledMysql(MysqlConnection),
    #[cfg(feature = "sqlite-sync")]
    PooledSqlite(SqliteConnection),
    #[cfg(feature = "mssql-sync")]
    PooledMssql(MssqlConnection),
    #[cfg(feature = "oracle-sync")]
    PooledOracle(OracleConnection),
    #[cfg(feature = "postgres-sync")]
    PooledPostgres(PostgresConnection),
}

#[allow(unused)]
impl SyncPool for DBPool {
    /// get a usable database connection from
    fn acquire(&self) -> Result<PooledConnection> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DBPool::MysqlPool(ref pool_mysql) => {
                let mut pooled_conn = pool_mysql.get().map_err(|e| AkitaError::R2D2Error(e))?;
                // Verify that the connection is still valid
                if !pooled_conn.ping().is_ok() {
                    return Err(AkitaError::ConnectionValidError);
                }
                Ok(PooledConnection::PooledMysql(pooled_conn))
            }
            #[cfg(feature = "sqlite-sync")]
            DBPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get()
                    .map_err(|e| AkitaError::R2D2Error(e))?;
                Ok(PooledConnection::PooledSqlite(pooled_conn))
            }
            #[cfg(feature = "postgres-sync")]
            DBPool::PostgresPool(ref pool_postgres) => {
                let pooled_conn = pool_postgres.get()
                    .map_err(|e| AkitaError::R2D2Error(e))?;
                Ok(PooledConnection::PooledPostgres(pooled_conn))
            }
            #[cfg(feature = "oracle-sync")]
            DBPool::OraclePool(ref pool_oracle) => {
                let pooled_conn = pool_oracle.get()
                    .map_err(|e| AkitaError::R2D2Error(e))?;
                Ok(PooledConnection::PooledOracle(pooled_conn))
            }
            #[cfg(feature = "mssql-sync")]
            DBPool::MssqlPool(ref pool_mssql) => {
                let pooled_conn = pool_mssql.get()
                    .map_err(|e| AkitaError::R2D2Error(e))?;
                Ok(PooledConnection::PooledMssql(pooled_conn))
            }
        }
    }

    fn database(&self) -> Result<DbDriver> {
        let conn = self.acquire()?;
        match conn {
            #[cfg(feature = "mysql-sync")]
            PooledConnection::PooledMysql(pooled_mysql) => Ok(DbDriver::MysqlDriver(Box::new(MySQL::new(pooled_mysql)))),
            #[cfg(feature = "sqlite-sync")]
            PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DbDriver::SqliteDriver(Box::new(Sqlite::new(pooled_sqlite)))),
            #[cfg(feature = "oracle-sync")]
            PooledConnection::PooledOracle(pooled_oracle) => Ok(DbDriver::OracleDriver(Box::new(Oracle::new(pooled_oracle)))),
            #[cfg(feature = "postgres-sync")]
            PooledConnection::PooledPostgres(pooled_postgres) => Ok(DbDriver::PostgresDriver(Box::new(Postgres::new(pooled_postgres)))),
            #[cfg(feature = "mssql-sync")]
            PooledConnection::PooledMssql(pooled_mssql) => Ok(DbDriver::MssqlDriver(Box::new(Mssql::new(pooled_mssql)))),
        }
    }

    fn status(&self) -> PoolStatus {
        match self {
            #[cfg(feature = "mysql-sync")]
            DBPool::MysqlPool(pool) => {
                let state = pool.state();
                let size = state.connections as usize;
                let available= state.idle_connections as usize;
                PoolStatus { size, available }
            }
            #[cfg(feature = "postgres-sync")]
            DBPool::PostgresPool(pool) => {
                let state = pool.state();
                let size = state.connections as usize;
                let available= state.idle_connections as usize;
                PoolStatus { size, available }
            }
            #[cfg(feature = "oracle-sync")]
            DBPool::OraclePool(pool) => {
                let state = pool.state();
                let size = state.connections as usize;
                let available= state.idle_connections as usize;
                PoolStatus { size, available }
            }
            #[cfg(feature = "sqlite-sync")]
            DBPool::SqlitePool(pool) => {
                let state = pool.state();
                let size = state.connections as usize;
                let available= state.idle_connections as usize;
                PoolStatus { size, available }
            }
            #[cfg(feature = "mssql-sync")]
            DBPool::MssqlPool(pool) => {
                let state = pool.state();
                let size = state.connections as usize;
                let available= state.idle_connections as usize;
                PoolStatus { size, available }
            }
        }
    }

    fn close(&self) {
        // No need to close
    }
}

#[allow(unused)]
impl DBPoolWrapper {
    pub fn new(mut cfg: AkitaConfig) -> Result<Self>  {
        let driver_type = cfg.get_platform()?;
        match driver_type {
            #[cfg(feature = "mysql-sync")]
            DriverType::MySQL => {
                let pool_mysql = mmysql::init_mysql_pool(cfg.clone())?;
                Ok(DBPoolWrapper { _inner: DBPool::MysqlPool(pool_mysql), _cfg: cfg })
            }
            #[cfg(feature = "sqlite-sync")]
            DriverType::Sqlite => {
                let pool_sqlite = sqlite::init_sqlite_pool(cfg.clone())?;
                Ok(DBPoolWrapper { _inner: DBPool::SqlitePool(pool_sqlite), _cfg: cfg })
            }
            #[cfg(feature = "oracle-sync")]
            DriverType::Oracle => {
                let pool_oracle = driver::blocking::oracle::init_oracle_pool(cfg.clone())?;
                Ok(DBPoolWrapper { _inner: DBPool::OraclePool(pool_oracle), _cfg: cfg })
            }
            #[cfg(feature = "postgres-sync")]
            DriverType::Postgres => {
                let pool_postgres = driver::blocking::postgres::init_postgres_pool(cfg.clone())?;
                Ok(DBPoolWrapper { _inner: DBPool::PostgresPool(pool_postgres), _cfg: cfg })
            }
            #[cfg(feature = "mssql-sync")]
            DriverType::Mssql => {
                let pool_mssql = driver::blocking::mssql::init_mssql_pool(cfg.clone())?;
                Ok(DBPoolWrapper { _inner: DBPool::MssqlPool(pool_mssql), _cfg: cfg })
            }
            _ => {
                Err(AkitaError::DatabaseError("Unknown".to_string()))
            }
        }
    }

    /// get a usable database connection from
    pub fn acquire(&self) -> Result<PooledConnection> {
        self._inner.acquire()
    }

    pub fn database(&self) -> Result<DbDriver> {
        self._inner.database()
    }
    
    pub fn config(&self) -> &AkitaConfig {
        &self._cfg
    }

    pub fn pool(self) -> DBPool {
        self._inner
    }

    /// get a usable database connection from
    pub fn connect(&self) -> Result<PooledConnection> {
        match self._inner {
            #[cfg(feature = "mysql-sync")]
            DBPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(pooled_conn)),
                    Err(e) => Err(AkitaError::R2D2Error(e)),
                }
            }
            #[cfg(feature = "sqlite-sync")]
            DBPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledSqlite(pooled_conn)),
                    Err(e) => Err(AkitaError::R2D2Error(e)),
                }
            }
            #[cfg(feature = "oracle-sync")]
            DBPool::OraclePool(ref pool_oracle) => {
                let pooled_conn = pool_oracle.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledOracle(pooled_conn)),
                    Err(e) => Err(AkitaError::R2D2Error(e)),
                }
            }
            #[cfg(feature = "mssql-sync")]
            DBPool::MssqlPool(ref pool_mssql) => {
                let pooled_conn = pool_mssql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMssql(pooled_conn)),
                    Err(e) => Err(AkitaError::R2D2Error(e)),
                }
            }
            #[cfg(feature = "postgres-sync")]
            DBPool::PostgresPool(ref pool_postgres) => {
                let pooled_conn = pool_postgres.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledPostgres(pooled_conn)),
                    Err(e) => Err(AkitaError::R2D2Error(e)),
                }
            }
            
        }
    }
}