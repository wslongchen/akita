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

use akita_core::cfg_if;
use crate::errors::Result;
use crate::{AkitaError, database::{DatabasePlatform, Platform}, manager::{AkitaEntityManager}};
use crate::config::AkitaConfig;

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::{mysql::{self as mmysql, MysqlConnectionManager, MysqlDatabase}};
}}

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::{self, SqliteConnectionManager, SqliteDatabase};
}}

#[allow(unused)]
#[derive(Clone)]
pub struct Pool {
    _inner: PlatformPool,
}


#[allow(unused)]
#[derive(Clone)]
pub enum PlatformPool {
    #[cfg(feature = "akita-mysql")]
    MysqlPool(r2d2::Pool<MysqlConnectionManager>),
    #[cfg(feature = "akita-sqlite")]
    SqlitePool(r2d2::Pool<SqliteConnectionManager>),
}

#[allow(unused)]
pub enum PooledConnection {
    #[cfg(feature = "akita-mysql")]
    PooledMysql(r2d2::PooledConnection<MysqlConnectionManager>),
    #[cfg(feature = "akita-sqlite")]
    PooledSqlite(r2d2::PooledConnection<SqliteConnectionManager>),
}

#[allow(unused)]
impl PlatformPool {
    /// get a usable database connection from
    pub fn acquire(&self) -> Result<PooledConnection> {
        match self {
            #[cfg(feature = "akita-mysql")]
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(pooled_conn)),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
            #[cfg(feature = "akita-sqlite")]
            PlatformPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledSqlite(pooled_conn)),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
            _=> unimplemented!()
        }
    }

    pub fn database(&self) -> Result<DatabasePlatform> {
        let conn = self.acquire()?;
        match conn {
            #[cfg(feature = "akita-mysql")]
            PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase::new(pooled_mysql)))),
            #[cfg(feature = "akita-sqlite")]
            PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DatabasePlatform::Sqlite(Box::new(SqliteDatabase::new(pooled_sqlite)))),
        }
    }
}

#[allow(unused)]
impl Pool {
    pub fn new(mut cfg: AkitaConfig) -> Result<Self>  {
        let platform = cfg.platform().clone();
        match platform {
            #[cfg(feature = "akita-mysql")]
            Platform::Mysql => {
                let pool_mysql = mmysql::init_pool(cfg)?;
                Ok(Pool{ _inner: PlatformPool::MysqlPool(pool_mysql) })
            }
            #[cfg(feature = "akita-sqlite")]
            Platform::Sqlite(ref path) => {
                cfg = cfg.set_url(path.to_string());
                let pool_sqlite = sqlite::init_pool(cfg)?;
                Ok(Pool{ _inner: PlatformPool::SqlitePool(pool_sqlite)})
            }
            Platform::Unsupported(scheme) => {
                Err(AkitaError::UnknownDatabase(scheme.to_string()))
            }
        }
    }

    /// get a usable database connection from
    pub fn acquire(&self) -> Result<PooledConnection> {
        self._inner.acquire()
    }

    pub fn database(&self) -> Result<DatabasePlatform> {
        self._inner.database()
    }

    pub fn pool(self) -> PlatformPool {
        self._inner
    }

    /// get a usable database connection from
    pub fn connect(&self) -> Result<PooledConnection> {
        match self._inner {
            #[cfg(feature = "akita-mysql")]
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(pooled_conn)),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
            #[cfg(feature = "akita-sqlite")]
            PlatformPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledSqlite(pooled_conn)),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
        }
    }

    /// return an entity manager which provides a higher level api
    pub fn entity_manager(self) -> Result<AkitaEntityManager> {
        Ok(AkitaEntityManager::new(self._inner))
    }
}