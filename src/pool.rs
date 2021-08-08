use std::convert::TryFrom;

use log::*;

use crate::{AkitaError, database::{DatabasePlatform, Platform}, manager::{AkitaEntityManager, AkitaManager}};
use crate::mysql::{self, MysqlDatabase, MysqlConnectionManager};

#[allow(unused)]
#[derive(Clone)]
pub struct Pool(PlatformPool, AkitaConfig);

#[derive(Clone)]
pub struct AkitaConfig {
    pub max_size: Option<usize>,
    pub url: &'static str,
    pub log_level: Option<LogLevel>, 
}

impl AkitaConfig {
    pub fn default() -> Self {
        AkitaConfig {
            max_size: None,
            url: "",
            log_level: LogLevel::Info.into(),
        }
    }

    pub fn url(&mut self, url: &'static str) -> &mut Self {
        self.url = url;
        self
    }

    pub fn max_size(&mut self, max_size: usize) -> &mut Self {
        self.max_size = max_size.into();
        self
    }

    pub fn log_level(&mut self, level: LogLevel) -> &mut Self {
        self.log_level = level.into();
        self
    }
}

#[derive(Clone)]
pub enum LogLevel {
    Debug,
    Info,
    Error
}

#[allow(unused)]
#[derive(Clone)]
pub enum PlatformPool {
    MysqlPool(r2d2::Pool<MysqlConnectionManager>),
}

#[allow(unused)]
pub enum PooledConnection {
    PooledMysql(Box<r2d2::PooledConnection<MysqlConnectionManager>>),
}

#[allow(unused)]
impl Pool {
    pub fn new(cfg: AkitaConfig) -> Result<Self, AkitaError>  {
        let database_url = cfg.url;
        let platform: Result<Platform, _> = TryFrom::try_from(database_url);
        match platform {
            Ok(platform) => match platform {
                Platform::Mysql => {
                    let pool_mysql = mysql::init_pool(database_url, 4)?;
                    Ok(Pool(PlatformPool::MysqlPool(pool_mysql), cfg))
                }
                Platform::Unsupported(scheme) => {
                    info!("unsupported");
                    Err(AkitaError::UnknownDatabase(scheme))
                }
            },
            Err(e) => Err(AkitaError::UrlParseError(e.to_string())),
        }
    }

    fn get_pool(&self) -> Result<&PlatformPool, AkitaError> {
        Ok(&self.0)
    }

    /// get a usable database connection from
    pub fn connect(&mut self) -> Result<PooledConnection, AkitaError> {
        match self.0 {
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
        }
    }

    /// returns a akita manager which provides api which data is already converted into
    /// Data, Rows and Value
    pub fn akita_manager(&mut self) -> Result<AkitaManager, AkitaError> {
        let db = self.database()?;
        let cfg = self.1.clone();
        Ok(AkitaManager(db, cfg))
    }

    fn get_pool_mut(&mut self) -> Result<&PlatformPool, AkitaError> {
        Ok(&self.0)
    }

    /// get a usable database connection from
    pub fn connect_mut(&mut self) -> Result<PooledConnection, AkitaError> {
        let pool = self.get_pool_mut()?;
        match *pool {
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
        }
    }

    /// get a database instance with a connection, ready to send sql statements
    pub fn database(&mut self) -> Result<DatabasePlatform, AkitaError> {
        let pooled_conn = self.connect_mut()?;
        match pooled_conn {
            PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase(*pooled_mysql)))),
        }
    }

    /// return an entity manager which provides a higher level api
    pub fn entity_manager(&mut self) -> Result<AkitaEntityManager, AkitaError> {
        let db = self.database()?;
        let cfg = self.1.clone();
        Ok(AkitaEntityManager(db, cfg))
    }
}