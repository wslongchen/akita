use std::{convert::TryFrom, time::Duration};
use akita_core::cfg_if;

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::{mysql::{self, MysqlConnectionManager, MysqlDatabase}};
}}

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::{self, SqliteConnectionManager, SqliteDatabase};
}}
use crate::{AkitaError, database::{DatabasePlatform, Platform}, manager::{AkitaEntityManager, AkitaManager}};

#[allow(unused)]
#[derive(Clone)]
pub struct Pool(PlatformPool, AkitaConfig);

#[derive(Clone, Debug)]
pub struct AkitaConfig {
    connection_timeout: Duration,
    min_idle: Option<u32>,
    max_size: u32,
    url: String,
    log_level: Option<LogLevel>, 
}

impl AkitaConfig {
    pub fn default() -> Self {
        AkitaConfig {
            max_size: 16,
            url: String::default(),
            log_level: None,
            connection_timeout: Duration::from_secs(6),
            min_idle: None,
        }
    }

    pub fn new(url: String) -> Self {
        AkitaConfig {
            max_size: 16,
            url,
            log_level: None,
            connection_timeout: Duration::from_secs(6),
            min_idle: None,
        }
    }

    pub fn set_url(mut self, url: String) -> Self {
        self.url = url;
        self
    }
    
    pub fn url(&self) -> String {
        self.url.to_owned()
    }

    pub fn set_max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn max_size(&self) -> u32 {
        self.max_size
    }
    
    pub fn set_connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }
    
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    pub fn set_min_idle(mut self, min_idle: Option<u32>) -> Self {
        self.min_idle = min_idle;
        self
    }

    pub fn min_idle(&self) -> Option<u32> {
        self.min_idle
    }

    pub fn set_log_level(mut self, level: LogLevel) -> Self {
        self.log_level = level.into();
        self
    }

    pub fn log_level(&self) -> Option<LogLevel> {
        self.log_level.to_owned()
    }
}

#[derive(Clone, Debug)]
pub enum LogLevel {
    Debug,
    Info,
    Error
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
    PooledMysql(Box<r2d2::PooledConnection<MysqlConnectionManager>>),
    #[cfg(feature = "akita-sqlite")]
    PooledSqlite(Box<r2d2::PooledConnection<SqliteConnectionManager>>),
}

#[allow(unused)]
impl Pool {
    pub fn new(mut cfg: AkitaConfig) -> Result<Self, AkitaError>  {
        let database_url = &cfg.url;
        let platform: Result<Platform, _> = TryFrom::try_from(database_url.as_str());
        match platform {
            Ok(platform) => match platform {
                #[cfg(feature = "akita-mysql")]
                Platform::Mysql => {
                    let pool_mysql = mysql::init_pool(&cfg)?;
                    Ok(Pool(PlatformPool::MysqlPool(pool_mysql), cfg))
                }
                #[cfg(feature = "akita-sqlite")]
                Platform::Sqlite(path) => {
                    cfg.url = path;
                    let pool_sqlite = sqlite::init_pool(&cfg)?;
                    Ok(Pool(PlatformPool::SqlitePool(pool_sqlite), cfg))
                }
                Platform::Unsupported(scheme) => {
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
            #[cfg(feature = "akita-mysql")]
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
            #[cfg(feature = "akita-sqlite")]
            PlatformPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledSqlite(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
        }
    }

    /// returns a akita manager which provides api which data is already converted into
    /// Data, Rows and Value
    pub fn akita_manager(&self) -> Result<AkitaManager, AkitaError> {
        let db = self.database()?;
        Ok(AkitaManager::new(db))
    }

    fn get_pool_mut(&self) -> Result<&PlatformPool, AkitaError> {
        Ok(&self.0)
    }

    /// get a usable database connection from
    pub fn connect_mut(&self) -> Result<PooledConnection, AkitaError> {
        let pool = self.get_pool_mut()?;
        match *pool {
            #[cfg(feature = "akita-mysql")]
            PlatformPool::MysqlPool(ref pool_mysql) => {
                let pooled_conn = pool_mysql.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledMysql(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
            #[cfg(feature = "akita-sqlite")]
            PlatformPool::SqlitePool(ref pool_sqlite) => {
                let pooled_conn = pool_sqlite.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConnection::PooledSqlite(Box::new(pooled_conn))),
                    Err(e) => Err(AkitaError::MySQLError(e.to_string())),
                }
            }
        }
    }

    /// get a database instance with a connection, ready to send sql statements
    pub fn database(&self) -> Result<DatabasePlatform, AkitaError> {
        let pooled_conn = self.connect_mut()?;
        match pooled_conn {
            #[cfg(feature = "akita-mysql")]
            PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase::new(*pooled_mysql, self.1.to_owned())))),
            #[cfg(feature = "akita-sqlite")]
            PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DatabasePlatform::Sqlite(Box::new(SqliteDatabase::new(*pooled_sqlite, self.1.to_owned())))),
        }
    }

    /// return an entity manager which provides a higher level api
    pub fn entity_manager(&self) -> Result<AkitaEntityManager, AkitaError> {
        let db = self.database()?;
        Ok(AkitaEntityManager::new(db))
    }
}