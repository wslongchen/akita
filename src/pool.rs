use std::{time::Duration};
use akita_core::cfg_if;
use url::Url;

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::{mysql::{self as mmysql, MysqlConnectionManager, MysqlDatabase}};
}}

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::{self, SqliteConnectionManager, SqliteDatabase};
}}
use crate::{AkitaError, database::{DatabasePlatform, Platform}, manager::{AkitaEntityManager}};

#[allow(unused)]
#[derive(Clone)]
pub struct Pool(PlatformPool, AkitaConfig);

#[derive(Clone, Debug)]
pub struct AkitaConfig {
    connection_timeout: Duration,
    min_idle: Option<u32>,
    max_size: u32,
    platform: Platform,
    url: Option<String>,
    password: Option<String>,
    db_name: Option<String>,
    port: Option<u16>,
    ip_or_hostname: Option<String>,
    username: Option<String>,
    log_level: Option<LogLevel>, 
}

#[cfg(feature = "akita-mysql")]
impl From<&AkitaConfig> for mysql::OptsBuilder {
    fn from(v: &AkitaConfig) -> Self {
        if let Some(url) = &v.url {
            let opts = mysql::Opts::from_url(url).unwrap();
            mysql::OptsBuilder::from_opts(opts)
        } else {
            mysql::OptsBuilder::new().db_name(v.db_name.to_owned()).user(v.username.to_owned())
                .db_name(v.db_name.to_owned())
                .ip_or_hostname(v.ip_or_hostname.to_owned()).pass(v.password.to_owned())
        }

    }
}

impl AkitaConfig {
    pub fn default() -> Self {
        AkitaConfig {
            max_size: 16,
            platform: Platform::Unsupported(String::default()),
            url: None,
            password: None,
            username: None,
            ip_or_hostname: None,
            db_name: None,
            log_level: None,
            connection_timeout: Duration::from_secs(6),
            min_idle: None,
            port: Some(3306)
        }
    }

    fn parse_url(mut self) -> Self {
        let url = Url::parse(&self.url.to_owned().unwrap_or_default());
        match url {
            Ok(url) => {
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "akita-mysql")]
                    "mysql" => {
                        self.platform = Platform::Mysql;
                        let host = url.host_str().unwrap_or_default();
                        self.ip_or_hostname = host.to_owned().into();

                    },
                    #[cfg(feature = "akita-sqlite")]
                    "sqlite" => {
                        let host = url.host_str().unwrap_or_default();
                        let path = url.path();
                        let path = if path == "/" { "" } else { path };
                        let db_file = format!("{}{}", host, path);
                        self.platform = Platform::Sqlite(db_file);
                    },
                    _ => {
                        self.platform = Platform::Unsupported(scheme.to_string());
                    },
                }
            }
            Err(_e) => {

            },
        }
        self
    }

    pub fn new(url: String) -> Self {
        let mut cfg = AkitaConfig {
            platform: Platform::Unsupported(String::default()),
            password: None,
            username: None,
            db_name: None,
            ip_or_hostname: None,
            max_size: 16,
            url: url.into(),
            log_level: None,
            connection_timeout: Duration::from_secs(6),
            min_idle: None,
            port: Some(3306)
        };
        cfg = cfg.parse_url();
        cfg
    }

    pub fn set_url(mut self, url: String) -> Self {
        self.url = url.into();
        self = self.parse_url();
        self
    }
    
    pub fn url(&self) -> String {
        self.url.to_owned().unwrap_or_default()
    }

    pub fn set_username(mut self, username: String) -> Self {
        self.username = username.into();
        self
    }

    pub fn username(&self) -> String {
        self.username.to_owned().unwrap_or_default()
    }

    pub fn set_password(mut self, password: String) -> Self {
        self.password = password.into();
        self
    }

    pub fn password(&self) -> String {
        self.password.to_owned().unwrap_or_default()
    }

    pub fn set_db_name(mut self, db_name: String) -> Self {
        self.db_name = db_name.into();
        self
    }

    pub fn db_name(&self) -> String {
        self.db_name.to_owned().unwrap_or_default()
    }

    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port.into();
        self
    }

    pub fn port(&self) -> u16 {
        self.port.to_owned().unwrap_or_default()
    }

    pub fn set_platform(mut self, platform: &str) -> Self {
        match platform {
            #[cfg(feature = "akita-mysql")]
            "mysql" => {
                self.platform = Platform::Mysql;
            },
            _ => {},
        }
        self
    }

    pub fn platform(&self) -> Platform {
        self.platform.to_owned()
    }

    pub fn set_ip_or_hostname(mut self, ip_or_hostname: String) -> Self {
        self.ip_or_hostname = ip_or_hostname.into();
        self
    }

    pub fn ip_or_hostname(&self) -> String {
        self.ip_or_hostname.to_owned().unwrap_or_default()
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
impl PlatformPool {
    /// get a usable database connection from
    pub fn acquire(&self) -> Result<PooledConnection, AkitaError> {
        match *self {
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

    pub fn database(&self, cfg: &AkitaConfig) -> Result<DatabasePlatform, AkitaError> {
        let conn = self.acquire()?;
        match conn {
            #[cfg(feature = "akita-mysql")]
            PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase::new(*pooled_mysql, cfg.to_owned())))),
            #[cfg(feature = "akita-sqlite")]
            PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DatabasePlatform::Sqlite(Box::new(SqliteDatabase::new(*pooled_sqlite, cfg.to_owned())))),
        }
    }
}

#[allow(unused)]
impl Pool {
    pub fn new(mut cfg: AkitaConfig) -> Result<Self, AkitaError>  {
        match cfg.platform {
            #[cfg(feature = "akita-mysql")]
            Platform::Mysql => {
                let pool_mysql = mmysql::init_pool(&cfg)?;
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
        }
    }

    pub fn get_pool(&self) -> Result<PlatformPool, AkitaError> {
        Ok(self.0.clone())
    }

    pub fn config(&self) -> &AkitaConfig {
        &self.1
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

    /// return an entity manager which provides a higher level api
    pub fn entity_manager(&self) -> Result<AkitaEntityManager, AkitaError> {
        let db = self.get_pool()?;
        Ok(AkitaEntityManager::new(db.clone(), self.1.to_owned()))
    }

    /// get a usable database connection from
    pub fn connect_mut(&self) -> Result<PooledConnection, AkitaError> {
        let pool = self.get_pool()?;
        match pool {
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
}