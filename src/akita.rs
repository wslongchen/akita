//! 
//! Akita
//!

use std::cell::RefCell;
use std::rc::Rc;
use akita_core::{ GetTableName};
use once_cell::sync::OnceCell;

use crate::{AkitaError, AkitaMapper, IPage, Pool, Wrapper, database::DatabasePlatform, AkitaConfig};
use crate::{cfg_if, Params, Rows, FromValue, ToValue, GetFields};
use crate::database::Platform;
use crate::manager::{AkitaTransaction};
use crate::pool::{PlatformPool};

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::{mysql::{self, MysqlDatabase}};
}}

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::{self, SqliteDatabase};
}}

#[allow(unused)]
pub struct Akita{
    /// the connection pool
    pool: OnceCell<PlatformPool>,
    cfg: AkitaConfig,
}

#[allow(unused)]
impl Akita {
    
    pub fn new(cfg: AkitaConfig) -> Result<Self, AkitaError> {
        let platform = Self::init_pool(&cfg)?;
        Ok(Self {
            pool: OnceCell::from(platform),
            cfg
        })
    }

    pub fn from_pool(pool: &Pool) -> Result<Self, AkitaError> {
        let platform = pool.get_pool()?;
        Ok(Self {
            pool: OnceCell::from(platform),
            cfg: pool.config().clone()
        })
    }

    #[cfg(feature = "akita-fuse")]
    pub fn fuse(&self) -> crate::fuse::Fuse {
        crate::fuse::Fuse::new(self)
    }

    /// get a database instance with a connection, ready to send sql statements
    fn init_pool(cfg: &AkitaConfig) -> Result<PlatformPool, AkitaError> {
        match cfg.platform() {
            #[cfg(feature = "akita-mysql")]
            Platform::Mysql => {
                let pool_mysql = mysql::init_pool(&cfg)?;
                Ok(PlatformPool::MysqlPool(pool_mysql))
            }
            #[cfg(feature = "akita-sqlite")]
            Platform::Sqlite(ref path) => {
                let mut cfg = cfg.clone();
                cfg = cfg.set_url(path.to_string());
                let pool_sqlite = sqlite::init_pool(&cfg)?;
                Ok(PlatformPool::SqlitePool(pool_sqlite))
            }
            Platform::Unsupported(scheme) => Err(AkitaError::UnknownDatabase(scheme))
        }
    }

    pub fn start_transaction(&self) -> Result<AkitaTransaction, AkitaError> {
        let mut conn = self.acquire()?;
        conn.start_transaction()?;
        Ok(AkitaTransaction {
            conn: Rc::new(RefCell::new(conn)),
            committed: false,
            rolled_back: false,
        })
    }

    /// get conn pool
    pub fn get_pool(&self) -> Result<&PlatformPool, AkitaError> {
        let p = self.pool.get();
        if p.is_none() {
            return Err(AkitaError::R2D2Error("[akita] akita pool not inited!".to_string()));
        }
        return Ok(p.unwrap());
    }

    /// get an DataBase Connection used for the next step
    pub fn acquire(&self) -> Result<DatabasePlatform, AkitaError> {
        let pool = self.get_pool()?;
        let conn = pool.acquire()?;
        match conn {
            #[cfg(feature = "akita-mysql")]
            crate::pool::PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase::new(*pooled_mysql, self.cfg.to_owned())))),
            #[cfg(feature = "akita-sqlite")]
            crate::pool::PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DatabasePlatform::Sqlite(Box::new(SqliteDatabase::new(*pooled_sqlite, self.cfg.to_owned())))),
            _ => return Err(AkitaError::UnknownDatabase("database must be init.".to_string()))
        }
    }

    pub fn new_wrapper(&self) -> Wrapper {
        Wrapper::new()
    }

    pub fn wrapper<T: GetTableName>(&self) -> Wrapper {
        Wrapper::new().table(T::table_name().complete_name())
    }
}

#[allow(unused)]
impl AkitaMapper for Akita {
    /// Get all the table of records
    fn list<T>(&self, wrapper:Wrapper) -> Result<Vec<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let mut conn = self.acquire()?;
        conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper:Wrapper) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let mut conn = self.acquire()?;
        conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,
            I: ToValue
    {
        let mut conn = self.acquire()?;
        conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T>(&self, page: usize, size: usize, wrapper:Wrapper) -> Result<IPage<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let mut conn = self.acquire()?;
        conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper:Wrapper) -> Result<usize, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let mut conn = self.acquire()?;
        conn.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper:Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let mut conn = self.acquire()?;
        conn.remove::<T>(wrapper)
    }

    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64, AkitaError> where I: ToValue, T: GetTableName + GetFields {
        let mut conn = self.acquire()?;
        conn.remove_by_ids::<T,I>(ids)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64, AkitaError>
        where
            I: ToValue,
            T: GetTableName + GetFields {
        let mut conn = self.acquire()?;
        conn.remove_by_id::<T, I>(id)

    }

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + ToValue {
        let mut conn = self.acquire()?;
        conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + ToValue {
        let mut conn = self.acquire()?;
        conn.update_by_id(entity)

    }

    #[allow(unused_variables)]
    fn save_batch<T>(&self, entities: &[&T]) -> Result<(), AkitaError>
        where
            T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.acquire()?;
        conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue,
    {
        let mut conn = self.acquire()?;
        conn.save(entity)
    }

    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>, AkitaError> where T: GetTableName + GetFields + ToValue, I: FromValue {
        let mut conn = self.acquire()?;
        conn.save_or_update(entity)
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> Result<Rows, AkitaError> {
        let mut conn = self.acquire()?;
        conn.exec_iter(sql, params)
    }

}

#[allow(unused)]
mod test {
    use std::time::Duration;
    use akita_core::ToValue;
    use once_cell::sync::Lazy;
    use crate::{Akita, AkitaTable, self as akita, AkitaConfig, LogLevel, AkitaMapper, Wrapper};

    pub static AK:Lazy<Akita> = Lazy::new(|| {
        let mut cfg = AkitaConfig::new("xxxx".to_string());
        cfg = cfg.set_max_size(5).set_connection_timeout(Duration::from_secs(5)).set_log_level(LogLevel::Info);
        let mut akita = Akita::new(cfg).unwrap();
        akita
    });
    #[derive(Clone, Debug, AkitaTable)]
    pub struct MchInfo {
        #[table_id]
        pub mch_no: Option<String>,
        #[field(fill( function = "fffff", mode = "default"))]
        pub mch_name: Option<String>,
    }

    #[sql(AK,"select * from mch_info where mch_no = ? and id = ? limit ?")]
    fn select(name: &str, id : u8, limit: u8) -> Vec<MchInfo> {
        todo!()
    }

    fn fffff() -> String {
        println!("跑起来啦");
        String::from("test")

    }

    #[test]
    fn test_akita() {
        let mut cfg = AkitaConfig::new("xxxxx".to_string());
        cfg = cfg.set_max_size(5).set_connection_timeout(Duration::from_secs(5)).set_log_level(LogLevel::Info);
        // let mut akita = Akita::new(cfg).unwrap();
        let wrapper = Wrapper::new().eq(MchInfo::mch_no(), "sdff");
        // let data = akita.select_by_id::<MchInfo, _>("23234234").unwrap();
        //let s = select("23234234");
        println!("ssssssss{:?}",wrapper.get_query_sql());
        // let s = select("i");
    }
}