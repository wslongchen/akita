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

//!
//! Akita
//!

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use once_cell::sync::Lazy;

use akita_core::GetTableName;

use crate::{AkitaConfig, AkitaError, AkitaMapper, database::DatabasePlatform, IdentifierGenerator, IPage, Pool, Wrapper};
use crate::{cfg_if, FromValue, GetFields, Params, Rows, ToValue};
use crate::key::SnowflakeGenerator;
use crate::manager::AkitaTransaction;

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::{mysql::{self, MysqlDatabase}};
}}

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::{self, SqliteDatabase};
}}

#[allow(unused)]
#[derive(Clone)]
pub struct Akita {
    /// the connection pool
    pool: Pool,
}

// 全局生成器
pub(crate) static GLOBAL_GENERATOR: Lazy<Arc<SnowflakeGenerator>> = Lazy::new(|| {
    Arc::new(SnowflakeGenerator::new())
});

// // 自定义 Clone 逻辑
// impl Clone for Akita {
//     fn clone(&self) -> Self {
//         // 注意：这里只复制结构体的状态，不会复制 `Box<dyn IdentifierGenerator>` 的内容
//         Akita {
//             pool: self.pool.clone(),
//             identifier_generator: OnceLock::new(),
//         }
//     }
// }

#[allow(unused)]
impl Akita {
    pub fn new(cfg: AkitaConfig) -> Result<Self, AkitaError> {
        let pool = Pool::new(cfg)?;
        Ok(Self {
            pool,
        })
    }

    pub fn from_pool(pool: Pool) -> Result<Self, AkitaError> {
        Ok(Self {
            pool,
        })
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
    pub fn get_pool(&self) -> Result<&Pool, AkitaError> {
        Ok(&self.pool)
    }

    /// get an DataBase Connection used for the next step
    pub fn acquire(&self) -> Result<DatabasePlatform, AkitaError> {
        let pool = self.get_pool()?;
        let conn = pool.acquire()?;
        match conn {
            #[cfg(feature = "akita-mysql")]
            crate::pool::PooledConnection::PooledMysql(pooled_mysql) => Ok(DatabasePlatform::Mysql(Box::new(MysqlDatabase::new(pooled_mysql)))),
            #[cfg(feature = "akita-sqlite")]
            crate::pool::PooledConnection::PooledSqlite(pooled_sqlite) => Ok(DatabasePlatform::Sqlite(Box::new(SqliteDatabase::new(pooled_sqlite)))),
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
    fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let mut conn = self.acquire()?;
        conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>, AkitaError>
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
    fn page<T>(&self, page: usize, size: usize, wrapper: Wrapper) -> Result<IPage<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let mut conn = self.acquire()?;
        conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper: Wrapper) -> Result<usize, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let mut conn = self.acquire()?;
        conn.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper: Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let mut conn = self.acquire()?;
        conn.remove::<T>(wrapper)
    }

    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64, AkitaError> where I: ToValue, T: GetTableName + GetFields {
        let mut conn = self.acquire()?;
        conn.remove_by_ids::<T, I>(ids)
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

    use once_cell::sync::Lazy;

    use akita_core::ToValue;

    use crate::{Akita, AkitaConfig, AkitaMapper, Entity, Converter, self as akita, Wrapper};

    pub static AK: Lazy<Akita> = Lazy::new(|| {
        let mut cfg = AkitaConfig::new("mysql://root:longchen@localhost:3306/test");
        cfg = cfg.set_max_size(5).set_connection_timeout(Duration::from_secs(5));
        let mut akita = Akita::new(cfg).unwrap();
        akita
    });


    pub struct EncrptConverter;

    impl Converter<String> for EncrptConverter {
        fn convert(data: &String) -> String {
            "1".to_string()
        }

        fn revert(data: &String) -> String {
            "2".to_string()
        }
    }

    #[derive(Clone, Debug, Entity)]
    pub struct MchInfo {
        #[id(name="mch_no", id_type="assign_uuid")]
        pub mch_no: Option<String>,
        #[field(fill(function = "test_function", mode = "default"))]
        pub mch_name: Option<String>,
        #[field(converter = "crate::akita::test::EncrptConverter")]
        pub id_card_no: String,
        #[field(exist = false)]
        pub sinner: Option<Inner>,
    }

    #[derive(Clone, Debug, FromValue, ToValue)]
    pub struct Inner {
        pub id: i32,
    }

    #[sql(AK, "select * from mch_info where mch_no = ? and mch_no = ? limit ?")]
    fn select(name: &str, id: u8, limit: u8) -> Vec<MchInfo> {
        todo!()
    }

    fn test_function() -> String {
        println!("跑起来啦");
        String::from("test")
    }

    #[test]
    fn test_akita() {
        let mut cfg = AkitaConfig::new("mysql://root:password@localhost:3306/akita");
        cfg = cfg.set_max_size(5).set_connection_timeout(Duration::from_secs(5));
        // let mut akita = Akita::new(cfg).unwrap();
        let wrapper = Wrapper::new().eq(MchInfo::mch_no(), "sdff");
        // let data = akita.select_by_id::<MchInfo, _>("23234234").unwrap();
        //let s = select("23234234");
        println!("ssssssss{:?}", wrapper.get_query_sql());
        // let s = select("i");
    }

    #[test]
    fn test_select() {
        let result = select("23234234", 1, 1).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_convert() {

        let handles: Vec<_> = (0..1)
            .map(|_| {
                std::thread::spawn(move || {
                    for _ in 0..1 {
                        let result = AK.save::<_, String>(&MchInfo {
                            mch_no: "ffff".to_string().into(),
                            mch_name: "1111".to_string().into(),
                            id_card_no: "sssdddd".to_string(),
                            sinner: None,
                        }).ok();
                        println!("save ID: {:?}", result.unwrap_or_default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // let result = AK.select_by_id::<MchInfo, &str>("ffff").unwrap();
        // let result = EncrptConverter::revert(&"stest".to_string());
    }
}