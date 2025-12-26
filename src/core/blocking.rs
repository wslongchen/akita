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

use std::borrow::Borrow;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};

use once_cell::sync::Lazy;
use akita_core::{cfg_if, AkitaValue, AndOr, Condition, FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, JoinClause, JoinType, OrderByClause, OrderDirection, Params, Rows, SetOperation, SqlOperator, SqlSecurityConfig};
use crate::config::XmlSqlLoaderConfig;
use crate::driver::blocking::DbDriver;
use crate::interceptor::blocking::{InterceptorBuilder, InterceptorChain};
use crate::prelude::{AkitaError};
use crate::prelude::{AkitaConfig, IdentifierGenerator, Wrapper};
use crate::key::SnowflakeGenerator;
use crate::mapper::blocking::AkitaMapper;
use crate::mapper::IPage;
use crate::pool::blocking::{DBPoolWrapper, PooledConnection};

use crate::transaction::blocking::AkitaTransaction;
use crate::xml::XmlSqlLoader;


cfg_if! {if #[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))] {
    use crate::repository::EntityRepository;
}}

cfg_if! {if #[cfg(feature = "mysql-sync")]{
    use crate::driver::blocking::mysql::{MySQL};
}}

cfg_if! {if #[cfg(feature = "sqlite-sync")]{
    use crate::driver::blocking::sqlite::{Sqlite};
}}

cfg_if! {if #[cfg(feature = "oracle-sync")]{
    use crate::driver::blocking::oracle::{Oracle};
}}

cfg_if! {if #[cfg(feature = "mssql-sync")]{
    use crate::driver::blocking::mssql::{Mssql};
}}

cfg_if! {if #[cfg(feature = "postgres-sync")]{
    use crate::driver::blocking::postgres::{Postgres};
}}

#[allow(unused)]
#[derive(Clone)]
pub struct Akita {
    /// the connection pool
    pool: DBPoolWrapper,
    interceptor_chain: Option<Arc<InterceptorChain>>,
}

impl std::fmt::Debug for Akita {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Akita")
            .field("pool", &"Pool { ... }") 
            .field("interceptor_chain", &match &self.interceptor_chain {
                Some(_) => "Some(InterceptorChain)",
                None => "None",
            })
            .finish()
    }
}

#[allow(unused)]
impl Akita {
    pub fn new(cfg: AkitaConfig) -> Result<Self, AkitaError> {
        let pool = DBPoolWrapper::new(cfg)?;
        Ok(Self {
            pool,
            interceptor_chain: None,
        })
    }

    pub fn from_pool(pool: DBPoolWrapper) -> Result<Self, AkitaError> {
        Ok(Self {
            pool,
            interceptor_chain: None,
        })
    }

    pub fn with_interceptor_chain(mut self, interceptor_chain: InterceptorChain) -> Self {
        self.interceptor_chain = Some(Arc::new(interceptor_chain));
        self
    }

    pub fn with_interceptor_builder(mut self, builder: InterceptorBuilder) -> Result<Self, AkitaError> {
        let chain = builder.build()
            .map_err(|e| AkitaError::InterceptorError(e.to_string()))?;
        self.interceptor_chain = Some(Arc::new(chain));
        Ok(self)
    }
    
    pub fn interceptor_chain(&self) -> Option<&Arc<InterceptorChain>> {
        self.interceptor_chain.as_ref()
    }

    #[cfg(all(
        any(
            feature = "mysql-sync",
            feature = "postgres-sync",
            feature = "sqlite-sync",
            feature = "oracle-sync",
            feature = "mssql-sync"
        ),
        not(any(
            feature = "mysql-async",
            feature = "postgres-async",
            feature = "sqlite-async",
            feature = "mssql-async",
            feature = "oracle-async"
        ))
    ))]
    pub fn repository<T>(&self) -> EntityRepository<Akita, T> {
        EntityRepository::new(self.clone())
    }
    
    /// get DataBase Connection used for the next step
    pub fn acquire(&self) -> Result<DbDriver, AkitaError> {
        let pool = self.get_pool()?;
        let conn = pool.acquire()?;
        let sql_security_config = pool.config().sql_security().map(Clone::clone);
        let platform = match conn {
            #[cfg(feature = "mysql-sync")]
            PooledConnection::PooledMysql(pooled_mysql) => {
                let mut db = MySQL::new(pooled_mysql).with_sql_security(sql_security_config);
                
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                if let Some(database) = self.database_name() {
                    db = db.with_database(database);
                }
                DbDriver::MysqlDriver(Box::new(db))
            },
            #[cfg(feature = "sqlite-sync")]
            PooledConnection::PooledSqlite(pooled_sqlite) => {
                let mut db = Sqlite::new(pooled_sqlite).with_sql_security(sql_security_config);
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                if let Some(database) = self.database_name() {
                    db = db.with_database(database);
                }
                DbDriver::SqliteDriver(Box::new(db))
            },
            #[cfg(feature = "postgres-sync")]
            PooledConnection::PooledPostgres(pooled_postgres) => {
                let mut db = Postgres::new(pooled_postgres).with_sql_security(sql_security_config);
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                if let Some(database) = self.database_name() {
                    db = db.with_database(database);
                }
                DbDriver::PostgresDriver(Box::new(db))
            },
            #[cfg(feature = "oracle-sync")]
            PooledConnection::PooledOracle(pooled_oracle) => {
                let mut db = Oracle::new(pooled_oracle).with_sql_security(sql_security_config);
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                if let Some(database) = self.database_name() {
                    db = db.with_database(database);
                }
                DbDriver::OracleDriver(Box::new(db))
            },
            #[cfg(feature = "mssql-sync")]
            PooledConnection::PooledMssql(pooled_mssql) => {
                let mut db = Mssql::new(pooled_mssql).with_sql_security(sql_security_config);
                if let Some(chain) = self.interceptor_chain.as_ref() {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                if let Some(database) = self.database_name() {
                    db = db.with_database(database);
                }
                DbDriver::MssqlDriver(Box::new(db))
            },
            _ => return Err(AkitaError::DatabaseError("database must be init.".to_string()))
        };

        Ok(platform)
    }
    
    pub fn start_transaction(&self) -> Result<AkitaTransaction, AkitaError> {
        let mut conn = self.acquire()?;
        conn.start()?;
        Ok(AkitaTransaction {
            conn,
            committed: false,
            rolled_back: false,
        })
    }

    /// get conn pool
    pub fn get_pool(&self) -> Result<&DBPoolWrapper, AkitaError> {
        Ok(&self.pool)
    }

    pub fn new_wrapper(&self) -> Wrapper {
        Wrapper::new()
    }

    pub fn wrapper(&self) -> Wrapper {
        Wrapper::new()
    }
    
    pub fn database_name(&self) -> Option<String> {
        self.pool.config().get_database().unwrap_or_default()
    }
}

#[allow(unused)]
impl AkitaMapper for Akita {
    /// Get all the table of records
    fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromAkitaValue,

    {
        let mut conn = self.acquire()?;
        conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromAkitaValue,

    {
        let mut conn = self.acquire()?;
        conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromAkitaValue,
            I: IntoAkitaValue
    {
        let mut conn = self.acquire()?;
        conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromAkitaValue,

    {
        let mut conn = self.acquire()?;
        conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper: Wrapper) -> Result<u64, AkitaError>
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

    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64, AkitaError> where I: IntoAkitaValue, T: GetTableName + GetFields {
        let mut conn = self.acquire()?;
        conn.remove_by_ids::<T, I>(ids)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64, AkitaError>
        where
            I: IntoAkitaValue,
            T: GetTableName + GetFields {
        let mut conn = self.acquire()?;
        conn.remove_by_id::<T, I>(id)
    }

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + IntoAkitaValue {
        let mut conn = self.acquire()?;
        conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + IntoAkitaValue {
        let mut conn = self.acquire()?;
        conn.update_by_id(entity)
    }

    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> Result<u64, AkitaError>
    where
        T: GetTableName + GetFields + IntoAkitaValue
    {
        let mut conn = self.acquire()?;
        conn.update_batch_by_id(entities)
    }

    #[allow(unused_variables)]
    fn save_batch<T, E>(&self, entities: E) -> crate::prelude::Result<()>
    where
        E: IntoIterator<Item = T>,
        T: GetTableName + GetFields + IntoAkitaValue,
    {
        let mut conn = self.acquire()?;
        conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + IntoAkitaValue,
            I: FromAkitaValue,
    {
        let mut conn = self.acquire()?;
        conn.save(entity)
    }

    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>, AkitaError> where T: GetTableName + GetFields + IntoAkitaValue, I: FromAkitaValue {
        let mut conn = self.acquire()?;
        conn.save_or_update(entity)
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> Result<Rows, AkitaError> {
        let mut conn = self.acquire()?;
        conn.exec_iter(sql, params)
    }
}


/// Chained calls
#[allow(mismatched_lifetime_syntaxes)]
impl Akita {

    /// Add a new chain query method
    pub fn query_builder<T>(&self) -> QueryBuilder<T> where T: GetTableName  {
        QueryBuilder::new(self).table(T::table_name().complete_name())
    }

    /// Or go straight back to the wrapper
    pub fn update_builder<T>(&self) -> UpdateBuilder<T> where
        T: GetTableName  {
        UpdateBuilder::new(self).table(T::table_name().complete_name())
    }
}


/// Added query builder
pub struct QueryBuilder<'a, T> {
    akita: &'a Akita,
    wrapper: Wrapper,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> QueryBuilder<'a, T> {
    pub fn new(akita: &'a Akita) -> Self {
        Self {
            akita,
            wrapper: Wrapper::new(),
            _phantom: std::marker::PhantomData,
        }
    }
    
    pub fn limit(mut self, limit: u64) -> Self {
        self.wrapper = self.wrapper.limit(limit);
        self
    }

    pub fn eq<S: Into<String>, V: Into<AkitaValue>>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.eq(column, value);
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.wrapper = self.wrapper.table(table);
        self
    }

    pub fn alias<S: Into<String>>(mut self, alias: S) -> Self {
        self.wrapper = self.wrapper.alias(alias);
        self
    }

    // ========== SELECT ==========

    pub fn select<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.select(columns);
        self
    }

    pub fn select_distinct<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.select_distinct(columns);
        self
    }

    // ========== WHERE ==========

    pub fn ne<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.ne(column, value);
        self
    }


    pub fn gt<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.gt(column, value);
        self
    }

    pub fn ge<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.ge(column, value);
        self
    }


    pub fn lt<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.lt(column, value);
        self
    }

    pub fn le<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.le(column, value);
        self
    }

    pub fn like<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.like(column, value);
        self
    }

    pub fn not_like<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.not_like(column, value);
        self
    }

    pub fn is_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_null(column);
        self
    }

    pub fn is_not_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_not_null(column);
        self
    }

    pub fn r#in<S,V, I>(mut self, column: S, values: I) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        self.wrapper = self.wrapper.r#in(column, values);
        self
    }

    pub fn not_in<S,V, I>(mut self, column: S, values: I) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        self.wrapper = self.wrapper.not_in(column, values);
        self
    }

    pub fn between<S,V>(mut self, column: S, start: V, end: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.between(column, start, end);
        self
    }

    pub fn not_between<S,V>(mut self, column: S, start: V, end: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.not_between(column, start, end);
        self
    }

    // ========== Logical operations ==========

    pub fn and<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        self.wrapper = self.wrapper.and(func);
        self
    }

    pub fn or<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        self.wrapper = self.wrapper.or(func);
        self
    }

    pub fn or_direct(mut self) -> Self {
        self.wrapper = self.wrapper.or_direct();
        self
    }

    // ========== JOIN ==========
    
    pub fn inner_join<S,C>(mut self, table: S, condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.inner_join(table, condition);
        self
    }

    pub fn left_join<S,C>(mut self, table: S, condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.left_join(table, condition);
        self
    }

    pub fn right_join<S,C>(mut self, table: S,condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.right_join(table, condition);
        self
    }

    pub fn full_join<S,C>(mut self, table: S,condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.full_join(table, condition);
        self
    }

    // ========== GROUP BY / HAVING ==========

    pub fn group_by<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.group_by(columns);
        self
    }

    pub fn having<S,V>(mut self, column: S,operator: SqlOperator, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.having(column, operator, value);
        self
    }

    // ========== ORDER BY ==========

    pub fn order_by_asc<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.order_by_asc(columns);
        self
    }

    pub fn order_by_desc<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.order_by_desc(columns);
        self
    }

    // ========== Conditional tagging method ==========

    /// When the condition is true, subsequent chained calls are executed
    pub fn when(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.when(condition);
        self
    }

    /// When the condition is false, subsequent chained calls are executed
    pub fn unless(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.unless(condition);
        self
    }

    /// Skip the next condition (whatever it is)
    pub fn skip_next(mut self) -> Self {
        self.wrapper = self.wrapper.skip_next();
        self
    }

    pub fn list(self) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.akita.list::<T>(self.wrapper)
    }

    /// Get one the table of records
    pub fn select_one(self) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.akita.select_one::<T>(self.wrapper)
    }

    /// Get one the table of records by id
    pub fn select_by_id<I>(self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkitaValue,
        I: IntoAkitaValue
    {
        self.akita.select_by_id::<T, I>(id)
    }

    /// Get table of records with page
    pub fn page(self, page: u64, size: u64) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.akita.page::<T>(page, size, self.wrapper)
    }

    /// Get the total count of records
    pub fn count(self) -> Result<u64, AkitaError>
    where
        T: GetTableName + GetFields,
    {
        self.akita.count::<T>(self.wrapper)
    }
    
}


/// Added a modified builder
pub struct UpdateBuilder<'a, T> {
    akita: &'a Akita,
    wrapper: Wrapper,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> UpdateBuilder<'a, T> {
    pub fn new(akita: &'a Akita) -> Self {
        Self {
            akita,
            wrapper: Wrapper::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn eq<S: Into<String>, V: Into<AkitaValue>>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.eq(column, value);
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.wrapper = self.wrapper.table(table);
        self
    }

    pub fn alias<S: Into<String>>(mut self, alias: S) -> Self {
        self.wrapper = self.wrapper.alias(alias);
        self
    }

    // ========== SELECT ==========

    pub fn select<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.select(columns);
        self
    }

    pub fn select_distinct<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.select_distinct(columns);
        self
    }

    // ========== WHERE ==========

    pub fn ne<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.ne(column, value);
        self
    }


    pub fn gt<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.gt(column, value);
        self
    }

    pub fn ge<S, V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.ge(column, value);
        self
    }


    pub fn lt<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.lt(column, value);
        self
    }

    pub fn le<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.le(column, value);
        self
    }

    pub fn like<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.like(column, value);
        self
    }

    pub fn not_like<S,V>(mut self, column: S, value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.not_like(column, value);
        self
    }

    pub fn is_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_null(column);
        self
    }

    pub fn is_not_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_not_null(column);
        self
    }

    pub fn r#in<S,V, I>(mut self, column: S, values: I) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        self.wrapper = self.wrapper.r#in(column, values);
        self
    }

    pub fn not_in<S,V, I>(mut self, column: S, values: I) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        self.wrapper = self.wrapper.not_in(column, values);
        self
    }

    pub fn between<S,V>(mut self, column: S, start: V, end: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.between(column, start, end);
        self
    }

    pub fn not_between<S,V>(mut self, column: S, start: V, end: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.not_between(column, start, end);
        self
    }

    // ========== Logical operations ==========

    pub fn and<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        self.wrapper = self.wrapper.and(func);
        self
    }

    pub fn or<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        self.wrapper = self.wrapper.or(func);
        self
    }

    pub fn or_direct(mut self) -> Self {
        self.wrapper = self.wrapper.or_direct();
        self
    }

    // ========== JOIN ==========

    pub fn inner_join<S,C>(mut self, table: S, condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.inner_join(table, condition);
        self
    }

    pub fn left_join<S,C>(mut self, table: S, condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.left_join(table, condition);
        self
    }

    pub fn right_join<S,C>(mut self, table: S,condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.right_join(table, condition);
        self
    }

    pub fn full_join<S,C>(mut self, table: S,condition: C) -> Self
    where
        S: Into<String>,
        C: Into<String>,
    {
        self.wrapper = self.wrapper.full_join(table, condition);
        self
    }

    // ========== ORDER BY ==========

    pub fn order_by_asc<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.order_by_asc(columns);
        self
    }

    pub fn order_by_desc<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.order_by_desc(columns);
        self
    }

    // ========== SET (UPDATE) ==========

    pub fn set<S,V>(mut self, column: S,value: V) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
    {
        self.wrapper = self.wrapper.set(column, value);
        self
    }

    pub fn set_multiple<S,V, I>(mut self, operations: I) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = (S,V)>,
    {
        self.wrapper = self.wrapper.set_multiple(operations);
        self
    }

    // ========== Conditional tagging method ==========

    /// When the condition is true, subsequent chained calls are executed
    pub fn when(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.when(condition);
        self
    }

    /// When the condition is false, subsequent chained calls are executed
    pub fn unless(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.unless(condition);
        self
    }

    /// Skip the next condition (whatever it is)
    pub fn skip_next(mut self) -> Self {
        self.wrapper = self.wrapper.skip_next();
        self
    }

    /// Remove the records by wrapper.
    pub fn remove(self) -> Result<u64, AkitaError>
    where
        T: GetTableName + GetFields,
    {
        self.akita.remove::<T>(self.wrapper)
    }

    pub fn remove_by_ids<I>(self, ids: Vec<I>) -> Result<u64, AkitaError> where I: IntoAkitaValue, T: GetTableName + GetFields {
        self.akita.remove_by_ids::<T, I>(ids)
    }

    /// Remove the records by id.
    pub fn remove_by_id<I>(self, id: I) -> Result<u64, AkitaError>
    where
        I: IntoAkitaValue,
        T: GetTableName + GetFields {
        self.akita.remove_by_id::<T, I>(id)
    }

    /// Update the records by wrapper.
    pub fn update(self, entity: &T) -> Result<u64, AkitaError> where T: GetTableName + GetFields + IntoAkitaValue {
        self.akita.update(entity, self.wrapper)
    }

}