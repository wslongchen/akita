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
use std::sync::{Arc, OnceLock};

use crate::driver::non_blocking::{AsyncDbDriver, AsyncDbExecutor};
use crate::prelude::{AkitaConfig, Result, Wrapper};
use crate::prelude::{AkitaError};
use crate::xml::XmlSqlLoader;
use akita_core::{cfg_if, AkitaValue, FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, Params, Rows, SqlOperator, SqlSecurityConfig};
use crate::interceptor::non_blocking::{AsyncInterceptorBuilder, AsyncInterceptorChain};
use crate::mapper::IPage;
use crate::mapper::non_blocking::AsyncAkitaMapper;
use crate::pool::non_blocking::{AsyncDBPoolWrapper, AsyncPooledConnection};
use crate::transaction::non_blocking::AsyncAkitaTransaction;

cfg_if! {if #[cfg(feature = "mysql-async")]{
    use crate::driver::non_blocking::mysql::{MySQLAsync};
}}

cfg_if! {if #[cfg(feature = "sqlite-async")]{
    use crate::driver::non_blocking::sqlite::{SqliteAsync};
}}

cfg_if! {if #[cfg(feature = "oracle-async")]{
    use crate::driver::non_blocking::oracle::{OracleAsync};
}}

cfg_if! {if #[cfg(feature = "mssql-async")]{
    use crate::driver::non_blocking::mssql::{MssqlAsync};
}}

cfg_if! {if #[cfg(feature = "postgres-async")]{
    use crate::driver::non_blocking::postgres::{PostgresAsync};
}}

#[allow(unused)]
#[derive(Clone)]
pub struct AkitaAsync {
    /// the connection pool
    pool: AsyncDBPoolWrapper,
    interceptor_chain: Option<Arc<AsyncInterceptorChain>>,
    sql_security_config: Option<SqlSecurityConfig>,
    xml_sql_loader: XmlSqlLoader,
}

impl std::fmt::Debug for AkitaAsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AkitaAsync")
            .field("pool", &"Pool { ... }")  // 简化显示
            .field("interceptor_chain", &match &self.interceptor_chain {
                Some(_) => "Some(InterceptorChain)",
                None => "None",
            })
            .finish()
    }
}

#[allow(unused)]
impl AkitaAsync {
    pub async fn new(cfg: AkitaConfig) -> Result<Self> {
        let sql_security_config = cfg.sql_security().map(Clone::clone);
        let xml_sql_loader = XmlSqlLoader::new(cfg.xml_sql_loader().map(Clone::clone).unwrap_or_default());
        let pool = AsyncDBPoolWrapper::new(cfg).await?;
        Ok(Self {
            pool,
            interceptor_chain: None,
            sql_security_config,
            xml_sql_loader,
        })
    }

    pub fn from_pool(pool: AsyncDBPoolWrapper) -> Result<Self> {
        Ok(Self {
            pool,
            interceptor_chain: None,
            sql_security_config: None,
            xml_sql_loader: XmlSqlLoader::default(),
        })
    }

    pub fn with_interceptor_chain(mut self, interceptor_chain: AsyncInterceptorChain) -> Self {
        self.interceptor_chain = Some(Arc::new(interceptor_chain));
        self
    }

    pub fn with_interceptor_builder(mut self, builder: AsyncInterceptorBuilder) -> Result<Self> {
        let chain = builder.build()
            .map_err(|e| AkitaError::InterceptorError(e.to_string()))?;
        self.interceptor_chain = Some(Arc::new(chain));
        Ok(self)
    }

    pub fn xml_sql_loader(&self) -> &XmlSqlLoader {
        &self.xml_sql_loader
    }
    
    pub fn interceptor_chain(&self) -> Option<&Arc<AsyncInterceptorChain>> {
        self.interceptor_chain.as_ref()
    }

    /// get DataBase Connection used for the next step
    pub async fn acquire(&self) -> Result<AsyncDbDriver> {
        let pool = self.get_pool()?;
        let conn = pool.acquire().await?;
        
        let platform = match conn {
            #[cfg(feature = "mysql-async")]
            AsyncPooledConnection::PooledMysqlAsync(pooled_mysql) => {
                let mut db = MySQLAsync::new(pooled_mysql).with_sql_security(self.sql_security_config.clone());
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                AsyncDbDriver::MysqlAsyncDriver(Box::new(db))
            },
            #[cfg(feature = "sqlite-async")]
            AsyncPooledConnection::PooledSqliteAsync(pooled_sqlite) => {
                let mut db = SqliteAsync::new(pooled_sqlite).with_sql_security(self.sql_security_config.clone());
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                AsyncDbDriver::SqliteAsyncDriver(Box::new(db))
            },
            #[cfg(feature = "postgres-async")]
            AsyncPooledConnection::PooledPostgresAsync(pooled_postgres) => {
                let mut db = PostgresAsync::new(pooled_postgres).with_sql_security(self.sql_security_config.clone());
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                AsyncDbDriver::PostgresAsyncDriver(Box::new(db))
            },
            #[cfg(feature = "oracle-async")]
            AsyncPooledConnection::PooledOracleAsync(pooled_oracle) => {
                let mut db = OracleAsync::new(pooled_oracle).with_sql_security(self.sql_security_config.clone());
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                AsyncDbDriver::OracleAsyncDriver(Box::new(db))
            },
            #[cfg(feature = "mssql-async")]
            AsyncPooledConnection::PooledMssqlAsync(pooled_mssql) => {
                let mut db = MssqlAsync::new(pooled_mssql).with_sql_security(self.sql_security_config.clone());
                if let Some(chain) = &self.interceptor_chain {
                    db = db.with_interceptor_chain(Arc::clone(chain));
                }
                AsyncDbDriver::MssqlAsyncDriver(Box::new(db))
            },
            _ => return Err(AkitaError::DatabaseError("database must be init.".to_string()))
        };
        Ok(platform)
    }
    
    pub async fn start_transaction(&self) -> Result<AsyncAkitaTransaction> {
        let mut conn = self.acquire().await?;
        conn.start().await?;
        Ok(AsyncAkitaTransaction {
            conn,
            committed: false,
            rolled_back: false,
        })
    }

    /// get conn pool
    pub fn get_pool(&self) -> Result<&AsyncDBPoolWrapper> {
        Ok(&self.pool)
    }

    pub fn new_wrapper(&self) -> Wrapper {
        Wrapper::new()
    }

    pub fn wrapper(&self) -> Wrapper {
        Wrapper::new()
    }
}

#[allow(unused)]
#[async_trait::async_trait]
impl AsyncAkitaMapper for AkitaAsync {
    /// Get all the table of records
    async fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.list(wrapper).await
    }

    /// Get one the table of records
    async fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.select_one(wrapper).await
    }

    /// Get one the table of records by id
    async fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
        I: IntoAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.select_by_id(id).await
    }

    /// Get table of records with page
    async fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.page(page, size, wrapper).await
    }

    /// Get the total count of records
    async fn count<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.count::<T>(wrapper).await
    }

    /// Remove the records by wrapper.
    async fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.remove::<T>(wrapper).await
    }

    /// Remove the records by ids.
    async fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.remove_by_ids::<T, I>(ids).await
    }

    /// Remove the records by id.
    async fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.remove_by_id::<T, I>(id).await
    }

    /// Update the records by wrapper.
    async fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.update(entity, wrapper).await
    }

    /// Update the records by id.
    async fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.update_by_id(entity).await
    }

    async fn update_batch_by_id<T>(&self, entities: &[T]) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.update_batch_by_id(entities).await
    }

    #[allow(unused_variables)]
    async fn save_batch<T, E>(&self, entities: E) -> Result<()>
    where
        E: IntoIterator<Item = T> + Send + Sync,
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.save_batch(entities).await
    }

    /// called multiple times when using database platform that doesn't support multiple value
    async fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.save(entity).await
    }

    async fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.save_or_update(entity).await
    }

    async fn exec_iter<S, P>(&self, sql: S, params: P) -> Result<Rows>
    where
        S: Into<String> + Send + Sync,
        P: Into<Params> + Send + Sync,
    {
        let mut conn = self.acquire().await?;
        conn.exec_iter(sql, params).await
    }
}


/// Chained calls
#[allow(mismatched_lifetime_syntaxes)]
impl AkitaAsync {

    /// Add a new chain query method
    pub fn query_builder<T>(&self) -> AsyncQueryBuilder<T> {
        AsyncQueryBuilder::new(self)
    }

    /// Or go straight back to the wrapper
    pub fn update_builder<T>(&self) -> AsyncUpdateBuilder<T> where
        T: GetTableName  {
        AsyncUpdateBuilder::new(self).table(T::table_name().name)
    }
}


/// Added query builder
pub struct AsyncQueryBuilder<'a, T> {
    akita: &'a AkitaAsync,
    wrapper: Wrapper,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> AsyncQueryBuilder<'a, T> {
    pub fn new(akita: &'a AkitaAsync) -> Self {
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

    pub async fn list(self) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send,

    {
        self.akita.list::<T>(self.wrapper).await
    }

    /// Get one the table of records
    pub async fn select_one(self) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send,

    {
        self.akita.select_one::<T>(self.wrapper).await
    }

    /// Get one the table of records by id
    pub async fn select_by_id<I>(self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send,
        I: IntoAkitaValue + Sync + Send
    {
        self.akita.select_by_id::<T, I>(id).await
    }

    /// Get table of records with page
    pub async fn page(self, page: u64, size: u64) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send,

    {
        self.akita.page::<T>(page, size, self.wrapper).await
    }

    /// Get the total count of records
    pub async fn count(self) -> Result<u64>
    where
        T: GetTableName + GetFields + Sync + Send,
    {
        self.akita.count::<T>(self.wrapper).await
    }
    
}


/// Added a modified builder
pub struct AsyncUpdateBuilder<'a, T> {
    akita: &'a AkitaAsync,
    wrapper: Wrapper,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> AsyncUpdateBuilder<'a, T> {
    pub fn new(akita: &'a AkitaAsync) -> Self {
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
    pub async fn remove(self) -> Result<u64>
    where
        T: GetTableName + GetFields + Sync + Send,
    {
        self.akita.remove::<T>(self.wrapper).await
    }

    pub async fn remove_by_ids<I>(self, ids: Vec<I>) -> Result<u64> where I: IntoAkitaValue + Sync + Send, T: GetTableName + GetFields + Sync + Send {
        self.akita.remove_by_ids::<T, I>(ids).await
    }

    /// Remove the records by id.
    pub async fn remove_by_id<I>(self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue+ Sync + Send,
        T: GetTableName + GetFields + Sync + Send {
        self.akita.remove_by_id::<T, I>(id).await
    }

    /// Update the records by wrapper.
    pub async fn update(self, entity: &T) -> std::result::Result<u64, AkitaError> where T: GetTableName + GetFields + IntoAkitaValue + Send + Sync {
        self.akita.update(entity, self.wrapper).await
    }

}