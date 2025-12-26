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
use std::ops::Deref;
use async_trait::async_trait;
use tokio::runtime::Handle;
use akita_core::{cfg_if, AkitaValue, FieldName, FieldType, FromAkitaValue, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, Rows, Wrapper};

use crate::comm::ExecuteResult;
use crate::core::GLOBAL_GENERATOR;
use crate::errors::{AkitaError, Result};
use crate::key::IdentifierGenerator;
use crate::mapper::{IPage};
use crate::mapper::non_blocking::AsyncAkitaMapper;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder, SqlBuilderFactory};

cfg_if! {
    if #[cfg(feature = "mysql-async")] {
        pub mod mysql;
        use crate::driver::non_blocking::mysql::MySQLAsync;
    }
}

cfg_if! {
    if #[cfg(feature = "postgres-async")] {
        pub mod postgres;
        use crate::driver::non_blocking::postgres::PostgresAsync;
    }
}

cfg_if! {
    if #[cfg(feature = "oracle-async")] {
        pub mod oracle;
        use crate::driver::non_blocking::oracle::OracleAsync;
    }
}

cfg_if! {
    if #[cfg(feature = "sqlite-async")] {
        pub mod sqlite;
        use crate::driver::non_blocking::sqlite::SqliteAsync;
    }
}

cfg_if! {
    if #[cfg(feature = "mssql-async")] {
        pub mod mssql;
        use crate::driver::non_blocking::mssql::MssqlAsync;
    }
}


#[async_trait::async_trait]
pub trait AsyncDbExecutor {
    async fn start(&self) -> crate::errors::Result<()>;

    async fn commit(&self) -> crate::errors::Result<()>;

    async fn rollback(&self) -> crate::errors::Result<()>;

    async fn query(&self, sql: &str, param: Params) -> crate::errors::Result<Rows>;

    async fn execute(&self, sql: &str, param: Params) -> crate::errors::Result<ExecuteResult>;

    async fn affected_rows(&self) -> u64 { 0 }

    async fn last_insert_id(&self) -> u64 { 0 }
}

/// Asynchronous database-driven enumeration
pub enum AsyncDbDriver {
    #[cfg(feature = "mysql-async")]
    MysqlAsyncDriver(Box<MySQLAsync>),
    #[cfg(feature = "postgres-async")]
    PostgresAsyncDriver(Box<PostgresAsync>),
    #[cfg(feature = "oracle-async")]
    OracleAsyncDriver(Box<OracleAsync>),
    #[cfg(feature = "sqlite-async")]
    SqliteAsyncDriver(Box<SqliteAsync>),
    #[cfg(feature = "mssql-async")]
    MssqlAsyncDriver(Box<MssqlAsync>),
}


impl Deref for AsyncDbDriver {
    type Target = dyn AsyncDbExecutor;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(ref mysql) => mysql.deref(),
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(ref sqlite) => sqlite.deref(),
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(ref postgres) => postgres.deref(),
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(ref oracle) => oracle.deref(),
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(ref mssql) => mssql.deref(),
        }
    }
}

impl std::ops::DerefMut for AsyncDbDriver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(ref mut mysql) => mysql.deref_mut(),
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(ref mut sqlite) => sqlite.deref_mut(),
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(ref mut postgres) => postgres.deref_mut(),
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(ref mut mssql) => mssql.deref_mut(),
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(ref mut oracle) => oracle.deref_mut(),
        }
    }
}



#[async_trait]
impl AsyncDbExecutor for AsyncDbDriver {
    async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.query(sql, params).await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.query(sql, params).await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.query(sql, params).await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.query(sql, params).await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.query(sql, params).await,
            _ => Err(AkitaError::Unknown),
        }
    }

    async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.execute(sql, params).await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.execute(sql, params).await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.execute(sql, params).await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.execute(sql, params).await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.execute(sql, params).await,
            _ => Err(AkitaError::Unknown),
        }
    }

    async fn start(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.start().await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.start().await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.start().await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.start().await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.start().await,
            _ => Err(AkitaError::Unknown),
        }
    }

    async fn commit(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.commit().await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.commit().await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.commit().await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.commit().await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.commit().await,
            _ => Err(AkitaError::Unknown),
        }
    }

    async fn rollback(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.rollback().await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.rollback().await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.rollback().await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.rollback().await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.rollback().await,
        }
    }

    async fn affected_rows(&self) -> u64 {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.affected_rows().await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.affected_rows().await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.affected_rows().await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.affected_rows().await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.affected_rows().await,
            _ => 0,
        }
    }

    async fn last_insert_id(&self) -> u64 {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(driver) => driver.last_insert_id().await,
            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(driver) => driver.last_insert_id().await,
            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(driver) => driver.last_insert_id().await,
            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(driver) => driver.last_insert_id().await,
            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(driver) => driver.last_insert_id().await,
            _ => 0,
        }
    }
}



#[async_trait::async_trait]
impl AsyncAkitaMapper for AsyncDbDriver {
    // ========== Query actions ==========

    /// Get all the table of records
    async fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let wrapper = wrapper.table(table.complete_name());
        let sql_builder = self.sql_builder();
        let (sql, params) = sql_builder.build_query_sql(&wrapper);

        let rows = self.query(&sql, params.into()).await?;
        let mut entities = Vec::new();

        for data in rows.object_iter() {
            entities.push(T::from_value(&data));
        }
        Ok(entities)
    }

    /// Get one the table of records
    async fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let mut results = self.list::<T>(wrapper.limit(1)).await?;
        Ok(results.pop())
    }

    /// Get one the table of records by id
    async fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
        I: IntoAkitaValue + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let wrapper = Wrapper::new()
            .table(T::table_name().complete_name())
            .eq(id_field.name, id)
            .limit(1);

        self.select_one::<T>(wrapper).await
    }

    /// Get table of records with page
    async fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let wrapper = wrapper.table(table.complete_name());

        // Count total
        let sql_builder = self.sql_builder();
        let count_sql = sql_builder.build_count_sql(&wrapper);
        let count_params = wrapper.get_parameters();
        let count: u64 = self.exec_first(&count_sql, count_params).await?;

        let mut page_result = IPage::new(page, size, count, Vec::new());

        if page_result.total > 0 {
            let wrapper = wrapper
                .limit(page_result.size)
                .offset(page_result.offset());

            let (sql, params) = sql_builder.build_query_sql(&wrapper);
            let rows = self.query(&sql, params.into()).await?;

            for data in rows.object_iter() {
                page_result.records.push(T::from_value(&data));
            }
        }

        Ok(page_result)
    }

    /// Get the total count of records
    async fn count<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let wrapper = wrapper.table(table.complete_name());
        let sql_builder = self.sql_builder();
        let sql = sql_builder.build_count_sql(&wrapper);
        let params = wrapper.get_parameters();

        self.exec_first(&sql, params).await
    }

    // ========== Delete action ==========

    /// Remove the records by wrapper.
    async fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let wrapper = wrapper.table(table.complete_name());
        let sql_builder = self.sql_builder();
        let sql = sql_builder.build_delete_sql(&table, &wrapper);

        let params = wrapper.get_parameters();
        let result = self.execute(&sql, params.into()).await?;

        Ok(result.affected_rows())
    }

    /// Remove the records by id.
    async fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let wrapper = Wrapper::new()
            .table(T::table_name().complete_name())
            .eq(id_field.name, id);

        self.remove::<T>(wrapper).await
    }

    /// Remove the records by multiple ids.
    async fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        if ids.is_empty() {
            return Ok(0);
        }

        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let id_values: Vec<AkitaValue> = ids.into_iter().map(|id| id.into()).collect();
        let wrapper = Wrapper::new()
            .table(T::table_name().complete_name())
            .r#in(id_field.name, id_values);

        self.remove::<T>(wrapper).await
    }


    // ========== Update actions ==========

    /// Update the records by wrapper.
    async fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let data = entity.into_value();
        let columns = T::fields();
        let mut set_wrapper = Wrapper::new().table(table.complete_name());
        if wrapper.get_set_operations().is_empty() {
            for col in columns.iter().filter(|c| c.exist && matches!(c.field_type, FieldType::TableField)) {
                let col_name = col.alias.as_ref().unwrap_or(&col.name);
                if let Some(value) = data.get_obj_value(col_name) {
                    set_wrapper = set_wrapper.set(col_name, value.clone());
                }
            }
        } else {
            set_wrapper.set_operations(wrapper.get_set_operations().clone());
        }
        let mut final_wrapper = set_wrapper;
        let sql_builder = self.sql_builder();
        final_wrapper.where_conditions(wrapper.get_where_conditions().clone());
        let sql = sql_builder.build_update_sql(&table, &final_wrapper)
            .ok_or_else(|| AkitaError::InvalidSQL("Invalid Update SQL.".to_string()))?;

        let params = final_wrapper.get_parameters();
        let result = self.execute(&sql, params.into()).await?;

        Ok(result.affected_rows())
    }

    /// Update the records by id.
    async fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let data = entity.into_value();
        let id_value = data.get_obj_value(&id_field.name)
            .ok_or_else(|| AkitaError::MissingIdent("Missing id value".to_string()))?;

        let wrapper = Wrapper::new().eq(id_field.name, id_value.clone());
        self.update(entity, wrapper).await
    }

    /// Update multiple records by ids.
    async fn update_batch_by_id<T>(&self, entities: &[T]) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        if entities.is_empty() {
            return Ok(0);
        }

        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let columns = T::fields();
        let update_fields: Vec<&FieldName> = columns.iter()
            .filter(|c| c.exist && matches!(c.field_type, FieldType::TableField))
            .collect();

        // Build CASE WHEN Update statements
        let mut set_clauses = Vec::new();
        for col in &update_fields {
            let col_name = col.alias.as_ref().unwrap_or(&col.name);
            let mut case_stmt = format!("`{}` = CASE", col_name);

            for entity in entities {
                let data = entity.into_value();
                let id_value = data.get_obj_value(&id_field.name)
                    .ok_or_else(|| AkitaError::MissingIdent("Missing id value".to_string()))?;

                let field_value = data.get_obj_value(col_name).unwrap_or(&AkitaValue::Null);

                let value_sql = match field_value {
                    AkitaValue::Text(s) => format!("'{}'", s.replace("'", "''")),
                    AkitaValue::Null => "NULL".to_string(),
                    v => v.to_string(),
                };

                case_stmt.push_str(&format!(" WHEN `{}` = {} THEN {}", id_field.name, id_value, value_sql));
            }

            case_stmt.push_str(&format!(" ELSE `{}` END", col_name));
            set_clauses.push(case_stmt);
        }

        // Build a list of IDs
        let ids: Vec<String> = entities.iter()
            .map(|e| {
                let data = e.into_value();
                data.get_obj_value(&id_field.name).unwrap().to_string()
            })
            .collect();

        let sql = format!(
            "UPDATE {} SET {} WHERE `{}` IN ({})",
            table.complete_name(),
            set_clauses.join(", "),
            id_field.name,
            ids.join(",")
        );

        let result = self.execute(&sql, Params::None).await?;
        Ok(result.affected_rows())
    }
    // ========== Insert action ==========

    /// Insert a single record
    async fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let columns = T::fields();
        let data = entity.into_value();
        let (sql, params) = sql_builder.build_insert_sql(&T::table_name(), columns, vec![data])?;
        let _result = self.execute(&sql, params.into()).await?;
        // Process the returned ID
        let last_insert_id = self.last_insert_id().await;
        Ok(I::from_value_opt(&AkitaValue::from(last_insert_id)).ok())
    }

    /// Insert multiple records
    async fn save_batch<T, E>(&self, entities: E) -> Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        E: IntoIterator<Item = T> + Send + Sync,
    {
        let entities: Vec<_> = entities.into_iter().collect();

        if entities.is_empty() {
            return Ok(());
        }
        let table = T::table_name();
        let columns = T::fields();
        let sql_builder = self.sql_builder();

        // Prepare the bulk insert
        let insert_columns: Vec<FieldName> = columns.iter()
            .filter(|field| field.exist)
            .map(Clone::clone)
            .collect();
        let mut rows = Vec::new();
        for entity in &entities {
            let data = entity.into_value();
            let row: Vec<AkitaValue> = insert_columns.iter()
                .filter_map(|col| {
                    let col_name = col.alias.as_ref().unwrap_or(&col.name).as_str();
                    data.get_obj_value(col_name)
                        .map(|value| {
                            // Handling field padding
                            let mut final_value = value.clone();
                            if let Some(fill) = &col.fill {
                                match fill.mode.as_str() {
                                    "insert" | "default" => {
                                        final_value = fill.value.clone().unwrap_or_default();
                                    }
                                    _ => {}
                                }
                            }
                            // Handle the ID generator
                            final_value = sql_builder.identifier_generator_value(col, final_value);
                            final_value
                        })
                })
                .collect();

            rows.push(row);
        }
        let id_field = columns.iter()
            .find(|field| matches!(field.field_type, FieldType::TableId(_)))
            .map(|field| field.clone());

        let batch_data = BatchInsertData {
            table,
            columns: insert_columns,
            rows,
            id_field,
        };

        if self.is_sql_server() {
            // SQL Server: Intelligently handle parameter limits
            let _result = self.save_batch_for_sqlserver(&batch_data).await?;
        } else {
            // Other databases: Handle them directly
            let (sql, params) = sql_builder.build_batch_insert_sql(&batch_data)?;
            let _result = self.execute(&sql, params.into()).await?;
        }
        Ok(())
    }

    /// Save or update record
    async fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let data = entity.into_value();
        let id_field = sql_builder.find_id_field(T::fields());

        if let Some(field) = id_field {
            if let Some(id_value) = data.get_obj_value(&field.name) {
                if !matches!(id_value, AkitaValue::Null) {
                    self.update_by_id(entity).await?;
                    return Ok(I::from_value_opt(id_value).ok());
                }
            }
        }

        // Add
        self.save(entity).await
    }

    // ========== Other methods ==========

    async fn exec_iter<S, P>(&self, sql: S, params: P) -> Result<Rows>
    where
        S: Into<String> + Send + Sync,
        P: Into<Params> + Send + Sync,
    {
        let sql_builder = self.sql_builder();
        let sql = sql_builder.process_placeholders(&sql.into());
        self.query(&sql, params.into()).await
    }
}

impl AsyncDbDriver {
    pub fn sql_builder(&self) -> Box<dyn SqlBuilder> {
        SqlBuilderFactory::create(self.dialect())
    }

    /// Gets the database dialect type
    pub fn dialect(&self) -> DatabaseDialect {
        match self {
            #[cfg(feature = "mysql-async")]
            AsyncDbDriver::MysqlAsyncDriver(_) => DatabaseDialect::MySQL,

            #[cfg(feature = "postgres-async")]
            AsyncDbDriver::PostgresAsyncDriver(_) => DatabaseDialect::Postgres,

            #[cfg(feature = "oracle-async")]
            AsyncDbDriver::OracleAsyncDriver(_) => DatabaseDialect::Oracle,

            #[cfg(feature = "mssql-async")]
            AsyncDbDriver::MssqlAsyncDriver(_) => DatabaseDialect::SQLServer,

            #[cfg(feature = "sqlite-async")]
            AsyncDbDriver::SqliteAsyncDriver(_) => DatabaseDialect::SQLite,

            _ => DatabaseDialect::MySQL,
        }
    }

    /// Determine if the database is SQL Server
    fn is_sql_server(&self) -> bool {
        matches!(self.dialect(), DatabaseDialect::SQLServer)
    }

    /// Calculate the safe batch size for SQL Server
    fn calculate_sqlserver_chunk_size(&self, data: &BatchInsertData) -> usize {
        // Count the number of nonincrementing fields
        let column_count = data.columns.iter()
            .filter(|c| c.exist && !c.is_auto_increment())
            .count();

        if column_count == 0 {
            return 0;
        }

        // SQL Server maximum parameter limit
        const MAX_PARAMS: usize = 2100;

        // Calculate the maximum number of rows per batch
        let max_rows = MAX_PARAMS / column_count;

        // Set reasonable limits: minimum 1 row, maximum 100 rows (to avoid single query size)
        max_rows.max(1).min(100)
    }

    /// Perform smart bulk inserts for SQL Server
    async fn save_batch_for_sqlserver(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
        let column_count = data.columns.iter()
            .filter(|c| c.exist && !c.is_auto_increment())
            .count();
        if column_count == 0 {
            return Err(AkitaError::EmptyData);
        }

        let total_params = column_count * data.rows.len();
        const MAX_PARAMS: usize = 2100;

        if total_params <= MAX_PARAMS {
            // The parameter is within the limit and is executed directly
            self.execute_single_batch(data).await
        } else {
            // Parameter exceeds limit, need to be chunked
            self.execute_chunked_batches(data).await
        }
    }

    async fn execute_single_batch(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
        let sql_builder = self.sql_builder();
        let (sql, params) = sql_builder.build_batch_insert_sql(data)?;

        // Executed in a transaction
        let _ = self.start().await?;
        let _result = self.execute(&sql, params.into()).await?;
        let _ = self.commit().await?;
        Ok(())
    }

    // Execute chunked batch (parameter exceeds limit)
    async fn execute_chunked_batches(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
        let sql_builder = self.sql_builder();
        let column_count = data.columns.iter()
            .filter(|c| c.exist && !c.is_auto_increment())
            .count();

        if column_count == 0 {
            return Err(AkitaError::EmptyData);
        }

        const MAX_PARAMS: usize = 2100;
        let max_rows_per_batch = MAX_PARAMS / column_count;

        if max_rows_per_batch == 0 {
            return Err(AkitaError::DatabaseError(format!(
                "Too many columns ({}) for SQL Server batch insert",
                column_count
            )));
        }
        // All batches are executed in a transaction
        let _ = self.start().await?;
        for chunk in data.rows.chunks(max_rows_per_batch) {
            // Create sub-batches of data
            let chunk_data = BatchInsertData {
                table: data.table.clone(),
                columns: data.columns.clone(),
                rows: chunk.to_vec(),
                id_field: data.id_field.clone(),
            };

            // Build and execute SQL
            let (sql, params) = sql_builder.build_batch_insert_sql(&chunk_data)?;
            self.execute(&sql, params.into()).await?;

        }

        self.commit().await?;
        Ok(())
    }
}

pub(crate) fn get_tokio_context() -> crate::errors::Result<Handle> {
    let handle = match Handle::try_current() {
        Ok(h) => {
            tracing::debug!("Creating a connection pool -in the context of Tokio, the runtime type: {:?}", h.runtime_flavor());
            h
        }
        Err(_) => {
            return Err(AkitaError::DatabaseError(
                "The connection pool must be created in the Tokio asynchronous context. Make sure：\n\
                 1. Called from the #[tokio::main] or #[tokio::test] function\n\
                 Called in the Tokio runtime environment".to_string()
            ));
        }
    };
    Ok(handle)
}