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
use akita_core::{cfg_if, AkitaValue, FieldName, FieldType, FromAkitaValue, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, Rows, Wrapper};
use crate::comm::ExecuteResult;
use crate::core::GLOBAL_GENERATOR;
use crate::key::IdentifierGenerator;
use crate::errors::AkitaError;
use crate::mapper::blocking::AkitaMapper;
use crate::mapper::IPage;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder, SqlBuilderFactory};

cfg_if! {
    if #[cfg(feature = "mysql-sync")] {
        pub mod mysql;
        use crate::driver::blocking::mysql::MySQL;
    }
}

cfg_if! {
    if #[cfg(feature = "postgres-sync")] {
        pub mod postgres;
        use crate::driver::blocking::postgres::Postgres;
    }
}

cfg_if! {
    if #[cfg(feature = "oracle-sync")] {
        pub mod oracle;
        use crate::driver::blocking::oracle::Oracle;
    }
}

cfg_if! {
    if #[cfg(feature = "sqlite-sync")] {
        pub mod sqlite;
        use crate::driver::blocking::sqlite::Sqlite;
    }
}

cfg_if! {
    if #[cfg(feature = "mssql-sync")] {
        pub mod mssql;
        use crate::driver::blocking::mssql::Mssql;
    }
}



pub trait DbExecutor {
    fn start(&self) -> crate::errors::Result<()>;

    fn commit(&self) -> crate::errors::Result<()>;

    fn rollback(&self) -> crate::errors::Result<()>;

    fn query(&self, sql: &str, param: Params) -> crate::errors::Result<Rows>;

    fn execute(&self, sql: &str, param: Params) -> crate::errors::Result<ExecuteResult>;

    fn affected_rows(&self) -> u64 { 0 }

    fn last_insert_id(&self) -> u64 { 0 }
}


pub enum DbDriver {
    #[cfg(feature = "mysql-sync")]
    MysqlDriver(Box<MySQL>),
    #[cfg(feature = "sqlite-sync")]
    SqliteDriver(Box<Sqlite>),
    #[cfg(feature = "postgres-sync")]
    PostgresDriver(Box<Postgres>),
    #[cfg(feature = "oracle-sync")]
    OracleDriver(Box<Oracle>),
    #[cfg(feature = "mssql-sync")]
    MssqlDriver(Box<Mssql>),
}

impl Deref for DbDriver {
    type Target = dyn DbExecutor;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(ref mysql) => mysql.deref(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(ref sqlite) => sqlite.deref(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(ref postgres) => postgres.deref(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(ref oracle) => oracle.deref(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(ref mssql) => mssql.deref(),
        }
    }
}

impl std::ops::DerefMut for DbDriver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(ref mut mysql) => mysql.deref_mut(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(ref mut sqlite) => sqlite.deref_mut(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(ref mut postgres) => postgres.deref_mut(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(ref mut mssql) => mssql.deref_mut(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(ref mut oracle) => oracle.deref_mut(),
        }
    }
}

impl DbExecutor for DbDriver {
     fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.query(sql, params),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.query(sql, params),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.query(sql, params),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.query(sql, params),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.query(sql, params),
            _ => Err(AkitaError::Unknown),
        }
    }

    fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.execute(sql, params),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.execute(sql, params),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.execute(sql, params),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.execute(sql, params),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.execute(sql, params),
            _ => Err(AkitaError::Unknown),
        }
    }

    fn start(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.start(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.start(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.start(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.start(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.start(),
            _ => Err(AkitaError::Unknown),
        }
    }

    fn commit(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.commit(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.commit(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.commit(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.commit(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.commit(),
            _ => Err(AkitaError::Unknown),
        }
    }

    fn rollback(&self) -> crate::prelude::Result<()> {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.rollback(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.rollback(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.rollback(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.rollback(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.rollback(),
            _ => Err(AkitaError::Unknown),
        }
    }

    fn affected_rows(&self) -> u64 {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.affected_rows(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.affected_rows(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.affected_rows(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.affected_rows(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.affected_rows(),
            _ => 0,
        }
    }

    fn last_insert_id(&self) -> u64 {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(driver) => driver.last_insert_id(),
            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(driver) => driver.last_insert_id(),
            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(driver) => driver.last_insert_id(),
            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(driver) => driver.last_insert_id(),
            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(driver) => driver.last_insert_id(),
            _ => 0,
        }
    }
}



#[allow(unreachable_patterns,unused)]
impl AkitaMapper for DbDriver {
    // ========== Query actions ==========

    /// Get all the table of records
    fn list<T>(&self, wrapper: Wrapper) -> crate::errors::Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let sql_builder = self.sql_builder();
        let wrapper = wrapper.table(&table.complete_name());
        let (sql, params) = sql_builder.build_query_sql(&wrapper);

        let rows = self.query(&sql, params.into())?;
        let mut entities = Vec::new();

        for data in rows.object_iter() {
            entities.push(T::from_value(&data));
        }
        Ok(entities)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper: Wrapper) -> crate::errors::Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
    {
        let mut results = self.list::<T>(wrapper)?;
        Ok(results.pop())
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> crate::errors::Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
        I: Into<AkitaValue>,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;
        let wrapper = Wrapper::new()
            .table(&T::table_name().complete_name())
            .eq(id_field.name, id.into())
            .limit(1);

        self.select_one::<T>(wrapper)
    }

    /// Get table of records with page
    fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> crate::errors::Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let sql_builder = self.sql_builder();
        let wrapper = wrapper.table(&table.complete_name());

        // Count total
        let count_sql = sql_builder.build_count_sql(&wrapper);
        let count_params = wrapper.get_parameters();
        let count: u64 = self.exec_first(&count_sql, count_params)?;

        let mut page_result = IPage::new(page, size, count, Vec::new());

        if page_result.total > 0 {
            // 构建分页查询
            let wrapper = wrapper
                .limit(page_result.size)
                .offset(page_result.offset());

            let (sql, params) = sql_builder.build_query_sql(&wrapper);
            let rows = self.query(&sql, params.into())?;

            for data in rows.object_iter() {
                page_result.records.push(T::from_value(&data));
            }
        }

        Ok(page_result)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper: Wrapper) -> crate::errors::Result<u64>
    where
        T: GetTableName + GetFields,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let sql_builder = self.sql_builder();
        let wrapper = wrapper.table(&table.complete_name());
        let sql = sql_builder.build_count_sql(&wrapper);
        let params = wrapper.get_parameters();

        self.exec_first(&sql, params)
    }

    // ========== Delete action ==========

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper: Wrapper) -> crate::errors::Result<u64>
    where
        T: GetTableName + GetFields,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let sql_builder = self.sql_builder();
        let wrapper = wrapper.table(&table.complete_name());
        let sql = sql_builder.build_delete_sql(&table, &wrapper);

        let params = wrapper.get_parameters();
        let result = self.execute(&sql, params.into())?;

        Ok(result.affected_rows())
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> crate::errors::Result<u64>
    where
        I: Into<AkitaValue>,
        T: GetTableName + GetFields,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;
        let wrapper = Wrapper::new()
            .table(&T::table_name().complete_name())
            .eq(id_field.name, id.into());

        self.remove::<T>(wrapper)
    }

    /// Remove the records by multiple ids.
    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> crate::errors::Result<u64>
    where
        I: Into<AkitaValue>,
        T: GetTableName + GetFields,
    {
        if ids.is_empty() {
            return Ok(0);
        }
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let id_values: Vec<AkitaValue> = ids.into_iter().map(|id| id.into()).collect();
        let wrapper = Wrapper::new()
            .table(&T::table_name().complete_name())
            .r#in(id_field.name, id_values);

        self.remove::<T>(wrapper)
    }


    // ========== Update actions ==========

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> crate::errors::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let data = entity.into_value();
        let columns = T::fields();
        let sql_builder = self.sql_builder();

        // Construct the SET clause
        let mut set_wrapper = Wrapper::new().table(&table.complete_name());
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
        final_wrapper.where_conditions(wrapper.get_where_conditions().clone());
        let sql = sql_builder.build_update_sql(&table, &final_wrapper)
            .ok_or_else(|| AkitaError::InvalidSQL("Invalid Update SQL.".to_string()))?;

        let params = final_wrapper.get_parameters();
        let result = self.execute(&sql, params.into())?;

        Ok(result.affected_rows())
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> crate::errors::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
    {
        let sql_builder = self.sql_builder();
        let id_field = sql_builder.find_id_field(T::fields())
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let data = entity.into_value();
        let id_value = data.get_obj_value(&id_field.name)
            .ok_or_else(|| AkitaError::MissingIdent("Missing id value".to_string()))?;

        let wrapper = Wrapper::new().eq(id_field.name, id_value.clone());
        self.update(entity, wrapper)
    }

    /// Update multiple records by ids.
    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> crate::errors::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
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

        let result = self.execute(&sql, Params::None)?;
        Ok(result.affected_rows())
    }
    // ========== Insert action ==========

    /// Insert a single record
    fn save<T, I>(&self, entity: &T) -> crate::errors::Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue,
    {
        let sql_builder = self.sql_builder();
        let columns = T::fields();
        let data = entity.into_value();
        let (sql, params) = sql_builder.build_insert_sql(&T::table_name(), columns, vec![data])?;
        let _result = self.execute(&sql, params.into())?;
        // Process the returned ID
        let last_insert_id = self.last_insert_id();
        Ok(I::from_value_opt(&AkitaValue::from(last_insert_id)).ok())
    }

    /// Save or update record
    fn save_or_update<T, I>(&self, entity: &T) -> crate::errors::Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue,
    {
        let sql_builder = self.sql_builder();
        let data = entity.into_value();
        let id_field = sql_builder.find_id_field(T::fields());

        if let Some(field) = id_field {
            if let Some(id_value) = data.get_obj_value(&field.name) {
                if !matches!(id_value, AkitaValue::Null) {
                    self.update_by_id(entity)?;
                    return Ok(I::from_value_opt(id_value).ok());
                }
            }
        }

        // Add
        self.save(entity)
    }

    /// Insert multiple records
    fn save_batch<T, E>(&self, entities: E) -> crate::errors::Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        E: IntoIterator<Item = T>
    {
        let entities: Vec<T> = entities.into_iter().collect();

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

        let id_field = insert_columns.iter()
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
            let _result = self.save_batch_for_sqlserver(&batch_data)?;
        } else if self.is_oracle() {
            let _result = self.save_batch_for_oracle(&batch_data)?;
        } else {
            // Other databases: Handle them directly
            let (sql, params) = sql_builder.build_batch_insert_sql(&batch_data)?;
            let _result = self.execute(&sql, params.into())?;
        }
        
        Ok(())
        
    }

    // ========== Other methods ==========

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> crate::errors::Result<Rows> {
        let sql_builder = self.sql_builder();
        let sql = sql_builder.process_placeholders(&sql.into());
        self.query(&sql, params.into())
    }

}

impl DbDriver {

    pub fn sql_builder(&self) -> Box<dyn SqlBuilder> {
        SqlBuilderFactory::create(self.dialect())
    }

    /// Gets the database dialect type
    pub fn dialect(&self) -> DatabaseDialect {
        match self {
            #[cfg(feature = "mysql-sync")]
            DbDriver::MysqlDriver(_) => DatabaseDialect::MySQL,

            #[cfg(feature = "postgres-sync")]
            DbDriver::PostgresDriver(_) => DatabaseDialect::Postgres,

            #[cfg(feature = "oracle-sync")]
            DbDriver::OracleDriver(_) => DatabaseDialect::Oracle,

            #[cfg(feature = "mssql-sync")]
            DbDriver::MssqlDriver(_) => DatabaseDialect::SQLServer,

            #[cfg(feature = "sqlite-sync")]
            DbDriver::SqliteDriver(_) => DatabaseDialect::SQLite,

            _ => DatabaseDialect::MySQL,
        }
    }

    /// Determine if the database is SQL Server
    fn is_sql_server(&self) -> bool {
        matches!(self.dialect(), DatabaseDialect::SQLServer)
    }

    /// Determine if the database is oracle
    fn is_oracle(&self) -> bool {
        matches!(self.dialect(), DatabaseDialect::Oracle)
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
    fn save_batch_for_sqlserver(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
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
            self.execute_single_batch(data)
        } else {
            // Parameter exceeds limit, need to be chunked
            self.execute_chunked_batches(data)
        }
    }
    
    fn save_batch_for_oracle(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
        self.execute_single_batch(data)
    }

    fn execute_single_batch(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
        let sql_builder = self.sql_builder();
        let (sql, params) = sql_builder.build_batch_insert_sql(data)?;

        // Executed in a transaction
        let _ = self.start().unwrap();
        let result = self.execute(&sql, params.into());
        match result {
            Ok(_res) => {
                let _ = self.commit()?;
            }
            Err(_err) => {
                let _ = self.rollback()?;
            }
        }
        Ok(())
    }

    // Execute chunked batch (parameter exceeds limit)
    fn execute_chunked_batches(&self, data: &BatchInsertData) -> crate::errors::Result<()> {
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
        let _ = self.start()?;
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
            self.execute(&sql, params.into())?;
        }
        self.commit()?;
        Ok(())
    }
}
