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
//! MySQL modules.
//!

mod adapter;
mod connection;

pub use adapter::*;
pub use connection::*;



use akita_core::{cfg_if, AkitaValue, ColumnInfo, ColumnSpecification, FieldName, FromAkitaValue, IntoAkitaValue, OperationType, Params, Rows, SchemaContent, SqlInjectionDetector, SqlSecurityConfig, SqlType, TableInfo, TableName};
use std::sync::Arc;
use akita_core::comm::extract_datatype_with_capacity;
use akita_derive::FromValue;
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::driver::blocking::{DbExecutor};
use crate::errors::AkitaError;
use crate::interceptor::blocking::InterceptorChain;
use crate::prelude::AkitaConfig;

cfg_if! {if #[cfg(feature = "auth")]{
    use crate::driver::blocking::{DbManager};
    use crate::driver::{DataBaseUser, GrantUserPrivilege, Role, UserInfo};
}}

pub struct MySQL {
    adapter: MysqlAdapter,
    database: Option<String>,
    interceptor_chain: Option<Arc<InterceptorChain>>,
    sql_injection_detector: Option<SqlInjectionDetector>,
}

impl MySQL {
    pub fn new(conn: MysqlConnection) -> Self {
        Self {
            adapter: MysqlAdapter::new(conn),
            interceptor_chain: None,
            sql_injection_detector: None,
            database: None,
        }
    }

    /// Set up the interceptor chain
    pub fn with_interceptor_chain(mut self, interceptor_chain: Arc<InterceptorChain>) -> Self {
        self.interceptor_chain = Some(interceptor_chain);
        self
    }
    
    /// Set up SQL security configuration
    pub fn with_sql_security(mut self, sql_security_config: Option<SqlSecurityConfig>) -> Self {
        if let Some(sql_security_config) = sql_security_config {
            self.sql_injection_detector = Some(SqlInjectionDetector::with_config(sql_security_config));
        }
        self
    }
    
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }
    
    pub fn database(&self) -> Option<&String> {
        self.database.as_ref()
    }

    /// Get a clone of the interceptor chain
    pub fn interceptor_chain(&self) -> Option<Arc<InterceptorChain>> {
        self.interceptor_chain.clone()
    }

    /// Execute queries with interceptors
    fn execute_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<ExecuteResult> {
        // Create a query context
        let mut ctx = ExecuteContext::new(sql.to_string(), params, TableName::parse_table_name(sql), OperationType::detect_operation_type(sql));
        // Record parsing begins
        ctx.record_parse_complete();

        // If there is an interceptor chain, perform a pre-intercept
        if let Some(chain) = &self.interceptor_chain {
            // Perform pre-interception synchronously
            if let Err(e) = chain.before_query(&mut ctx) {
                return Err(e);
            }

            if ctx.stop_propagation {
                // If the interceptor stops propagating, returns an empty result
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(ExecuteResult::None);
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }
        
        // Execute the query
        let mut result = self.adapter.execute(ctx.final_sql(), ctx.final_params().clone());
        
        // Record the number of affected rows
        if let Ok(_rows) = &result {
            ctx.set_connection_id(self.adapter.connection_id());
            // Record execution complete
            let rows_affected = self.adapter.affected_rows();
            ctx.record_execute_complete(rows_affected);
        }
        // If there is an interceptor chain, perform a post-interception
        if let Some(chain) = &self.interceptor_chain {
            // Perform post-intercepts synchronously
            if let Err(e) = chain.after_query(&mut ctx, &mut result) {
                return Err(e);
            }
        }

        // Record query metrics
        ctx.record_query_metrics();
        
        result
    }


    fn query_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<Rows> {
        // Create a query context
        let mut ctx = ExecuteContext::new(sql.to_string(), params, TableName::parse_table_name(sql), OperationType::detect_operation_type(sql));
        // Record parsing begins
        ctx.record_parse_complete();

        // If there is an interceptor chain, perform a pre-intercept
        if let Some(chain) = &self.interceptor_chain {
            // Perform pre-interception synchronously
            if let Err(e) = chain.before_query(&mut ctx) {
                return Err(e);
            }

            if ctx.stop_propagation {
                // If the interceptor stops propagating, returns an empty result
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(Rows::new());
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }

        // Execute the query
        let mut result = self.adapter.query(ctx.final_sql(), ctx.final_params().clone()).map(ExecuteResult::Rows);

        // Record the number of affected rows
        if let Ok(_rows) = &result {
            ctx.set_connection_id(self.adapter.connection_id());
            // Record execution complete
            let rows_affected = self.adapter.affected_rows();
            ctx.record_execute_complete(rows_affected);
        }
        // If there is an interceptor chain, perform a post-interception
        if let Some(chain) = &self.interceptor_chain {
            // Perform post-intercepts synchronously
            if let Err(e) = chain.after_query(&mut ctx, &mut result) {
                return Err(e);
            }
        }

        // Record query metrics
        ctx.record_query_metrics();

        result.map(|v| v.rows())
    }
}


impl DbExecutor for MySQL {
    fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        self.query_with_interceptors(sql, params)
    }

    fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        self.execute_with_interceptors(sql, params)
    }

    fn start(&self) -> crate::prelude::Result<()> {
        self.adapter.start_transaction()
    }

    fn commit(&self) -> crate::prelude::Result<()> {
        self.adapter.commit_transaction()
    }

    fn rollback(&self) -> crate::prelude::Result<()> {
        self.adapter.rollback_transaction()
    }
    
    fn affected_rows(&self) -> u64 {
        self.adapter.affected_rows()
    }

    fn last_insert_id(&self) -> u64 {
        self.adapter.last_insert_id()
    }

}

#[cfg(feature = "auth")]
impl DbManager for MySQL {

    fn get_table(&self, table_name: &TableName) -> crate::errors::Result<Option<TableInfo>> {
        #[derive(Debug, FromValue)]
        struct TableSpec {
            schema: String,
            name: String,
            comment: String,
            is_view: i32,
        }
        let schema: &AkitaValue = &table_name
            .schema
            .as_ref()
            .map(String::as_str)
            .unwrap_or("__DUMMY__")
            .into();
        let table_name: AkitaValue = table_name.name.clone().into();

        let mut tables: Vec<TableSpec> = self
            .query_with_interceptors(
                r#"
            SELECT TABLE_SCHEMA AS `schema`,
                   TABLE_NAME AS name,
                   TABLE_COMMENT AS comment,
                   CASE TABLE_TYPE WHEN 'VIEW' THEN TRUE ELSE FALSE END AS is_view
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_SCHEMA = CASE ? WHEN '__DUMMY__' THEN DATABASE() ELSE ? END AND TABLE_NAME = ?"#,
                (
                    schema, schema,
                    &table_name,
                ).into(),
            )?
            .iter()
            .map(|data| FromAkitaValue::from_value(&data.as_object()))
            .collect();
        let table_spec = match tables.len() {
            0 => return Err(AkitaError::DataError("Unknown table found.".to_string())),
            _ => tables.remove(0),
        };

        #[derive(Debug, FromValue)]
        struct ColumnSpec {
            schema: String,
            table_name: String,
            name: String,
            comment: String,
            type_: String,
        }
        let table_schema: AkitaValue =  table_spec.schema.clone().into();
        let columns: Vec<ColumnInfo> = self
            .query_with_interceptors(
                r#"
                SELECT TABLE_SCHEMA AS `schema`,
                       TABLE_NAME AS table_name,
                       COLUMN_NAME AS name,
                       COLUMN_COMMENT AS comment,
                       CAST(COLUMN_TYPE as CHAR(255)) AS type_
                  FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"#,
                (&table_schema, &table_name).into(),
            )?
            .iter()
            .map(|data| FromAkitaValue::from_value(&data.as_object()))
            .map(|spec: ColumnSpec| {
                let (sql_type, capacity) =
                    if spec.type_.starts_with("enum(") || spec.type_.starts_with("set(") {
                        let start = spec.type_.find('(');
                        let end = spec.type_.find(')');
                        if let (Some(start), Some(end)) = (start, end) {
                            let dtype = &spec.type_[0..start];
                            let range = &spec.type_[start + 1..end];
                            let choices = range
                                .split(',')
                                .map(|v| v.to_owned())
                                .collect::<Vec<String>>();

                            match dtype {
                                "enum" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                "set" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                _ => panic!("not yet handled: {}", dtype),
                            }
                        } else {
                            panic!("not yet handled spec_type: {:?}", spec.type_)
                        }
                    } else {
                        let (dtype, capacity) = extract_datatype_with_capacity(&spec.type_);
                        let sql_type = match &*dtype {
                            "tinyint" | "tinyint unsigned" => SqlType::Tinyint,
                            "smallint" | "smallint unsigned" | "year" => SqlType::Smallint,
                            "mediumint" | "mediumint unsigned" => SqlType::Int,
                            "int" | "int unsigned" => SqlType::Int,
                            "bigint" | "bigint unsigned" => SqlType::Bigint,
                            "float" | "float unsigned" => SqlType::Float,
                            "double" | "double unsigned" => SqlType::Double,
                            "decimal" => SqlType::Numeric,
                            "tinyblob" => SqlType::Tinyblob,
                            "mediumblob" => SqlType::Mediumblob,
                            "blob" => SqlType::Blob,
                            "longblob" => SqlType::Longblob,
                            "binary" | "varbinary" => SqlType::Varbinary,
                            "char" => SqlType::Char,
                            "varchar" => SqlType::Varchar,
                            "tinytext" => SqlType::Tinytext,
                            "mediumtext" => SqlType::Mediumtext,
                            "text" | "longtext" => SqlType::Text,
                            "date" => SqlType::Date,
                            "datetime" | "timestamp" => SqlType::Timestamp,
                            "time" => SqlType::Time,
                            "json" => SqlType::Json,
                            _ => panic!("not yet handled: {}", dtype),
                        };

                        (sql_type, capacity)
                    };

                ColumnInfo {
                    table: TableName::from(&format!("{}.{}", spec.schema, spec.table_name)),
                    name: FieldName::from(&spec.name),
                    comment: Some(spec.comment),
                    specification: ColumnSpecification {
                        capacity,
                        // TODO: implementation
                        constraints: vec![],
                        sql_type,
                    },
                    stat: None,
                }
            })
            .collect();

        Ok(Some(TableInfo {
            name: TableName {
                name: table_spec.name,
                schema: Some(table_spec.schema),
                alias: None,
                ignore_interceptors: Default::default(),
            },
            comment: Some(table_spec.comment),
            columns,
            is_view: table_spec.is_view == 1,
            // TODO: implementation
            table_key: vec![],
        }))
    }

    fn exist_table(&self, table_name: &TableName) -> crate::errors::Result<bool> {
        let sql = "SELECT count(1) as count FROM information_schema.tables WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?";
        self.query_with_interceptors(&sql, (&table_name.name, &table_name.schema).into()).map(|rows| {
            rows.iter().next()
                .map(|row| {
                    row.get_by_column::<i32>("count")
                        .expect("must not error")
                }).unwrap_or_default() > 0
        })
    }

    fn get_grouped_tables(&self) -> crate::errors::Result<Vec<SchemaContent>> {
        let table_names = get_table_names(&self, &"BASE TABLE".to_string())?;
        let view_names = get_table_names(&self, &"VIEW".to_string())?;
        let mut schema_contents: Vec<SchemaContent> = Vec::new();
        for table in table_names.iter() {
            let schema = table.schema.to_owned().unwrap_or_default();
            if let Some(t) = schema_contents.iter_mut().find(|data| data.to_owned().schema.to_owned().eq(&schema)) {
                t.tablenames.push(table.to_owned());
            } else {
                schema_contents.push(SchemaContent {
                    schema,
                    tablenames: vec![table.to_owned()],
                    views: vec![],
                });
            }
        }

        for table in view_names.iter() {
            let schema = table.schema.to_owned().unwrap_or_default();
            if let Some(t) = schema_contents.iter_mut().find(|data| data.to_owned().schema.to_owned().eq(&schema)) {
                t.tablenames.push(table.to_owned());
            } else {
                schema_contents.push(SchemaContent {
                    schema,
                    tablenames: vec![table.to_owned()],
                    views: vec![],
                });
            }
        }

        Ok(schema_contents)
    }

    fn get_all_tables(&self, schema: &str) -> crate::errors::Result<Vec<TableInfo>> {
        let tablenames = self.get_table_names(schema)?;
        Ok(tablenames
            .iter()
            .filter_map(|tablename| self.get_table(tablename).ok().flatten())
            .collect())
    }

    fn get_table_names(&self, schema: &str) -> crate::errors::Result<Vec<TableName>> {
        #[derive(Debug, FromValue)]
        struct TableNameSimple {
            table_name: String,
        }
        let sql =
            "SELECT TABLE_NAME as table_name FROM information_schema.tables WHERE TABLE_SCHEMA = ?";

        let result: Vec<TableNameSimple> = self
            .query_with_interceptors(sql, (schema, ).into())?
            .iter()
            .map(|row| TableNameSimple {
                table_name: row.get_by_column("table_name").expect("must have a table name"),
            })
            .collect();
        let tablenames = result
            .iter()
            .map(|r| TableName::from(&r.table_name))
            .collect();
        Ok(tablenames)
    }


    fn get_users(&self) -> crate::errors::Result<Vec<DataBaseUser>> {
        let sql = "SELECT USER as username FROM information_schema.user_attributes";
        let rows: crate::errors::Result<Rows> = self.query_with_interceptors(&sql, Params::None);
        rows.map(|rows| {
            rows.iter()
                .map(|row| DataBaseUser {
                    sysid: None,
                    username: row.get_by_column("username").expect("username"),
                    //TODO: join to the user_privileges tables
                    is_superuser: false,
                    is_inherit: false,
                    can_create_db: false,
                    can_create_role: false,
                    can_login: false,
                    can_do_replication: false,
                    can_bypass_rls: false,
                    valid_until: None,
                    conn_limit: None,
                })
                .collect()
        })
    }

    fn exist_user(&self, user: &UserInfo) -> crate::errors::Result<bool> {
        let sql = "SELECT count(1) as count FROM mysql.user where User = ? and Host = ?";
        self.query_with_interceptors(&sql, (&user.username, user.host.as_ref().unwrap_or(&"localhost".to_owned())).into()).map(|rows| {
            rows.iter().next()
                .map(|row| {
                    row.get_by_column::<i32>("count")
                        .expect("must not error")
                }).unwrap_or_default() > 0
        })
    }

    fn get_user_detail(&self, username: &str) -> crate::errors::Result<Vec<DataBaseUser>> {
        Ok(vec![])
    }

    fn get_roles(&self, username: &str) -> crate::errors::Result<Vec<Role>> {
        Ok(vec![])
    }

    fn create_user(&self, user: &UserInfo) -> crate::errors::Result<()> {
        let mut sql = format!("create user '{}'@'{}'", &user.username, &user.host.to_owned().unwrap_or("localhost".to_string()));
        if let Some(password) = user.password.to_owned() {
            sql.push_str(&format!("identified by '{}'", password));
        }
        if let Some(is_lock) = user.is_lock {
            if is_lock {
                sql.push_str("account lock")
            }
        }
        sql.push_str(";");
        // Creating a user
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn drop_user(&self, user: &UserInfo) -> crate::errors::Result<()> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("drop user '{}'@'{}';", &user.username, &user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn update_user_password(&self, user: &UserInfo) -> crate::errors::Result<()> {
        if user.username.is_empty() || user.host.is_none() || user.password.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' identified by '{}'", user.username, user.host.to_owned().unwrap_or("localhost".to_string()), user.password.to_owned().unwrap_or_default());
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn lock_user(&self, user: &UserInfo) -> crate::errors::Result<()> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' account lock;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn unlock_user(&self, user: &UserInfo) -> crate::errors::Result<()> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' account unlock;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn expire_user_password(&self, user: &UserInfo) -> crate::errors::Result<()> {
        if user.username.is_empty() || user.host.is_none() || user.password.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' password expire;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn grant_privileges(&self, user: &GrantUserPrivilege) -> crate::errors::Result<()> {
        // Assigning permissions
        if user.schema.is_empty() || user.table.is_empty() || user.username.is_empty() || user.host.is_none(){
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let privileges = if user.privileges.len() > 0 {
            user.privileges.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",")
        } else {
            "all".to_string()
        };
        if user.schema.eq("*") {
            return Err(AkitaError::UnsupportedOperation(
                "You are not allow this operation to use schema with *".to_string(),
            ))
        }
        let sql = format!("grant {} on {}.{} to '{}'@'{}';", privileges, user.schema, user.table, user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn revoke_privileges(&self, user: &GrantUserPrivilege) -> crate::errors::Result<()> {
        // Reclaim permissions
        if user.schema.is_empty() || user.table.is_empty() || user.username.is_empty() || user.host.is_none(){
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let privileges = if user.privileges.len() > 0 {
            user.privileges.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",")
        } else {
            "all".to_string()
        };
        if user.schema.eq("*") {
            return Err(AkitaError::UnsupportedOperation(
                "You are not allow this operation to use schema with *".to_string(),
            ))
        }
        let sql = format!("revoke {} on {}.{} from '{}'@'{}';", privileges, user.schema, user.table, user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }

    fn flush_privileges(&self) -> crate::errors::Result<()> {
        let sql = "flush privileges;";
        self.execute_with_interceptors(&sql, ().into()).map(|_|())
    }
}


#[cfg(feature = "auth")]
#[allow(unused)]
fn get_table_names(db: &MySQL, kind: &str) -> crate::errors::Result<Vec<TableName>> {
    #[derive(Debug, FromValue)]
    struct TableNameSimple {
        table_name: String,
        schema_name: String,
    }
    let sql = "SELECT TABLE_NAME as table_name, TABLE_SCHEMA as schema_name FROM information_schema.tables WHERE table_type= ?";
    let result: Vec<TableNameSimple> = db
        .query_with_interceptors(sql, (kind.into_value(),).into())?
        .iter()
        .map(|row| TableNameSimple {
            table_name: row.get_by_column("table_name").expect("must have a table name"),
            schema_name: row.get_by_column("schema_name").expect("must have a schema name"),
        })
        .collect();
    let mut table_names = vec![];
    for r in result {
        let mut table_name = TableName::from(&r.table_name);
        table_name.schema = r.schema_name.into();
        table_names.push(table_name);
    }
    Ok(table_names)
}