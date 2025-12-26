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
//! SQLite modules.
//!

mod connection;

pub use connection::*;

use bigdecimal::ToPrimitive;
use std::sync::Arc;
use chrono::{Datelike, Timelike};
use rusqlite::{params_from_iter, ToSql};
use rusqlite::types::{Value};
use akita_core::{AkitaValue, OperationType, Params, Rows, SqlInjectionDetector, SqlSecurityConfig, TableName};
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::driver::blocking::DbExecutor;
use crate::errors::{AkitaError, Result};
use crate::interceptor::blocking::InterceptorChain;

pub struct Sqlite {
    conn: SqliteConnection,
    interceptor_chain: Option<Arc<InterceptorChain>>,
    sql_injection_detector: Option<SqlInjectionDetector>,
    database: Option<String>,
}

impl Sqlite {
    pub fn new(conn: SqliteConnection) -> Self {
        Sqlite {
            conn,
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

    /// Get a clone of the interceptor chain
    pub fn interceptor_chain(&self) -> Option<Arc<InterceptorChain>> {
        self.interceptor_chain.clone()
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn database(&self) -> Option<&String> {
        self.database.as_ref()
    }


    /// Execute queries with interceptors
    fn _execute(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<ExecuteResult> {
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
        let mut result = self.inner_execute(ctx.final_sql(), ctx.final_params().clone());

        // Record the number of affected rows
        if let Ok(_rows) = &result {
            // Record execution complete
            ctx.record_execute_complete(0);
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


    fn _query(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<Rows> {
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
        let mut result = self.inner_query(ctx.final_sql(), ctx.final_params().clone()).map(ExecuteResult::Rows);

        // Record the number of affected rows
        if let Ok(_rows) = &result {
            // Record execution complete
            ctx.record_execute_complete(0);
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

        result.map(|v|v.rows())
    }
    
    pub fn inner_execute(&self, sql: &str, params: Params) -> Result<ExecuteResult> {
        let stmt_type = OperationType::detect_operation_type(&sql);

        match stmt_type {
            OperationType::Select => {
                let rows = self.inner_query(sql, params)?;
                Ok(ExecuteResult::Rows(rows))
            }
            _ => {
                let stmt = self.conn.prepare(&sql);
                match stmt {
                    Ok(mut stmt) => {
                        let sqlite_params = convert_to_sqlite_params(params);
                        let affected_rows = stmt.execute(params_from_iter(sqlite_params.into_iter()))?;
                        Ok(ExecuteResult::AffectedRows(affected_rows as u64))
                    }
                    Err(e) => Err(AkitaError::from(e)),
                }
            }
        }

    }
    
    fn inner_query(&self, sql: &str, params: Params) -> Result<Rows> {
        let stmt = self.conn.prepare(&sql);
        let column_names = if let Ok(ref stmt) = stmt {
            stmt.column_names()
        } else {
            vec![]
        };
        let column_names: Vec<String> = column_names.iter().map(ToString::to_string).collect();
        match stmt {
            Ok(mut stmt) => {
                let column_count = stmt.column_count();
                let mut records = Rows::new();
                let sqlite_params = convert_to_sqlite_params(params);
                if let Ok(mut rows) = stmt.query(params_from_iter(sqlite_params.into_iter())) {
                    while let Some(row) = rows.next()? {
                        let mut record: Vec<AkitaValue> = vec![];
                        for i in 0..column_count {
                            let raw = row.get(i);
                            if let Ok(raw) = raw {
                                let v = convert_sqlite_value(raw)?;
                                record.push(v);
                            }
                        }
                        records.push(crate::prelude::Row{
                            columns: column_names.clone(),
                            data: record
                        });
                    }
                }
                Ok(records)
            }
            Err(e) => Err(AkitaError::from(e)),
        }
    }
}

/// SQLite data operations
#[allow(unused)]
impl DbExecutor for Sqlite {
    fn start(&self) -> Result<()> {
        self._execute("BEGIN TRANSACTION", Params::None).map(|_| ())
    }

    fn commit(&self) -> Result<()> {
        self._execute("COMMIT TRANSACTION", Params::None).map(|_| ())
    }

    fn rollback(&self) -> Result<()> {
        self._execute("ROLLBACK TRANSACTION", Params::None).map(|_| ())
    }
    
    fn query(&self, sql: &str, params: Params) -> Result<Rows> {
        self._query(sql, params)
    }

    fn execute(&self, sql: &str, params: Params) -> Result<ExecuteResult> {
        self._execute(sql, params)
    }
    
    fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_rowid() as u64
    }
}


fn convert_to_sqlite_params(params: Params) -> Vec<Box<dyn ToSql + Sync + Send>> {
    match params {
        Params::None => {
            vec![]
        },
        Params::Positional(param) => param.into_iter()
            .map(|val| {
                convert_value_to_to_sql(val)
            })
            .collect(),
        Params::Named(param) => {
            param.values().cloned().into_iter()
                .map(|val| {
                    convert_value_to_to_sql(val)
                })
                .collect()
        },
    }
}


fn convert_value_to_to_sql(value: AkitaValue) -> Box<dyn ToSql + Sync + Send> {
    match value {
        AkitaValue::Text(v) => Box::new(v),
        AkitaValue::Bool(v) => Box::new(if v { 1i64 } else { 0i64 }),
        AkitaValue::Tinyint(v) => Box::new(i64::from(v)),
        AkitaValue::Smallint(v) => Box::new(i64::from(v)),
        AkitaValue::Int(v) => Box::new(i64::from(v)),
        AkitaValue::Bigint(v) => Box::new(v),
        AkitaValue::Float(v) => Box::new(f64::from(v)),
        AkitaValue::Double(v) => Box::new(v),
        AkitaValue::BigDecimal(ref v) => match v.to_f64() {
            Some(v) => Box::new(v),
            None => Box::new(0.0),
        },
        AkitaValue::Blob(v) => Box::new(v),
        AkitaValue::Char(v) => Box::new(v.to_string()),
        AkitaValue::Json(v) => Box::new(v),
        AkitaValue::Uuid(v) => Box::new(v.to_string()),
        AkitaValue::Date(v) => {
            let formatted = format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day());
            Box::new(formatted)
        },
        AkitaValue::DateTime(v) => {
            let formatted = format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                v.year(),
                v.month(),
                v.day(),
                v.hour(),
                v.minute(),
                v.second()
            );
            Box::new(formatted)
        },
        AkitaValue::Time(v) => {
            let formatted = format!("{:02}:{:02}:{:02}", v.hour(), v.minute(), v.second());
            Box::new(formatted)
        },
        AkitaValue::Timestamp(v) => Box::new(v),
        AkitaValue::Null => Box::new(rusqlite::types::Null),

        _ => Box::new(value.to_string())
    }
}


fn convert_sqlite_value(value: rusqlite::types::Value) -> crate::prelude::Result<AkitaValue> {
    match value {
        rusqlite::types::Value::Null => Ok(AkitaValue::Null),
        rusqlite::types::Value::Integer(i) => Ok(AkitaValue::Bigint(i)),
        rusqlite::types::Value::Real(f) => Ok(AkitaValue::Double(f)),
        rusqlite::types::Value::Text(text) => {
            Ok(AkitaValue::Text(text))
        }
        rusqlite::types::Value::Blob(bytes) => {
            Ok(AkitaValue::Blob(bytes.to_vec()))
        }
    }
}