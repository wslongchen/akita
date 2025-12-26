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
use crate::comm::ExecuteResult;
use crate::errors::AkitaError;
use akita_core::{AkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector};
use rusqlite::types::{ToSqlOutput, Value};
use std::sync::{Arc, RwLock};
use bigdecimal::ToPrimitive;
use chrono::{Datelike, Timelike};
use rusqlite::{params_from_iter, ParamsFromIter, ToSql};
use tokio::sync::{Mutex};
use tokio::task;
use crate::driver::non_blocking::sqlite::SqliteAsyncConnection;

/// SQLite Asynchronous adapter
pub struct SqliteAsyncAdapter {
    conn: SqliteAsyncConnection,
}

impl SqliteAsyncAdapter {
    pub fn new(conn: SqliteAsyncConnection) -> Self {
        Self {
            conn,
        }
    }
    
    pub async fn start_transaction(&self) -> crate::prelude::Result<()> {
        self.conn.interact(|conn| {
            conn.execute("BEGIN TRANSACTION", [])?;
            Ok(())
        }).await?
    }

    pub async fn commit_transaction(&self) -> crate::prelude::Result<()> {
        self.conn.interact(|conn| {
            conn.execute("COMMIT", [])?;
            Ok(())
        }).await?
    }

    pub async fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        self.conn.interact(|conn| {
            conn.execute("ROLLBACK", [])?;
            Ok(())
        }).await?
    }

    pub async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        let sqlite_params = convert_to_sqlite_params(params);

        self.inner_query(sql, sqlite_params).await
    }

    async fn inner_query(&self, sql: &str, sqlite_params: Vec<Box<dyn ToSql + Sync + Send>>) -> crate::prelude::Result<Rows> {
        let sql = sql.to_string();
        self.conn.interact(move |conn| {
            let mut stmt = conn.prepare(&sql)?;

            let column_names = stmt.column_names().iter().map(ToString::to_string).collect::<Vec<_>>();
            let column_count = stmt.column_count();
            let mut rows = stmt.query(params_from_iter(sqlite_params.into_iter()))?;

            let mut result_rows = Vec::new();
            let mut columns = Vec::new();

            // Getting column names
            for i in 0..column_count {
                columns.push(column_names.get(i).unwrap_or(&"".to_string()).to_string());
            }

            while let Some(row) = rows.next()? {
                let mut values = Vec::new();
                for i in 0..column_count {
                    let raw = row.get(i);
                    if let Ok(raw) = raw {
                        let v = convert_sqlite_value(raw)?;
                        values.push(v);
                    }
                }
                result_rows.push(Row {
                    columns: columns.clone(),
                    data: values,
                });
            }

            Ok(Rows {
                data: result_rows,
                count: None,
            })
        }).await?
    }

    pub async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        let sqlite_params = convert_to_sqlite_params(params);
        let stmt_type = OperationType::detect_operation_type(sql);

        match stmt_type {
            OperationType::Select => {
                self.inner_query(sql, sqlite_params).await.map(ExecuteResult::Rows)
            }
            _ => {
                let sql_clone = sql.to_string();
                self.conn.interact(move |conn| {
                    let mut stmt = conn.prepare(&sql_clone)?;
                    let changes = stmt.execute(params_from_iter(sqlite_params.into_iter()))?;

                    Ok(ExecuteResult::AffectedRows(changes as u64))
                }).await?
            }
        }
    }

    pub async fn affected_rows(&self) -> u64 {
        0
    }

    pub async fn connection_id(&self) -> u32 {
        0
    }

    pub async fn last_insert_id(&self) -> u64 {
        self.conn.interact(|conn| {
            conn.last_insert_rowid() as u64
        }).await.unwrap_or(0)
    }

    pub async fn ping(&self) -> crate::prelude::Result<()> {
        self.conn.interact(|conn| {
            // SQLite does not have a ping command and performs simple queries
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            Ok(())
        }).await?
    }

    pub async fn is_valid(&self) -> crate::prelude::Result<bool> {
        match self.ping().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
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