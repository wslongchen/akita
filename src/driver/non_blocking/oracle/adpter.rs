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
use crate::driver::non_blocking::oracle::OracleAsyncConnection;
use crate::errors::AkitaError;
use akita_core::{AkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector};
use std::sync::{Arc};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use oracle::sql_type::{OracleType, Timestamp};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
/// Oracle Asynchronous adapters (wrapper synchronous connections)
pub struct OracleAsyncAdapter {
    conn: OracleAsyncConnection,
    in_transaction: RwLock<bool>,
}

impl OracleAsyncAdapter {
    pub fn new(conn: OracleAsyncConnection) -> Self {
        Self {
            conn,
            in_transaction: RwLock::new(false),
        }
    }

    pub async fn start_transaction(&self) -> crate::prelude::Result<()> {
        // Oracle defaults to auto-commit, which needs to be set to manual commit
        let mut in_transaction = self.in_transaction.write().await;
        if !*in_transaction {
            self.conn.interact(|conn| {
                conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", &[])?;
                Ok::<(), AkitaError>(())
            }).await??;
            *in_transaction = true;
        }
        Ok(())
        
    }

    pub async fn commit_transaction(&self) -> crate::prelude::Result<()> {
        let mut in_transaction = self.in_transaction.write().await;
        if *in_transaction {
            self.conn.interact(|conn| {
                conn.commit()?;
                Ok::<(), AkitaError>(())
            }).await??;
            *in_transaction = false;
        }
        Ok(())
    }

    pub async fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        let mut in_transaction = self.in_transaction.write().await;
        if *in_transaction {
            self.conn.interact(|conn| {
                conn.rollback()?;
                Ok::<(), AkitaError>(())
            }).await??;
            *in_transaction = false;
        }
        Ok(())
       
    }

    pub async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        let sql = sql.to_string();
        
        self.conn.interact( move |conn| {
            // Prepare the statement
            let mut stmt = conn.statement(&sql).build().map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
            })?;
            // Getting column information
            let column_count = stmt.bind_names().len();
            let column_names: Vec<String> = stmt.bind_names()
                .iter()
                .map(|col| col.to_string())
                .collect();
            // Binding parameters
            bind_oracle_params(&mut stmt, &params)?;
            // Executing queries
            let rows = stmt.query(&[]).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to execute query: {}", e))
            })?;

            // Conversion result
            let mut records = Rows::new();
            for row_result in rows {
                let row = row_result.map_err(|e| {
                    AkitaError::DatabaseError(format!("Failed to fetch row: {}", e))
                })?;
                let mut record = Vec::new();
                for i in 0..column_count {
                    let value = get_value_from_oracle_row(&row, i)?;
                    record.push(value);
                }
                records.push(Row {
                    columns: column_names.clone(),
                    data: record,
                });
            }
            Ok(records)
        }).await?
    }

    pub async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        let sql = sql.to_string();
        let stmt_type = OperationType::detect_operation_type(&sql);
        match stmt_type {
            OperationType::Select => {
                self.conn.interact(move |conn| {
                    // Prepare the statement
                    let mut stmt = conn.statement(&sql).build().map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
                    })?;
                    // Getting column information
                    let column_count = stmt.bind_names().len();
                    let column_names: Vec<String> = stmt.bind_names()
                        .iter()
                        .map(|col| col.to_string())
                        .collect();
                    // Binding parameters
                    bind_oracle_params(&mut stmt, &params)?;
                    // Executing queries
                    let rows = stmt.query(&[]).map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to execute query: {}", e))
                    })?;

                    // Conversion result
                    let mut records = Rows::new();
                    for row_result in rows {
                        let row = row_result.map_err(|e| {
                            AkitaError::DatabaseError(format!("Failed to fetch row: {}", e))
                        })?;
                        let mut record = Vec::new();
                        for i in 0..column_count {
                            let value = get_value_from_oracle_row(&row, i)?;
                            record.push(value);
                        }
                        records.push(Row {
                            columns: column_names.clone(),
                            data: record,
                        });
                    }
                    Ok(ExecuteResult::Rows(records))
                }).await?
            }
            _ => {
                let in_transaction = *self.in_transaction.read().await;
                self.conn.interact(move |conn| {
                    // Prepare the statement
                    let mut stmt = conn.statement(&sql).build().map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
                    })?;
                    // Binding parameters
                    bind_oracle_params(&mut stmt, &params)?;
                    let _rows = stmt.execute(&[]).map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to execute query: {}", e))
                    })?;
                    // If not in the transaction, commit automatically
                    
                    if !in_transaction {
                        conn.commit()
                            .map_err(AkitaError::OracleError)?;
                    }
                    let rows_affected = stmt.row_count()?;
                    Ok(ExecuteResult::AffectedRows(rows_affected))
                }).await?
            }
        }
    }

    pub async fn affected_rows(&self) -> u64 {
        // Oracle The number of affected lines needs to be obtained immediately after execution
        0
    }

    pub async fn connection_id(&self) -> u32 {
        self.conn.interact(|conn| {
            let sql = "SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL";
            let row = conn.query_row_as::<u32>(sql, &[]).unwrap_or(0);
            row
        }).await.unwrap_or(0)
    }

    pub async fn last_insert_id(&self) -> u64 {
        // Oracle To use sequences, you need to use the RETURNING clause on insertion
        0
    }

    pub async fn ping(&self) -> crate::prelude::Result<()> {
        self.conn.interact(|conn| {
            conn.ping()?;
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



/// Convert named parameter SQL (Oracle uses :name format)
fn convert_named_params_sql(sql: &str, params: &Params) -> String {
    match params {
        Params::Named(named_params) => {
            let new_sql = sql.to_string();
            // Oracle already supports the :name format, so no conversion is required
            // The main thing here is to make sure the parameters exist
            for name in named_params.keys() {
                // Check if the parameter is included in SQL
                if !new_sql.contains(&format!(":{}", name)) {
                    tracing::warn!("Parameter :{} not found in SQL", name);
                }
            }
            new_sql
        }
        _ => sql.to_string(),
    }
}

/// Binding Oracle parameters
fn bind_oracle_params(stmt: &mut oracle::Statement, params: &Params) -> Result<(), AkitaError> {
    match params {
        Params::None => Ok(()),
        Params::Positional(param) => {
            for (i, value) in param.iter().enumerate() {
                bind_oracle_value(stmt, i, value)?;
            }
            Ok(())
        }
        Params::Named(param) => {
            for (name, value) in param.iter() {
                bind_oracle_value_by_name(stmt, name, value)?;
            }
            Ok(())
        }
    }
}

/// helper function that converts AkitaValue to Oracle argument values
fn convert_to_oracle_value(val: AkitaValue) -> Box<dyn oracle::sql_type::ToSql> {
    match val {
        AkitaValue::Text(v) => Box::new(v),
        AkitaValue::Bool(v) => {
            let int_val = if v { 1 } else { 0 };
            Box::new(int_val)
        }
        AkitaValue::Tinyint(v) => Box::new(v as i16),
        AkitaValue::Smallint(v) => Box::new(v),
        AkitaValue::Int(v) => Box::new(v),
        AkitaValue::Bigint(v) => Box::new(v),
        AkitaValue::Float(v) => Box::new(v),
        AkitaValue::Double(v) => Box::new(v),
        AkitaValue::BigDecimal(ref v) => Box::new(v.to_string()),
        AkitaValue::Blob(ref v) => Box::new(v.clone()),
        AkitaValue::Char(v) => Box::new(format!("{}", v)),
        AkitaValue::Json(ref v) => {
            // JSON is passed as a string
            Box::new(v.to_string())
        }
        AkitaValue::Uuid(ref v) => {
            // The UUID is passed as a string
            Box::new(v.to_string())
        }
        AkitaValue::Date(ref v) => {
            Box::new(v.clone())
        }
        AkitaValue::DateTime(ref v) => {
            Box::new(v.clone())
        }
        AkitaValue::Null => {
            // For NULL values, we need to create a special value
            // The Oracle driver usually handles NULL automatically
            Box::new(Option::<String>::None)
        }
        _ => {
            // For unsupported types, convert to text
            tracing::warn!("Unsupported value type: {:?}, converting to text", val);
            Box::new(val.to_string())
        }
    }
}

/// Bind Oracle values by location
fn bind_oracle_value(stmt: &mut oracle::Statement, index: usize, value: &AkitaValue) -> Result<(), AkitaError> {
    let pos = index + 1; // Oracle parameters start at 1
    match value {
        AkitaValue::Text(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind text parameter: {}", e))
        }),
        AkitaValue::Bool(v) => {
            let int_val = if *v { 1 } else { 0 };
            stmt.bind(pos, &int_val).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind bool parameter: {}", e))
            })
        }
        AkitaValue::Int(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind int parameter: {}", e))
        }),
        AkitaValue::Bigint(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind bigint parameter: {}", e))
        }),
        AkitaValue::Float(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind float parameter: {}", e))
        }),
        AkitaValue::Double(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind double parameter: {}", e))
        }),
        AkitaValue::Blob(v) => stmt.bind(pos, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind blob parameter: {}", e))
        }),
        AkitaValue::Date(v) => {
            // Convert the date string to oracle::Timestamp
            stmt.bind(pos, v).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind date parameter: {}", e))
            })
        }
        AkitaValue::DateTime(v) => {
            // Convert the datetime string to oracle::Timestamp
            stmt.bind(pos, v).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind datetime parameter: {}", e))
            })
        }
        AkitaValue::Null => stmt.bind(pos, &"null").map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind null parameter: {}", e))
        }),
        _ => {
            // For unsupported types, convert to text
            stmt.bind(pos, &value.to_string()).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind parameter as text: {}", e))
            })
        }
    }
}


/// Get the value from the Oracle row
fn get_value_from_oracle_row(row: &oracle::Row, index: usize) -> Result<AkitaValue, AkitaError> {
    // Checks for NULL
    if row.sql_values().len() == 0 {
        return Ok(AkitaValue::Null);
    }
    // Get the value based on the column type
    let col_type = row.column_info()[index].oracle_type();

    match col_type {
        OracleType::Number(_, _) => {
            if let Ok(val) = row.get::<usize, i64>(index) {
                return Ok(AkitaValue::Bigint(val));
            }
            if let Ok(val) = row.get::<usize, f64>(index) {
                return Ok(AkitaValue::Double(val));
            }
            let val: String = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get number value: {}", e))
            })?;
            Ok(AkitaValue::Text(val))
        }
        OracleType::Varchar2(_) | OracleType::Char(_) | OracleType::NChar(_) | OracleType::NVarchar2(_) => {
            let val: String = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get string value: {}", e))
            })?;
            Ok(AkitaValue::Text(val))
        }
        OracleType::Date => {
            let val: NaiveDateTime = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get Date value: {}", e))
            })?;
            Ok(AkitaValue::DateTime(val))
        }
        OracleType::Timestamp(_v) => {
            let val = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get Date value: {}", e))
            })?;
            Ok(AkitaValue::Timestamp(val))
        }
        OracleType::TimestampTZ(_) => {
            // Try as Timestamp (for TIMESTAMP & TIMESTAMP WITH TIME ZONE)
            if let Ok(ts) = row.get::<usize, Timestamp>(index) {
                return Ok(AkitaValue::Timestamp(timestamp_to_utc(&ts)));
            }

            // Backend: Returns a string in some cases (TIMESTAMPTZ is common)
            let val: String = row.get(index)?;
            let dt = parse_timestamptz_str(&val);
            Ok(AkitaValue::Timestamp(dt))
        }
        OracleType::BLOB | OracleType::Raw(_) => {
            let val: Vec<u8> = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get blob value: {}", e))
            })?;
            Ok(AkitaValue::Blob(val))
        }
        OracleType::NCLOB | OracleType::CLOB => {
            let val: String = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get clob value: {}", e))
            })?;
            Ok(AkitaValue::Text(val))
        }
        _ => {
            // For an unknown type, try to get a string
            let val: String = row.get(index).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to get value: {}", e))
            })?;
            Ok(AkitaValue::Text(val))
        }
    }
}

fn timestamp_to_utc(ts: &Timestamp) -> DateTime<Utc> {
    let year = ts.year();
    let month = ts.month();
    let day = ts.day();
    let hour = ts.hour();
    let min = ts.minute();
    let sec = ts.second();
    let fsec = ts.nanosecond();
    let ndt = NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_nano_opt(hour, min, sec, fsec)
        .unwrap();

    DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc)
}

fn parse_timestamptz_str(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %:z"))
        .unwrap()
        .with_timezone(&Utc)
}

/// Bind Oracle values by name
fn bind_oracle_value_by_name(stmt: &mut oracle::Statement, name: &str, value: &AkitaValue) -> Result<(), AkitaError> {
    match value {
        AkitaValue::Text(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind text parameter: {}", e))
        }),
        AkitaValue::Bool(v) => {
            let int_val = if *v { 1 } else { 0 };
            stmt.bind(name, &int_val).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind bool parameter: {}", e))
            })
        }
        AkitaValue::Int(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind int parameter: {}", e))
        }),
        AkitaValue::Bigint(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind bigint parameter: {}", e))
        }),
        AkitaValue::Float(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind float parameter: {}", e))
        }),
        AkitaValue::Double(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind double parameter: {}", e))
        }),
        AkitaValue::Blob(v) => stmt.bind(name, v).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind blob parameter: {}", e))
        }),
        AkitaValue::Null => stmt.bind(name, &value.to_string()).map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to bind null parameter: {}", e))
        }),
        _ => {
            // For unsupported types, convert to text
            stmt.bind(name, &value.to_string()).map_err(|e| {
                AkitaError::DatabaseError(format!("Failed to bind parameter as text: {}", e))
            })
        }
    }
}