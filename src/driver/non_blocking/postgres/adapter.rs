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
use std::str::FromStr;
use crate::comm::ExecuteResult;
use crate::driver::non_blocking::postgres::connection::PostgresAsyncConnection;
use crate::errors::AkitaError;
use akita_core::{AkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector};
use std::sync::Arc;
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use tokio::sync::Mutex;
use tokio_postgres::types::{ToSql, Type};
use uuid::Uuid;

/// PostgreSQL Asynchronous adapter
pub struct PostgresAsyncAdapter {
    conn: Arc<PostgresAsyncConnection>,
}

impl PostgresAsyncAdapter {
    pub fn new(conn: PostgresAsyncConnection) -> Self {
        Self {
            conn: Arc::new(conn),
        }
    }

    pub async fn start_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("START TRANSACTION")
            .await
            .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;
        Ok(())
    }

    pub async fn commit_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("COMMIT")
            .await
            .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;
        Ok(())
    }

    pub async fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("ROLLBACK")
            .await
            .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;
        Ok(())
    }

    pub async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        // Prepare the statement
        let statement = self.conn.prepare(sql).await.map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
        })?;
        let param_types = statement.params();
        // Getting column names
        let column_names: Vec<String> = statement
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let pg_params = convert_to_pg_params(param_types, params);
        let pg_params_ref: Vec<&(dyn ToSql + Sync)> = pg_params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        
        let rows = self.conn
            .query(&statement, &pg_params_ref)
            .await
            .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;

        let mut records = Rows::new();
        for row in rows {
            let mut record = Vec::new();
            for (i, column) in statement.columns().iter().enumerate() {
                let pg_type = column.type_();
                let value = get_value_from_row(&row, i, pg_type);
                record.push(value);
            }
            records.push(Row {
                columns: column_names.clone(),
                data: record,
            });
        }

        Ok(records)
    }

    pub async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        // Prepare the statement
        let statement = self.conn.prepare(sql).await.map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
        })?;
        
        // Get the statement type (query, update, etc.)
        let stmt_type = OperationType::detect_operation_type(sql);

        // Getting column names
        let column_names: Vec<String> = statement
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();
        let param_types = statement.params();
        // Conversion parameters
        let pg_params = convert_to_pg_params(param_types, params);
        let pg_params_ref: Vec<&(dyn ToSql + Sync)> = pg_params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        
        match stmt_type {
            OperationType::Select => {
                let rows = self.conn
                    .query(&statement, &pg_params_ref)
                    .await
                    .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;

                let mut records = Rows::new();
                for row in rows {
                    let mut record = Vec::new();
                    for (i, column) in statement.columns().iter().enumerate() {
                        let pg_type = column.type_();
                        let value = get_value_from_row(&row, i, pg_type);
                        record.push(value);
                    }
                    records.push(Row {
                        columns: column_names.clone(),
                        data: record,
                    });
                }

                Ok(ExecuteResult::Rows(records))
            }
            _ => {
                let result = self.conn
                    .execute(&statement, &pg_params_ref)
                    .await
                    .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;

                Ok(ExecuteResult::AffectedRows(result as u64))
            }
        }
    }

    pub async fn affected_rows(&self) -> u64 {
        // PostgreSQL's affected_rows needs to be retrieved immediately after execution
        0
    }

    pub async fn connection_id(&self) -> u32 {
        match self.conn.query_one("SELECT pg_backend_pid()", &[]).await {
            Ok(row) => row.get::<_, i32>(0) as u32,
            _ => 0,
        }
    }

    pub async fn last_insert_id(&self) -> u64 {
        // PostgreSQL uses the RETURNING clause to get the insertion ID
        match self.conn.query_one("SELECT lastval()", &[]).await {
            Ok(row) => row.get::<_, i64>(0) as u64,
            _ => 0,
        }
    }

    pub async fn ping(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("SELECT 1")
            .await
            .map_err(|e| AkitaError::InvalidSQL(e.to_string()))?;
        Ok(())
    }

    pub async fn is_valid(&self) -> crate::prelude::Result<bool> {
        match self.ping().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}


/// Convert AkitaValue to PostgreSQL parameters
fn convert_to_pg_params(param_types: &[Type], params: Params) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
    match params {
        Params::None => vec![],
        Params::Positional(param) => {
            param.into_iter()
                .zip(param_types.iter())
                .map(|(val, pg_type)| {
                    convert_pg_value_with_type(val, pg_type)
                })
                .collect()
        }
        Params::Named(named_params) => {
            named_params.values().cloned().into_iter()
                .zip(param_types.iter())
                .map(|(val, pg_type)| convert_pg_value_with_type(val, pg_type))
                .collect::<Vec<_>>()
        }
    }
}

fn convert_pg_value_with_type(val: AkitaValue, pg_type: &Type) -> Box<dyn ToSql + Sync + Send> {
    match val {
        AkitaValue::Text(v) => Box::new(v.clone()),
        AkitaValue::Bool(v) => Box::new(v),
        AkitaValue::Tinyint(i) => match pg_type {
            &Type::INT4 => Box::new(i as i32),
            &Type::INT8 => Box::new(i as i64),
            _ => Box::new(i as i16),
        },
        // Special note: Smallint may have to be converted depending on the target type
        AkitaValue::Smallint(i) => match pg_type {
            &Type::INT4 => Box::new(i as i32),
            &Type::INT8 => Box::new(i as i64),
            _ => Box::new(i),
        },

        AkitaValue::Int(i) => match pg_type {
            &Type::INT2 => Box::new(i as i16),
            &Type::INT8 => Box::new(i as i64),
            _ => Box::new(i),
        },

        AkitaValue::Bigint(i) => convert_integer_to_pg(i, pg_type),
        AkitaValue::Float(v) => Box::new(v as f32),
        AkitaValue::Double(v) => Box::new(v),
        AkitaValue::BigDecimal(v) => Box::new(v.to_string()),
        AkitaValue::Blob(v) => Box::new(v),
        AkitaValue::Char(v) => Box::new(format!("{}", v)),
        AkitaValue::Json(v) => Box::new(v.clone()),
        AkitaValue::Uuid(v) => Box::new(v.simple().to_string()),
        AkitaValue::Date(v) => Box::new(v.clone()),
        AkitaValue::DateTime(v) => Box::new(v.clone()),

        AkitaValue::Null => match pg_type {
            &Type::INT2 => Box::new(Option::<i16>::None),
            &Type::INT4 => Box::new(Option::<i32>::None),
            &Type::INT8 => Box::new(Option::<i64>::None),
            &Type::TEXT => Box::new(Option::<String>::None),
            _ => Box::new(Option::<i32>::None),
        },

        _ => Box::new(val.to_string()),
    }
}

fn convert_integer_to_pg(value: i64, target_type: &Type) -> Box<dyn ToSql + Sync + Send> {
    match target_type {
        // If there is explicit type information, match exactly
        &Type::INT2 => Box::new(value as i16),
        &Type::INT4 => Box::new(value as i32),  // This is required for the seventh parameter
        &Type::INT8 => Box::new(value),

        // If no type information is available, it is inferred from the value size
        _ => {
            if value >= i8::MIN as i64 && value <= i8::MAX as i64 {
                Box::new(value as i16)
            } else if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
                Box::new(value as i16)
            } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                Box::new(value as i32)
            } else {
                Box::new(value)
            }
        }
        _ => {
            Box::new(value)
        }
    }
}


fn get_value_from_row(row: &tokio_postgres::Row, index: usize, pg_type: &Type) -> AkitaValue {
    if row.is_empty() {
        return AkitaValue::Null;
    }

    match pg_type {
        // Integer types
        &Type::INT2 => {
            let val: Option<i16> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Smallint(val),
            }
        }
        &Type::INT4 => {
            let val: Option<i32> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Int(val),
            }
        }
        &Type::INT8 => {
            let val: Option<i64> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Bigint(val),
            }
        }

        // Floating-point types
        &Type::FLOAT4 => {
            let val: Option<f32> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Float(val),
            }
        }
        &Type::FLOAT8 => {
            let val: Option<f64> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Double(val),
            }
        }

        // Text type
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
            let val: Option<String> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Text(val),
            }
        }

        // Boolean types
        &Type::BOOL => {
            let val: Option<bool> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Bool(val),
            }
        }

        // Date-time types
        &Type::DATE => {
            let val: Option<chrono::NaiveDate> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Date(val),
            }
        }
        &Type::TIME => {
            let val: Option<chrono::NaiveTime> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Time(val),
            }
        }
        &Type::TIMESTAMP => {
            let val: Option<chrono::NaiveDateTime> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::DateTime(val),
            }
        }
        &Type::TIMESTAMPTZ => {
            let val: Option<chrono::DateTime<chrono::Utc>> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::DateTime(val.naive_utc()),
            }
        }

        // Binary type
        &Type::BYTEA => {
            let val: Option<Vec<u8>> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Blob(val),
            }
        }

        // UUID TYPES
        &Type::UUID => {
            let val: Option<String> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Uuid(Uuid::from_str(&val).unwrap_or_default()),
            }
        }

        // numeric type (numeric/decimal)
        &Type::NUMERIC => {
            // PostgreSQL numeric conversion to BigDecimal
            let val: Option<String> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::BigDecimal(BigDecimal::from_str(&val).unwrap_or_default()),
            }
        }

        // JSON type
        &Type::JSON | &Type::JSONB => {
            let val: Option<serde_json::Value> = row.get(index);
            match val {
                None => AkitaValue::Null,
                Some(val) => AkitaValue::Json(val),
            }
        }
        // Unknown type attempts to convert to text
        _ => {
            // Let's try to get the string
            let val: Option<String> = row.try_get(index).ok().flatten();
            match val {
                Some(val) => AkitaValue::Text(val),
                None => {
                    // Try other common types
                    if let Ok(val) = row.try_get::<_, Option<i64>>(index) {
                        match val {
                            Some(v) => AkitaValue::Bigint(v),
                            None => AkitaValue::Null,
                        }
                    } else if let Ok(val) = row.try_get::<_, Option<f64>>(index) {
                        match val {
                            Some(v) => AkitaValue::Double(v),
                            None => AkitaValue::Null,
                        }
                    } else if let Ok(val) = row.try_get::<_, Option<bool>>(index) {
                        match val {
                            Some(v) => AkitaValue::Bool(v),
                            None => AkitaValue::Null,
                        }
                    } else {
                        AkitaValue::Null
                    }
                }
            }
        }
    }
}
