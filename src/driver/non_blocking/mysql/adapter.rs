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
use std::collections::HashMap;
use crate::comm::ExecuteResult;
use crate::errors::AkitaError;
use akita_core::{AkitaValue, FromAkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector};
use mysql_async::prelude::Queryable;
use mysql_async::{ Value as MysqlValue, Params as MysqlParams, Row as MysqlRow };
use std::sync::Arc;
use deadpool::managed::Manager;
use serde_json::Map;
use tokio::sync::RwLock;
use crate::driver::non_blocking::mysql::MysqlAsyncConnection;

/// MySQL Asynchronous adapter
pub struct MysqlAsyncAdapter {
    conn: Arc<RwLock<MysqlAsyncConnection>>,
}

impl MysqlAsyncAdapter {
    pub fn new(conn: MysqlAsyncConnection) -> Self {
        Self {
            conn: Arc::new(RwLock::new(conn)),
        }
    }
    
    pub async fn start_transaction(&self) -> crate::prelude::Result<()> {
        let mut conn = self.conn.write().await;
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(|e| AkitaError::MySQLAsyncError(e))
    }
    
    pub async fn commit_transaction(&self) -> crate::prelude::Result<()> {
        let mut conn = self.conn.write().await;
        conn.query_drop("COMMIT")
            .await
            .map_err(|e| AkitaError::MySQLAsyncError(e))
    }
    
    pub async fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        let mut conn = self.conn.write().await;
        conn.query_drop("ROLLBACK")
            .await
            .map_err(|e| AkitaError::MySQLAsyncError(e))
    }

    pub async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        // Conversion parameters
        let mysql_params = convert_to_mysql_params(params)?;
        self.inner_query(sql, mysql_params).await
    }
    
    async fn inner_query(&self, sql:  &str, params: mysql_async::Params) -> crate::prelude::Result<Rows> {
        let mut conn = self.conn.write().await;
        // Executing queries
        let result = conn
            .exec_iter(sql, params)
            .await
            .map_err(|e| AkitaError::MySQLAsyncError(e))?;

        let rows_fut = result.map_and_drop(|mysql_row| convert_mysql_row(mysql_row));
        let rows: Vec<Row> = rows_fut
            .await
            .map_err(|e| AkitaError::MySQLAsyncError(e))?
            .into_iter()
            .collect::<crate::prelude::Result<Vec<Row>>>()?;

        Ok(Rows {
            data: rows,
            count: None,
        })
    }
    
    pub async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        let mut conn = self.conn.write().await;
        // Conversion parameters
        let mysql_params = convert_to_mysql_params(params)?;

        let stmt_type = OperationType::detect_operation_type(sql);

        match stmt_type {
            OperationType::Select => {
                let rows = self.inner_query(sql, mysql_params).await?;
                Ok(ExecuteResult::Rows(rows))
            }
            _ => {
                conn.exec_drop(sql, mysql_params)
                    .await
                    .map_err(|e| AkitaError::MySQLAsyncError(e))?;

                Ok(ExecuteResult::AffectedRows(conn.affected_rows() as u64))
            }
        }
    }
    
    pub async fn affected_rows(&self) -> u64 {
        let conn = self.conn.read().await;
        conn.affected_rows()
    }
    
    pub async fn connection_id(&self) -> u32 {
        let mut conn = self.conn.write().await;
        match conn.query_first::<u32, _>("SELECT CONNECTION_ID()").await {
            Ok(Some(id)) => id,
            _ => 0,
        }
    }

    /// Get the last inserted ID
    pub async fn last_insert_id(&self) -> u64 {
        let conn = self.conn.read().await;
        conn.last_insert_id().map(|id| id as u64).unwrap_or(0)
    }

    /// Ping Checking connections
    pub async fn ping(&self) -> crate::prelude::Result<()> {
        let mut conn = self.conn.write().await;
        conn.ping().await.map_err(|e| AkitaError::MySQLAsyncError(e))
    }

    /// Check that the connection is valid
    pub async fn is_valid(&self) -> crate::prelude::Result<bool> {
        match self.ping().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}



/// Converted Value To MySQL Value
pub(crate) fn convert_value_to_mysql(value: AkitaValue) -> MysqlValue {
    match value {
        AkitaValue::Null => MysqlValue::NULL,
        AkitaValue::Bool(b) => MysqlValue::from(b),
        AkitaValue::Tinyint(i) => MysqlValue::from(i),
        AkitaValue::Smallint(i) => MysqlValue::from(i),
        AkitaValue::Int(i) => MysqlValue::from(i),
        AkitaValue::Bigint(i) => MysqlValue::from(i),
        AkitaValue::Float(f) => MysqlValue::from(f),
        AkitaValue::Double(d) => MysqlValue::from(d),
        AkitaValue::BigDecimal(bd) => MysqlValue::from(bd),
        AkitaValue::Blob(vec) => MysqlValue::from(vec),
        AkitaValue::Char(c) => MysqlValue::from(c.to_string()),
        AkitaValue::Text(s) => MysqlValue::from(s),
        AkitaValue::Json(j) => MysqlValue::from(j.to_string()),
        AkitaValue::Uuid(uuid) => MysqlValue::from(uuid.to_string()),
        AkitaValue::Date(date) => MysqlValue::from(date),
        AkitaValue::Time(time) => MysqlValue::from(time),
        AkitaValue::DateTime(dt) => MysqlValue::from(dt),
        AkitaValue::Timestamp(ts) => MysqlValue::from(ts.naive_utc()),
        AkitaValue::Interval(interval) => MysqlValue::from(interval.to_string()),
        AkitaValue::Array(arr) => MysqlValue::from(serde_json::to_string(&arr).unwrap_or_default()),
        AkitaValue::Object(obj) => {
            let mut data = Map::new();
            for (k, v) in obj.into_iter() {
                if v.is_null() {
                    continue;
                }
                data.insert(k.to_string(), serde_json::Value::from_value(&v));
            }
            let value = serde_json::to_string(&data).unwrap_or_default();
            value.into()
        },
        AkitaValue::Column(v) => MysqlValue::Bytes(v.into_bytes()),
        AkitaValue::RawSql(v) => MysqlValue::Bytes(v.into_bytes()),
        AkitaValue::List(v) =>  {
            let value = serde_json::to_string(&v).unwrap_or_default();
            value.into()
        }
        _ => MysqlValue::NULL,
    }
}


/// Converted MySQL Value To Value
fn convert_mysql_value(mysql_value: MysqlValue, column_type: mysql_async::consts::ColumnType) -> crate::prelude::Result<AkitaValue> {
    use mysql_async::consts::ColumnType;

    if mysql_value == MysqlValue::NULL {
        return Ok(AkitaValue::Null);
    }

    match column_type {
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            convert_decimal_value(mysql_value)
        }
        ColumnType::MYSQL_TYPE_TINY => {
            let val: i8 = try_convert(mysql_value)?;
            Ok(AkitaValue::Tinyint(val))
        }
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            let val: i16 = try_convert(mysql_value)?;
            Ok(AkitaValue::Smallint(val))
        }
        ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
            let val: i32 = try_convert(mysql_value)?;
            Ok(AkitaValue::Int(val))
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            let val: i64 = try_convert(mysql_value)?;
            Ok(AkitaValue::Bigint(val))
        }
        ColumnType::MYSQL_TYPE_FLOAT => {
            let val: f32 = try_convert(mysql_value)?;
            Ok(AkitaValue::Float(val))
        }
        ColumnType::MYSQL_TYPE_DOUBLE => {
            let val: f64 = try_convert(mysql_value)?;
            Ok(AkitaValue::Double(val))
        }
        ColumnType::MYSQL_TYPE_TIMESTAMP => {
            let val: chrono::NaiveDateTime = try_convert(mysql_value)?;
            Ok(AkitaValue::Timestamp(chrono::DateTime::from_naive_utc_and_offset(val, chrono::Utc)))
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            let val: chrono::NaiveDate = try_convert(mysql_value)?;
            Ok(AkitaValue::Date(val))
        }
        ColumnType::MYSQL_TYPE_TIME => {
            let val: chrono::NaiveTime = try_convert(mysql_value)?;
            Ok(AkitaValue::Time(val))
        }
        ColumnType::MYSQL_TYPE_DATETIME => {
            let val: chrono::NaiveDateTime = try_convert(mysql_value)?;
            Ok(AkitaValue::DateTime(val))
        }
        ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_VAR_STRING | ColumnType::MYSQL_TYPE_STRING => {
            let val: String = try_convert(mysql_value)?;
            Ok(AkitaValue::Text(val))
        }
        ColumnType::MYSQL_TYPE_JSON => {
            let val: String = try_convert(mysql_value)?;
            let json_val = serde_json::from_str(&val)
                .map_err(|e| AkitaError::DataError(e.to_string()))?;
            Ok(AkitaValue::Json(json_val))
        }
        ColumnType::MYSQL_TYPE_TINY_BLOB | ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
        ColumnType::MYSQL_TYPE_LONG_BLOB | ColumnType::MYSQL_TYPE_BLOB => {
            let val: Vec<u8> = try_convert(mysql_value)?;
            Ok(AkitaValue::Blob(val))
        }
        ColumnType::MYSQL_TYPE_BIT => {
            convert_bit_value(mysql_value)
        }
        ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIME2 => {
            let val: String = try_convert(mysql_value)?;
            Ok(AkitaValue::Text(val))
        }
        _ => {
            try_generic_conversion(mysql_value)
        }
    }
}

/// Converted MySQL Row To Row
fn convert_mysql_row(mysql_row: MysqlRow) -> crate::prelude::Result<Row> {
    let columns: Vec<String> = mysql_row
        .columns_ref()
        .iter()
        .map(|col| col.name_str().to_string())
        .collect();
    if mysql_row.is_empty() {
        return Ok(Row::new(columns, vec![]))
    }
    let column_types = mysql_row.columns();
    let rows = mysql_row.unwrap();
    let values = rows.into_iter().enumerate().map(|(i, mysql_value)| {
        let column_type = column_types.get(i)
            .map(|col| col.column_type())
            .unwrap_or(mysql_async::consts::ColumnType::MYSQL_TYPE_STRING);
        convert_mysql_value(mysql_value, column_type)
    })
        .collect::<crate::prelude::Result<Vec<AkitaValue>>>()?;
    Ok(Row {
        columns,
        data: values,
    })
}



/// Converted decimal
fn convert_decimal_value(mysql_value: MysqlValue) -> crate::prelude::Result<AkitaValue> {
    let bytes: Vec<u8> = mysql_value.try_into()
        .map_err(|_e| AkitaError::DataError("convert decimal error...".to_string()))?;

    let decimal_str = String::from_utf8(bytes)
        .map_err(|e| AkitaError::DataError(e.to_string()))?;

    let big_decimal = bigdecimal::BigDecimal::parse_bytes(decimal_str.as_bytes(), 10)
        .ok_or_else(|| AkitaError::DataError("Invalid decimal format".to_string()))?;

    Ok(AkitaValue::BigDecimal(big_decimal))
}

/// Converted bit
fn convert_bit_value(mysql_value: MysqlValue) -> crate::prelude::Result<AkitaValue> {
    let bytes: Vec<u8> = mysql_async::from_value_opt(mysql_value).map_err(|e| AkitaError::DataError(e.to_string()))?;
    if bytes.len() == 1 {
        Ok(AkitaValue::Bool(bytes[0] != 0))
    } else {
        Ok(AkitaValue::Blob(bytes))
    }
}

/// Generic type conversion
fn try_generic_conversion(mysql_value: MysqlValue) -> crate::prelude::Result<AkitaValue> {
    if let Ok(s) = String::try_from(mysql_value.clone()) {
        return Ok(AkitaValue::Text(s));
    }

    if let Ok(i) = mysql_async::from_value_opt::<i64>(mysql_value.clone()) {
        return Ok(AkitaValue::Bigint(i));
    }

    if let Ok(f) = mysql_async::from_value_opt::<f64>(mysql_value.clone()) {
        return Ok(AkitaValue::Double(f));
    }

    if let Ok(bytes) = mysql_async::from_value_opt::<Vec<u8>>(mysql_value) {
        return Ok(AkitaValue::Blob(bytes));
    }

    Err(AkitaError::DataError("Unsupported MySQL value type".to_string()))
}


/// Convert parameters to MySQL format
fn convert_to_mysql_params(params: Params) -> crate::prelude::Result<MysqlParams> {
    if params.is_empty() {
        return Ok(MysqlParams::Empty);
    }
    
    match params {
        Params::None => Ok(MysqlParams::Empty),
        Params::Positional(param) => { 
            let mysql_values = param.into_iter()
                .map(convert_value_to_mysql)
                .collect();
            Ok(MysqlParams::Positional(mysql_values))
        },
        Params::Named(named_map) => {
            let named = named_map.into_iter().map(|(name, v)| (name.into_bytes(), convert_value_to_mysql(v))).collect();
            Ok(MysqlParams::Named(named))
        }
    }
}


/// Type-safe conversion
fn try_convert<T>(value: MysqlValue) -> crate::prelude::Result<T>
where
    T: mysql_async::prelude::FromValue,
{

    mysql_async::from_value_opt::<T>(value).map_err(|e| AkitaError::DataError(e.to_string()))
}