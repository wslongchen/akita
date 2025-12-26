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
use std::fmt::format;
use std::io::Write;
use std::str::FromStr;
use std::sync::{RwLock};
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use indexmap::IndexMap;
use postgres::types::{ToSql, Type};
use uuid::Uuid;
use crate::errors::AkitaError;
use akita_core::{AkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector};
use crate::comm::ExecuteResult;
use crate::driver::blocking::postgres::PostgresConnection;

pub struct PostgresAdapter {
    conn: RwLock<PostgresConnection>,
}

impl PostgresAdapter {
    pub fn new(conn: PostgresConnection) -> Self {
        Self {
            conn: RwLock::new(conn),
        }
    }

    /// Start the transaction
    pub fn start_transaction(&self) -> crate::prelude::Result<()> {
        self.execute("START TRANSACTION", Params::None)?;
        Ok(())
    }

    /// Submit transactions
    pub fn commit_transaction(&self) -> crate::prelude::Result<()> {
        self.execute("COMMIT", Params::None)?;
        Ok(())
    }

    /// Roll back transactions
    pub fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        self.execute("ROLLBACK", Params::None)?;
        Ok(())
    }

    pub fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        match self.conn.write() {
            Ok(mut conn) => {

                // Get the statement type (query, update, etc.)
                let stmt_type = OperationType::detect_operation_type(sql);

                // Prepare the statement
                let statement = conn.prepare(sql).map_err(|e| {
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
                let pg_params_ref = &pg_params
                    .iter()
                    .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                    .collect::<Vec<_>>()[..];
                match stmt_type {
                    OperationType::Select => {
                        let mut records = Rows::new();
                        let rows = conn.query(&statement ,&pg_params_ref).map_err(|e| {
                            AkitaError::DatabaseError(format!("Failed to execute query: {}", e))
                        })?;
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
                        // Perform updates
                        let rows_affected = conn.execute(&statement, &pg_params_ref).map_err(|e| {
                            AkitaError::DatabaseError(format!("Failed to execute update: {}", e))
                        })?;
                        Ok(ExecuteResult::AffectedRows(rows_affected))
                    }
                }
            }
            Err(_) => {
                Err(AkitaError::DatabaseError("error to get connection.".to_string()))
            }
        }
    }

    
    pub fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        match self.conn.write() {
            Ok(mut conn) => {
                // Prepare the statement
                let statement = conn.prepare(sql).map_err(|e| {
                    AkitaError::DatabaseError(format!("Failed to prepare statement: {}", e))
                })?;
                // Getting column names
                let column_names: Vec<String> = statement
                    .columns()
                    .iter()
                    .map(|col| col.name().to_string())
                    .collect();
                let param_types = statement.params();
                // Conversion parameters
                let pg_params = convert_to_pg_params(param_types, params);
                let pg_params_ref = &pg_params
                    .iter()
                    .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                    .collect::<Vec<_>>()[..];
                let mut records = Rows::new();
                // Executing queries
                let rows = conn.
                    query(&statement ,&pg_params_ref)
                    .map_err(|e| {
                    AkitaError::DatabaseError(format!("Failed to execute query: {}", e))
                })?;
                // Conversion result
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
            Err(_) => {
                Err(AkitaError::DatabaseError("error to get connection.".to_string()))
            }
        }
        
    }

    /// Specific to PostgreSQL: Perform bulk inserts
    pub fn execute_batch(&self, sql: &str, params_list: Vec<Params>) -> Result<Vec<ExecuteResult>, AkitaError> {
        if params_list.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::with_capacity(params_list.len());
        for params in params_list {
            let rows_affected = self.execute(sql, params)?;
            results.push(rows_affected);
        }

        Ok(results)
    }
    
    pub fn batch_execute(&mut self, sql: &str) -> Result<(), AkitaError> {
        match self.conn.write() {
            Ok(mut conn) => {
                conn.batch_execute(sql).map_err(AkitaError::from)
            }
            Err(_) => {
                Err(AkitaError::DatabaseError("error to get connection.".to_string()))
            }
        }
        
    }

    /// Postgresql-specific: COPY (high-performance bulk import)
    pub fn copy_in(&mut self, table: &str, columns: &[&str], data: &[Vec<AkitaValue>]) -> Result<u64, AkitaError> {
        match self.conn.write() {
            Ok(mut conn) => {
                let columns_str = columns.join(", ");
                let sql = format!("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)", table, columns_str);

                // Start COPY
                let sink = conn.copy_in(&sql).map_err(|e| {
                    AkitaError::DatabaseError(format!("Failed to start COPY: {}", e))
                })?;

                // Writing data
                let mut writer = postgres::CopyInWriter::from(sink);
                for row in data {
                    let line = row
                        .iter()
                        .map(|v| Self::convert_csv_value(v))
                        .collect::<Vec<_>>()
                        .join(",");

                    writer.write_all(line.as_bytes()).map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to write COPY data: {}", e))
                    })?;
                    writer.write_all(b"\n").map_err(|e| {
                        AkitaError::DatabaseError(format!("Failed to write newline: {}", e))
                    })?;
                }

                // Complete COPY
                writer.finish().map_err(|e| {
                    AkitaError::DatabaseError(format!("Failed to finish COPY: {}", e))
                })?;

                // Get the number of affected rows (PostgreSQL COPY does not return the number of rows directly)
                let count_result = self.execute(
                    &format!("SELECT COUNT(*) FROM {}", table),
                    Params::None,
                )?.rows();

                if let Some(row) = count_result.get(0) {
                    if let Some(AkitaValue::Bigint(count)) = row.get(0) {
                        return Ok(count as u64);
                    }
                }

                Ok(0)
            }
            Err(_) => {
                Err(AkitaError::DatabaseError("error to get connection.".to_string()))
            }
        }
        
    }

    /// Get the number of affected rows
    pub fn affected_rows(&self) -> u64 {
        0
    }
    
    pub fn connection_id(&self) -> u32 {
        0
    }

    /// Get the last inserted ID
    pub fn last_insert_id(&self) -> u64 {
        0
    }

    fn convert_csv_value(val: &AkitaValue) -> String {
        match val {
            AkitaValue::Text(v) => Self::escape_csv(v),
            AkitaValue::Bool(v) => (if *v { "true" } else { "false" }).to_string(),
            AkitaValue::Tinyint(v) => v.to_string(),
            AkitaValue::Smallint(v) => v.to_string(),
            AkitaValue::Int(v) => v.to_string(),
            AkitaValue::Bigint(v) => v.to_string(),
            AkitaValue::Float(v) => v.to_string(),
            AkitaValue::Double(v) => v.to_string(),
            AkitaValue::BigDecimal(v) => v.to_string(),
            AkitaValue::Blob(_) => "".to_string(), // BLOB 不适合 CSV
            AkitaValue::Char(v) => format!("{}", v),
            AkitaValue::Json(v) => Self::escape_csv(&serde_json::to_string(v).unwrap_or_default()),
            AkitaValue::Uuid(v) => Self::escape_csv(&v.to_string()),
            AkitaValue::Date(v) => Self::escape_csv(&v.format("%Y-%m-%d").to_string()),
            AkitaValue::DateTime(v) => Self::escape_csv(&v.format("%Y-%m-%d %H:%M:%S").to_string()),
            AkitaValue::Null => "".to_string(),
            _ => Self::escape_csv(&val.to_string()),
        }
    }

    /// Escaping CSV fields
    fn escape_csv(value: &str) -> String {
        if value.contains('"') || value.contains(',') || value.contains('\n') || value.contains('\r') {
            format!("\"{}\"", value.replace("\"", "\"\""))
        } else {
            value.to_string()
        }
    }
}

// pub fn convert_to_postgres_format(sql: &str, params: &Params) -> crate::prelude::Result<(String, Vec<AkitaValue>)> {
//     match params {
//         Params::None => Ok((sql.to_string(), vec![])),
//         Params::Positional(param_values) => {
//             convert_positional_params(sql, param_values)
//         },
//         Params::Named(named_params) => {
//             convert_named_params(sql, named_params)
//         },
//     }
// }

fn convert_named_params(sql: &str, named_params: &IndexMap<String, AkitaValue>)
                        -> crate::prelude::Result<(String, Vec<AkitaValue>)>
{
    let mut converted_sql = String::with_capacity(sql.len() + named_params.len() * 3);
    let mut postgres_params = Vec::with_capacity(named_params.len());
    let mut param_counter = 1;
    let mut param_indices = IndexMap::new();

    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == ':' {
            // Parsing parameter names
            let mut param_name = String::new();
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_alphanumeric() || next_ch == '_' {
                    param_name.push(chars.next().unwrap());
                } else {
                    break;
                }
            }

            if !param_name.is_empty() {
                if let Some(value) = named_params.get(&param_name) {
                    let index = *param_indices.entry(param_name.clone())
                        .or_insert_with(|| {
                            let idx = param_counter;
                            param_counter += 1;
                            idx
                        });

                    converted_sql.push_str(&format!("${}", index));
                    // Add the parameter name to the parameter list only the first time it is encountered
                    if index == param_counter - 1 {
                        postgres_params.push(value.clone());
                    }
                } else {
                    return Err(AkitaError::DatabaseError(format!(
                        "Undefined named parameters: :{}", param_name
                    )));
                }
            } else {
                converted_sql.push(':');
            }
        } else {
            converted_sql.push(ch);
        }
    }

    // Verify that all named arguments are used (optional)
    for param_name in named_params.keys() {
        if !param_indices.contains_key(param_name) {
            tracing::warn!("The named parameter '{}' is not used in SQL", param_name);
        }
    }
    Ok((converted_sql, postgres_params))
}

fn convert_positional_params(sql: &str, param_values: &[AkitaValue])
                             -> crate::prelude::Result<(String, Vec<AkitaValue>)>
{
    let mut converted_sql = String::with_capacity(sql.len() + 20);
    let mut param_counter = 1;

    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '?' {
            if param_counter - 1 >= param_values.len() {
                return Err(AkitaError::DatabaseError(format!(
                    "Insufficient number of parameters: SQL requires at least {} parameters, but provides only {}",
                    param_counter, param_values.len()
                )));
            }
            converted_sql.push_str(&format!("${}", param_counter));
            param_counter += 1;
        } else {
            converted_sql.push(ch);
        }
    }

    // Check that the number of parameters matches
    let expected_params = param_counter - 1;
    if expected_params != param_values.len() {
        return Err(AkitaError::DatabaseError(format!(
            "Mismatched number of arguments: SQL requires {} arguments, but provides {}",
            expected_params, param_values.len()
        )));
    }

    Ok((converted_sql, param_values.to_vec()))
}

// pub fn convert_batch_insert_for_postgres(sql: &str, params: &Params)
//                                          -> crate::prelude::Result<(String, Vec<AkitaValue>)>
// {
//     match params {
//         Params::Positional(param_values) => {
//             // Multiline insertions usually use positional arguments
//             convert_batch_positional_params(sql, param_values)
//         },
//         // Other cases use generic transformations
//         _ => convert_to_postgres_format(sql, params),
//     }
// }

/// Optimize positional parameter conversions for multiline insertions
fn convert_batch_positional_params(sql: &str, param_values: &[AkitaValue])
                                   -> crate::prelude::Result<(String, Vec<AkitaValue>)>
{
    // Pre-allocate sufficient capacity
    let placeholder_count = sql.chars().filter(|&c| c == '?').count();

    if placeholder_count != param_values.len() {
        return Err(AkitaError::DatabaseError(format!(
            "Bulk operate parameter mismatch: SQL has {} placeholders, providing {} parameters",
            placeholder_count, param_values.len()
        )));
    }

    let mut converted_sql = String::with_capacity(sql.len() + placeholder_count * 3);
    let mut param_index = 1;

    let mut chars = sql.chars();

    while let Some(ch) = chars.next() {
        if ch == '?' {
            converted_sql.push_str(&format!("${}", param_index));
            param_index += 1;
        } else {
            converted_sql.push(ch);
        }
    }

    Ok((converted_sql, param_values.to_vec()))
}



fn get_value_from_row(row: &postgres::Row, index: usize, pg_type: &Type) -> AkitaValue {
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

/// Convert AkitaValue to PostgreSQL parameters
fn convert_to_pg_params(param_types: &[Type],params: Params) -> Vec<Box<dyn postgres::types::ToSql + Sync + Send>> {
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
