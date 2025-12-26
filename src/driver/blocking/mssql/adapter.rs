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
use indexmap::IndexMap;
use akita_core::{AkitaValue, OperationType, Params, Row, Rows, SqlInjectionDetector, SqlSecurityConfig};
use crate::comm::ExecuteResult;
use crate::driver::blocking::mssql::MssqlConnection;
use crate::errors::AkitaError;

pub struct MssqlAdapter {
    conn: MssqlConnection,
}


impl MssqlAdapter {
    pub fn new(conn: MssqlConnection) -> Self {
        Self {
            conn,
        }
    }

    /// Start the transaction
    pub fn start_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("BEGIN TRANSACTION;")?;
        Ok(())
        
    }

    /// Submit transactions
    pub fn commit_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("COMMIT TRANSACTION;")?;
        Ok(())
    }

    /// Roll back transactions
    pub fn rollback_transaction(&self) -> crate::prelude::Result<()> {
        self.conn
            .simple_query("ROLLBACK TRANSACTION;")?;
        Ok(())
    }

    pub fn query(&self, sql: &str, params: Params) -> Result<Rows, AkitaError> {
        // Convert parameters
        let mssql_params = convert_to_mssql_params(params);
        let param_refs: Vec<&dyn tiberius::ToSql> = mssql_params
            .iter()
            .map(|p| &**p as &dyn tiberius::ToSql)
            .collect();
        self.inner_query(sql, &param_refs)
    }
    
    fn inner_query(&self, sql: &str, param_refs: &[&dyn tiberius::ToSql]) -> Result<Rows, AkitaError> {
        let rows = self.conn.query(sql, &param_refs)?;
        if rows.is_empty() {
            return Ok(Rows::new())
        }
        let first_row = &rows[0];
        let column_names: Vec<String> = (0..first_row.columns().len())
            .map(|i| first_row.columns()[i].name().to_string())
            .collect();

        let mut records = Rows::new();
        for row in rows {
            let mut record = Vec::new();

            for i in 0..row.columns().len() {
                let value = get_value_from_mssql_row(&row, i)?;
                record.push(value);
            }
            records.push(Row {
                columns: column_names.clone(),
                data: record,
            });
        }

        Ok(records)
    }

    pub fn execute(&self, sql: &str, params: Params) -> Result<ExecuteResult, AkitaError> {
        // Get statement types (query, update, etc.)
        let stmt_type = OperationType::detect_operation_type(sql);
        // Convert parameters
        let mssql_params = convert_to_mssql_params(params);
        let param_refs: Vec<&dyn tiberius::ToSql> = mssql_params
            .iter()
            .map(|p| &**p as &dyn tiberius::ToSql)
            .collect();
        match stmt_type {
            OperationType::Select => {
                let records = self.inner_query(sql, &param_refs)?;
                Ok(ExecuteResult::Rows(records))
            }
            _ => {
                let affected_rows = self.conn.execute(sql, &param_refs)?;
                Ok(ExecuteResult::AffectedRows(affected_rows))
            }
        }
    }
}

fn convert_to_mssql_params(params: Params) -> Vec<Box<dyn tiberius::ToSql>> {
    match params {
        Params::None => vec![],
        Params::Positional(param_values) => {
            param_values
                .into_iter()
                .map(|v| convert_akita_value_to_mssql(v))
                .collect::<Vec<_>>()
        },
        Params::Named(named_params) => {
            named_params.values().cloned().into_iter()
                .map(|v| convert_akita_value_to_mssql(v))
                .collect::<Vec<_>>()
        },
    }
    
}


fn convert_akita_value_to_mssql(val: AkitaValue) -> Box<dyn tiberius::ToSql> {
    use tiberius::numeric::BigDecimal;
    match val {
        AkitaValue::Text(v) => Box::new(v),
        AkitaValue::Bool(v) => Box::new(v),
        AkitaValue::Tinyint(v) => {
            let unsigned = if v >= 0 {
                v as u8
            } else {
                (v as i16 + 256) as u8
            };
            Box::new(unsigned)
        },
        AkitaValue::Smallint(v) => Box::new(v),
        AkitaValue::Int(v) => Box::new(v),
        AkitaValue::Bigint(v) => Box::new(v),
        AkitaValue::Float(v) => Box::new(v),
        AkitaValue::Double(v) => Box::new(v),
        AkitaValue::BigDecimal(v) => {
            let bd = BigDecimal::from_str(v.to_string().as_str()).unwrap_or(BigDecimal::from(0));
            Box::new(bd)
        }
        AkitaValue::Blob(v) => Box::new(v),
        AkitaValue::Char(v) => Box::new(format!("{}", v)),
        AkitaValue::Json(v) => Box::new(serde_json::to_string(&v).unwrap_or_default()),
        AkitaValue::Uuid(v) => Box::new(v),
        AkitaValue::Date(v) => Box::new(v),
        AkitaValue::DateTime(v) => Box::new(v),
        AkitaValue::Null => Box::new(Option::<String>::None),
        _ => Box::new(val.to_string()),
    }
}


fn get_value_from_mssql_row(row: &tiberius::Row, index: usize) -> Result<AkitaValue, AkitaError> {
    use tiberius::numeric::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime};

    let column = &row.columns()[index];

    match column.column_type() {
        // Bit / Boolean Type
        tiberius::ColumnType::Bit | tiberius::ColumnType::Bitn => {
            let val: Option<bool> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Bool(v),
                None => AkitaValue::Null,
            })
        }

        // Integer types
        tiberius::ColumnType::Int1 => {
            let val: Option<u8> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Tinyint(v as i8),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Int2 => {
            let val: Option<i16> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Smallint(v),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Int4 => {
            let val: Option<i32> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Int(v),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Int8 => {
            let val: Option<i64> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Bigint(v),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Intn => {
            // Intn may be an integer of 1, 2, 4, 8 bytes, try to get i64
            let val: Option<i64> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Bigint(v),
                None => AkitaValue::Null,
            })
        }

        // Floating-point types
        tiberius::ColumnType::Float4 => {
            let val: Option<f32> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Float(v),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Float8 => {
            let val: Option<f64> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Double(v),
                None => AkitaValue::Null,
            })
        }
        tiberius::ColumnType::Floatn => {
            //Floatn may be a 4 or 8 byte floating-point number; try to get it as f64
            let val: Option<f64> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Double(v),
                None => AkitaValue::Null,
            })
        }

        // Numeric types（Decimal/Numeric）
        tiberius::ColumnType::Decimaln | tiberius::ColumnType::Numericn => {
            let val: Option<BigDecimal> = row.get(index);
            Ok(match val {
                Some(v) => {
                    // Convert to a string, and then create BigDecimal
                    let decimal_str = v.to_string();
                    match decimal_str.parse() {
                        Ok(bd) => AkitaValue::BigDecimal(bd),
                        Err(_) => AkitaValue::Text(decimal_str),
                    }
                }
                None => AkitaValue::Null,
            })
        }

        // String Types
        tiberius::ColumnType::BigVarChar | tiberius::ColumnType::BigChar
        | tiberius::ColumnType::NVarchar | tiberius::ColumnType::NChar
        | tiberius::ColumnType::Text | tiberius::ColumnType::NText => {
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Text(v.to_string()),
                None => AkitaValue::Null,
            })
        }

        // XML type
        tiberius::ColumnType::Xml => {
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Text(v.to_string()), // 或者 AkitaValue::Xml 如果你有这种类型
                None => AkitaValue::Null,
            })
        }

        // Date-time types
        tiberius::ColumnType::Daten => {
            let val: Option<NaiveDate> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Date(v),
                None => AkitaValue::Null,
            })
        }

        tiberius::ColumnType::Datetime | tiberius::ColumnType::Datetime4
        | tiberius::ColumnType::Datetimen | tiberius::ColumnType::Datetime2 => {
            if let Ok(val) = row.try_get::<NaiveDateTime,_>(index) {
                return Ok(match val {
                    Some(v) => AkitaValue::DateTime(v),
                    None => AkitaValue::Null,
                });
            }
            // If you can't get NaiveDateTime directly, try getting it as a string
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::DateTime(
                    NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap_or_else(|_| {
                            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                .and_hms_opt(0, 0, 0).unwrap()
                        })
                ),
                None => AkitaValue::Null,
            })
        }

        tiberius::ColumnType::Timen => {
            //The Time type, possibly treated as a string
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Text(v.to_string()), // 或者创建专门的 Time 类型
                None => AkitaValue::Null,
            })
        }

        tiberius::ColumnType::DatetimeOffsetn => {
            // Date and time with a time zone, treated as a string
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Text(v.to_string()),
                None => AkitaValue::Null,
            })
        }

        // Binary type
        tiberius::ColumnType::BigVarBin | tiberius::ColumnType::BigBinary
        | tiberius::ColumnType::Image => {
            let val: Option<&[u8]> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Blob(v.to_vec()),
                None => AkitaValue::Null,
            })
        }

        // GUID/UUID type
        tiberius::ColumnType::Guid => {
            let val = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Uuid(v),
                None => AkitaValue::Null,
            })
        }

        // Type of currency
        tiberius::ColumnType::Money | tiberius::ColumnType::Money4 => {
            // Currency types can be treated as floating-point numbers or strings
            if let Ok(val) = row.try_get::<f64, _>(index) {
                return Ok(match val {
                    Some(v) => AkitaValue::Double(v),
                    None => AkitaValue::Null,
                });
            }
            let val: Option<&str> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Text(v.to_string()),
                None => AkitaValue::Null,
            })
        }

        // Variant type
        tiberius::ColumnType::SSVariant => {
            // SQL Variant type.Try every possible type
            if let Ok(val) = row.try_get::<&str,_>(index) {
                if let Some(v) = val {
                    return Ok(AkitaValue::Text(v.to_string()));
                }
            }
            if let Ok(val) = row.try_get::<i64, _>(index) {
                if let Some(v) = val {
                    return Ok(AkitaValue::Bigint(v));
                }
            }
            if let Ok(val) = row.try_get::<f64, _>(index) {
                if let Some(v) = val {
                    return Ok(AkitaValue::Double(v));
                }
            }
            Ok(AkitaValue::Null)
        }

        // Other types
        tiberius::ColumnType::Udt => {
            // User-defined types, treated as binary
            let val: Option<&[u8]> = row.get(index);
            Ok(match val {
                Some(v) => AkitaValue::Blob(v.to_vec()),
                None => AkitaValue::Null,
            })
        }

        // Null Type or unknown type
        tiberius::ColumnType::Null => {
            Ok(AkitaValue::Null)
        }
    }
}
