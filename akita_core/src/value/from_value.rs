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
use crate::{AkitaDataError, AkitaValue, ConversionError};
use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde_json::Value as JsonValue;
use std::any::type_name;
use std::collections::HashMap;
use uuid::Uuid;

// 从 AkitaValue CONVERTED trait
pub trait FromAkitaValue: Sized {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError>;
    fn from_value(value: &AkitaValue) -> Self {
        match Self::from_value_opt(value) {
            Ok(x) => x,
            Err(_err) => panic!(
                "Couldn't from {:?} to type {}. (see FromAkitaValue documentation)",
                value,
                type_name::<Self>(),
            ),
        }
    }
}

impl FromAkitaValue for AkitaValue
{
    fn from_value_opt(v: &AkitaValue) -> Result<Self, AkitaDataError> {
        Ok(v.clone())
    }
}

// 为 Option TYPE IMPLEMENTATION
impl<T: FromAkitaValue> FromAkitaValue for Option<T> {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Null => Ok(None),
            other => T::from_value_opt(other).map(Some),
        }
    }
}

// Result Type implementation (easy error handling)
impl<T: FromAkitaValue, E: From<AkitaDataError>> FromAkitaValue for Result<T, E> {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        T::from_value_opt(value).map(Ok)
    }
}

macro_rules! impl_from_akita_value_numeric {
    ($ty: ty, $method:ident, $ty_name: tt, $($variant: ident),*) => {
        impl FromAkitaValue for $ty {
            fn from_value_opt(v: &AkitaValue) -> Result<Self, AkitaDataError> {
                match *v {
                    $(AkitaValue::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    AkitaValue::BigDecimal(ref v) => Ok(v.$method().unwrap_or_default()),
                    AkitaValue::Object(ref v) => {
                        let (_, v) = v.first().unwrap_or((&String::default(), &AkitaValue::Null));
                        Ok(<$ty>::from_value(v))
                    },
                    _ => Err(AkitaDataError::not_supported_error(format!("{:?}", v), $ty_name.to_string())),
                }
            }
        }
    }
}


impl_from_akita_value_numeric!(i8, to_i8, "i8", Tinyint);
impl_from_akita_value_numeric!(isize, to_isize, "isize", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(u8, to_u8, "u8", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(u16, to_u16, "u16", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(u32, to_u32, "u32", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(u64, to_u64, "u64", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(usize, to_usize, "usize", Tinyint, Bigint, Int);
impl_from_akita_value_numeric!(i16, to_i16, "i16", Tinyint, Smallint);
impl_from_akita_value_numeric!(i32, to_i32, "i32", Tinyint, Smallint, Int, Bigint);
impl_from_akita_value_numeric!(i64, to_i64, "i64", Tinyint, Smallint, Int, Bigint);
impl_from_akita_value_numeric!(f32, to_f32, "f32", Float);
impl_from_akita_value_numeric!(f64, to_f64, "f64", Float, Double);

// Implement for other types
impl FromAkitaValue for bool {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Bool(b) => Ok(*b),
            AkitaValue::Tinyint(v) => Ok(*v != 0),
            AkitaValue::Smallint(v) => Ok(*v != 0),
            AkitaValue::Int(v) => Ok(*v != 0),
            AkitaValue::Bigint(v) => Ok(*v != 0),
            AkitaValue::Text(ref s) => {
                let lower = s.to_lowercase();
                match lower.as_str() {
                    "true" | "1" | "yes" | "on" => Ok(true),
                    "false" | "0" | "no" | "off" => Ok(false),
                    _ => Err(AkitaDataError::ParseError(format!("Failed to parse '{}' as bool", s))),
                }
            }
            AkitaValue::Null => Ok(false),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "bool".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for String {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Text(s) => Ok(s.to_string()),
            AkitaValue::Char(c) => Ok(c.to_string()),
            AkitaValue::Tinyint(v) => Ok(v.to_string()),
            AkitaValue::Smallint(v) => Ok(v.to_string()),
            AkitaValue::Int(v) => Ok(v.to_string()),
            AkitaValue::Bigint(v) => Ok(v.to_string()),
            AkitaValue::Float(v) => Ok(v.to_string()),
            AkitaValue::Double(v) => Ok(v.to_string()),
            AkitaValue::Bool(v) => Ok(v.to_string()),
            AkitaValue::Date(v) => Ok(v.to_string()),
            AkitaValue::Time(v) => Ok(v.to_string()),
            AkitaValue::DateTime(v) => Ok(v.to_string()),
            AkitaValue::Timestamp(v) => Ok(v.to_rfc3339()),
            AkitaValue::Uuid(v) => Ok(v.to_string()),
            AkitaValue::BigDecimal(v) => Ok(v.to_string()),
            AkitaValue::Json(v) => serde_json::to_string(&v).map_err(|e| AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: e.to_string(),
            })),
            AkitaValue::Null => Ok("".to_string()),
            AkitaValue::Blob(v) => Ok(String::from_utf8(v.clone()).map_err(|e| AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: e.to_string(),
            }))?),
            AkitaValue::List(items) => {
                let strings: Result<Vec<String>, _> = items.into_iter()
                    .map(String::from_value_opt)
                    .collect();
                Ok(strings?.join(","))
            }
            AkitaValue::Object(map) => {
                let mut ham = HashMap::new();
                for (k, v) in map.iter() {
                    ham.insert(k, v);
                }
                serde_json::to_string(&ham).map_err(|e| AkitaDataError::ConversionError(ConversionError::ConversionError {
                    message: e.to_string(),
                }))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "String".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for char {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Char(c) => Ok(*c),
            AkitaValue::Text(s) => {
                if s.len() == 1 {
                    Ok(s.chars().next().unwrap())
                } else {
                    Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                        message: format!("String '{}' is not a single character", s),
                    }))
                }
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "char".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for Uuid {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Uuid(uuid) => Ok(*uuid),
            AkitaValue::Text(s) => Uuid::parse_str(&s).map_err(|e| AkitaDataError::ConversionError(ConversionError::ParseError {
                message: e.to_string(),
            })),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "Uuid".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for NaiveDate {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Date(date) => Ok(*date),
            AkitaValue::Text(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map_err(|e| AkitaDataError::ConversionError(ConversionError::ParseError {
                    message: e.to_string(),
                })),
            AkitaValue::DateTime(dt) => Ok(dt.date()),
            AkitaValue::Timestamp(ts) => Ok(ts.date_naive()),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "NaiveDate".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for NaiveTime {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Time(time) => Ok(*time),
            AkitaValue::Text(s) => NaiveTime::parse_from_str(&s, "%H:%M:%S")
                .map_err(|e| AkitaDataError::ConversionError(ConversionError::ParseError {
                    message: e.to_string(),
                })),
            AkitaValue::DateTime(dt) => Ok(dt.time()),
            AkitaValue::Timestamp(ts) => Ok(ts.time()),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "NaiveTime".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for NaiveDateTime {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::DateTime(dt) => Ok(*dt),
            AkitaValue::Text(s) => {
                // Experiment with multiple datetime formats
                if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                    return Ok(dt);
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(dt);
                }
                if let Ok(date) = NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                    return Ok(date.and_hms_opt(0, 0, 0).unwrap());
                }
                Err(AkitaDataError::ConversionError(ConversionError::ParseError {
                    message: format!("Failed to parse '{}' as NaiveDateTime", s),
                }))
            }
            AkitaValue::Date(date) => Ok(date.and_hms_opt(0, 0, 0).unwrap()),
            AkitaValue::Timestamp(ts) => Ok(ts.naive_utc()),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "NaiveDateTime".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for DateTime<Utc> {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Timestamp(ts) => Ok(*ts),
            AkitaValue::Text(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| AkitaDataError::ConversionError(ConversionError::ParseError {
                    message: e.to_string(),
                })),
            AkitaValue::DateTime(dt) => Ok(DateTime::from_naive_utc_and_offset(*dt, Utc)),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "DateTime<Utc>".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for BigDecimal {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::BigDecimal(bd) => Ok(bd.clone()),
            AkitaValue::Text(s) => s.parse().map_err(|_e| AkitaDataError::ConversionError(ConversionError::ParseError {
                message: "error to parse Text to BigDecimal".to_string(),
            })),
            AkitaValue::Tinyint(v) => Ok(BigDecimal::from(*v)),
            AkitaValue::Smallint(v) => Ok(BigDecimal::from(*v)),
            AkitaValue::Int(v) => Ok(BigDecimal::from(*v)),
            AkitaValue::Bigint(v) => Ok(BigDecimal::from(*v)),
            AkitaValue::Float(v) => Ok(BigDecimal::from_f32(*v).ok_or_else(|| AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: "Cannot convert f32 to BigDecimal".to_string(),
            }))?),
            AkitaValue::Double(v) => Ok(BigDecimal::from_f64(*v).ok_or_else(|| AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: "Cannot convert f64 to BigDecimal".to_string(),
            }))?),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "BigDecimal".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl FromAkitaValue for JsonValue {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Json(json) => Ok(json.clone()),
            AkitaValue::Text(s) => Ok(JsonValue::String(s.to_string())),
            AkitaValue::Object(map) => {
                let json_map: serde_json::Map<String, JsonValue> = map
                    .into_iter()
                    .map(|(k, v)| Ok((k.to_string(), JsonValue::from_value_opt(&v)?)))
                    .collect::<Result<_, AkitaDataError>>()?;
                Ok(JsonValue::Object(json_map))
            }
            AkitaValue::List(items) => {
                let json_array: Vec<JsonValue> = items
                    .iter()
                    .map(JsonValue::from_value_opt)
                    .collect::<Result<_, _>>()?;
                Ok(JsonValue::Array(json_array))
            }
            AkitaValue::Array(items) => {
                let json_array: Vec<JsonValue> = items.to_list().unwrap_or_default()
                    .iter()
                    .map(JsonValue::from_value_opt)
                    .collect::<Result<_, _>>()?;
                Ok(JsonValue::Array(json_array))
            }
            AkitaValue::Bool(b) => Ok(JsonValue::Bool(*b)),
            AkitaValue::Int(i) => Ok(JsonValue::Number(i.to_owned().into())),
            AkitaValue::Bigint(i) => Ok(JsonValue::Number(i.to_owned().into())),
            AkitaValue::Double(f) => {
                if let Some(num) = serde_json::Number::from_f64(*f) {
                    Ok(JsonValue::Number(num))
                } else {
                    Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                        message: "Cannot convert f64 to JSON number".to_string(),
                    }))
                }
            }
            AkitaValue::Null => Ok(JsonValue::Null),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "JsonValue".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl<T> FromAkitaValue for &T
where
    T: FromAkitaValue,
{
    fn from_value_opt(v: &AkitaValue) -> Result<Self, AkitaDataError> {
        match v {
            AkitaValue::Null => Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", v))),
            _ => FromAkitaValue::from_value_opt(v),
        }

    }
}

impl FromAkitaValue for () {
    fn from_value_opt(v: &AkitaValue) -> Result<Self, AkitaDataError> {
        match v {
            AkitaValue::Null => Ok(()),
            _ => Err(AkitaDataError::not_supported_error(
                format!("{:?}", v),
                "Vec<String>".to_string(),
            )),
        }
    }
}


// For HashMap Realize
impl<V: FromAkitaValue> FromAkitaValue for HashMap<String, V> {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(map) => {
                map.into_iter()
                    .map(|(k, v)| Ok((k.to_string(), V::from_value_opt(v)?)))
                    .collect()
            }
            AkitaValue::Json(JsonValue::Object(obj)) => {
                obj.iter()
                    .map(|(k, v)| Ok((k.to_string(), V::from_value_opt(&AkitaValue::Json(v.clone()))?)))
                    .collect()
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "HashMap<String, V>".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}