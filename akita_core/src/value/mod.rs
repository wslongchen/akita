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

use bigdecimal::{BigDecimal};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indexmap::IndexMap;
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::{fmt, mem};
use uuid::Uuid;


mod interval;
mod array;
mod tuple;
mod blob;
mod expr;
mod to_value;
mod from_value;

use crate::{AkitaDataError, ConversionError, Wrapper};
pub use array::*;
pub use blob::*;
pub use expr::*;
pub use from_value::*;
pub use interval::*;
pub use to_value::*;
pub use tuple::*;

#[derive(Debug, Clone, PartialEq)]
pub enum AkitaValue {
    // ========== Underlying Data Type (Value Compatible)==========
    Null,
    Bool(bool),
    Tinyint(i8),
    Smallint(i16),
    Int(i32),
    Bigint(i64),
    Float(f32),
    Double(f64),
    BigDecimal(BigDecimal),
    Blob(Vec<u8>),
    Char(char),
    Text(String),
    Json(JsonValue),
    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Interval(Interval),
    Array(Array),
    Object(IndexMap<String, AkitaValue>),

    // ========== SQL Specific type ==========
    Column(String),
    RawSql(String),
    Wrapper(Box<Wrapper>),

    // ========== Container type ==========
    List(Vec<AkitaValue>),
}

#[derive(Serialize, Deserialize)]
enum AkitaValueType {
    Null,
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Float,
    Double,
    BigDecimal,
    Blob,
    Char,
    Text,
    Json,
    Uuid,
    Array,
    Date,
    Time,
    DateTime,
    Timestamp,
    Object,
    Column,
    RawSql,
    Wrapper,
    List,
    Interval,
}

// Implement Serialize
impl Serialize for AkitaValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Use the wrapper structure to include type information
        match self {
            AkitaValue::Null => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValue::Null)?;
                state.serialize_field("value", &())?;
                state.end()
            }
            AkitaValue::Bool(b) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Bool)?;
                state.serialize_field("value", b)?;
                state.end()
            }
            AkitaValue::Tinyint(i) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Tinyint)?;
                state.serialize_field("value", i)?;
                state.end()
            }
            AkitaValue::Smallint(i) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Smallint)?;
                state.serialize_field("value", i)?;
                state.end()
            }
            AkitaValue::Int(i) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Int)?;
                state.serialize_field("value", i)?;
                state.end()
            }
            AkitaValue::Bigint(i) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Bigint)?;
                state.serialize_field("value", i)?;
                state.end()
            }
            AkitaValue::Float(f) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Float)?;
                state.serialize_field("value", f)?;
                state.end()
            }
            AkitaValue::Double(f) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Double)?;
                state.serialize_field("value", f)?;
                state.end()
            }
            AkitaValue::BigDecimal(bd) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::BigDecimal)?;
                state.serialize_field("value", &bd.to_string())?;
                state.end()
            }
            AkitaValue::Blob(blob) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Blob)?;
                state.serialize_field("value", &base64::encode(blob))?;
                state.end()
            }
            AkitaValue::Char(c) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Char)?;
                state.serialize_field("value", &c.to_string())?;
                state.end()
            }
            AkitaValue::Text(s) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Text)?;
                state.serialize_field("value", s)?;
                state.end()
            }
            AkitaValue::Json(value) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Json)?;
                state.serialize_field("value", value)?;
                state.end()
            }
            AkitaValue::Uuid(uuid) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Uuid)?;
                state.serialize_field("value", &uuid.to_string())?;
                state.end()
            }
            AkitaValue::Date(date) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Date)?;
                state.serialize_field("value", &date.to_string())?;
                state.end()
            }
            AkitaValue::Time(time) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Time)?;
                state.serialize_field("value", &time.to_string())?;
                state.end()
            }
            AkitaValue::DateTime(dt) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::DateTime)?;
                state.serialize_field("value", &dt.to_string())?;
                state.end()
            }
            AkitaValue::Timestamp(ts) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Timestamp)?;
                state.serialize_field("value", &ts.to_rfc3339())?;
                state.end()
            }
            AkitaValue::Object(map) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Object)?;
                let data = map.iter().collect::<HashMap<_, _>>();
                state.serialize_field("value", &data)?;
                state.end()
            }
            AkitaValue::Column(c) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Column)?;
                state.serialize_field("value", c)?;
                state.end()
            }
            AkitaValue::RawSql(sql) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::RawSql)?;
                state.serialize_field("value", sql)?;
                state.end()
            }
            AkitaValue::Wrapper(w) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Wrapper)?;
                // 对于 Wrapper，我们序列化其 SQL 表示
                state.serialize_field("value", &w.to_string())?;
                state.end()
            }
            AkitaValue::List(items) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::List)?;
                state.serialize_field("value", items)?;
                state.end()
            }
            AkitaValue::Interval(iv) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Interval)?;
                state.serialize_field("value", &iv.to_string())?;
                state.end()
            }
            AkitaValue::Array(arr) => {
                let mut state = serializer.serialize_struct("AkitaValue", 2)?;
                state.serialize_field("type", &AkitaValueType::Array)?;
                state.serialize_field("value", &arr.to_string())?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for AkitaValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 期望的结构：{"type": "TypeName", "value": ...}
        #[derive(Deserialize)]
        struct AkitaValueHelper {
            r#type: AkitaValueType,
            value: serde_json::Value,
        }

        let helper = AkitaValueHelper::deserialize(deserializer)?;

        match helper.r#type {
            AkitaValueType::Null => Ok(AkitaValue::Null),
            AkitaValueType::Bool => {
                let b = bool::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Bool(b))
            }
            AkitaValueType::Tinyint => {
                let i = i8::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Tinyint(i))
            }
            AkitaValueType::Smallint => {
                let i = i16::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Smallint(i))
            }
            AkitaValueType::Int => {
                let i = i32::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Int(i))
            }
            AkitaValueType::Bigint => {
                let i = i64::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Bigint(i))
            }
            AkitaValueType::Float => {
                let f = f32::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Float(f))
            }
            AkitaValueType::Double => {
                let f = f64::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Double(f))
            }
            AkitaValueType::BigDecimal => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let bd = s.parse::<BigDecimal>()
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::BigDecimal(bd))
            }
            AkitaValueType::Blob => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let bytes = base64::decode(&s)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Blob(bytes))
            }
            AkitaValueType::Char => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let c = s.chars().next()
                    .ok_or_else(|| de::Error::custom("Empty string for char"))?;
                Ok(AkitaValue::Char(c))
            }
            AkitaValueType::Text => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Text(s))
            }
            AkitaValueType::Json => {
                let json = JsonValue::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Json(json))
            }
            AkitaValueType::Uuid => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let uuid = Uuid::parse_str(&s)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Uuid(uuid))
            }
            AkitaValueType::Date => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let date = NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Date(date))
            }
            AkitaValueType::Time => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let time = NaiveTime::parse_from_str(&s, "%H:%M:%S")
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Time(time))
            }
            AkitaValueType::DateTime => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let dt = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::DateTime(dt))
            }
            AkitaValueType::Timestamp => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let ts = DateTime::parse_from_rfc3339(&s)
                    .map_err(de::Error::custom)?
                    .with_timezone(&Utc);
                Ok(AkitaValue::Timestamp(ts))
            }
            AkitaValueType::Object => {
                let map: HashMap<String, AkitaValue> = HashMap::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                let mut im = IndexMap::new();
                // 遍历 HashMap 并按顺序插入
                for (k, v) in map {
                    im.insert(k.clone(), v.clone());
                }
                Ok(AkitaValue::Object(im))
            }
            AkitaValueType::Column => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::Column(s))
            }
            AkitaValueType::RawSql => {
                let s = String::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::RawSql(s))
            }
            AkitaValueType::Wrapper => {
                // Wrapper 无法完整反序列化，返回默认值
                Ok(AkitaValue::Wrapper(Box::new(Wrapper::new())))
            }
            AkitaValueType::List => {
                let items: Vec<AkitaValue> = Vec::deserialize(helper.value)
                    .map_err(de::Error::custom)?;
                Ok(AkitaValue::List(items))
            }
            AkitaValueType::Array => {
                Ok(AkitaValue::Array(Array::Text(vec![])))
            }
            AkitaValueType::Interval => {
                Ok(AkitaValue::Interval(Interval::new(0,0,0)))
            }
        }
    }
}


impl Default for AkitaValue {
    fn default() -> Self {
        AkitaValue::Null
    }
}

impl <'a> From<&'a AkitaValue> for AkitaValue {
    fn from(v: &'a AkitaValue) -> AkitaValue {
        v.to_owned()
    }
}

impl<T> From<T> for AkitaValue
where
    T: IntoAkitaValue,
{
    fn from(v: T) -> AkitaValue {
        v.into_value()
    }
}


impl AkitaValue {

    // ========== Type check method ==========

    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn is_object(&self) -> bool {
        self.as_object().is_some()
    }

    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn is_number(&self) -> bool {
        matches!(
            *self,
            AkitaValue::Tinyint(_) 
            | AkitaValue::Smallint(_) 
            | AkitaValue::Int(_)
            | AkitaValue::Bigint(_)  
            | AkitaValue::Float(_) 
            | AkitaValue::Double(_)
            | AkitaValue::BigDecimal(_)
        )
    }

    pub fn is_boolean(&self) -> bool {
        self.as_bool().is_some()
    }

    pub fn is_null(&self) -> bool {
        self.as_null().is_some()
    }

    pub fn is_zero(&self) -> bool {
        match self {
            AkitaValue::Tinyint(v) => *v == 0,
            AkitaValue::Smallint(v) => *v == 0,
            AkitaValue::Int(v) => *v == 0,
            AkitaValue::Bigint(v) => *v == 0,
            AkitaValue::Float(v) => *v == 0.0,
            AkitaValue::Double(v) => *v == 0.0,
            AkitaValue::Text(v) => v.eq("0"),
            _ => false,
        }
    }

    // ========== Type conversion method ==========

    pub fn as_object(&self) -> Option<&IndexMap<String, AkitaValue>> {
        match self {
            AkitaValue::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut IndexMap<String, AkitaValue>> {
        match self {
            AkitaValue::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<AkitaValue>> {
        match self {
            AkitaValue::List(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Vec<AkitaValue>> {
        match self {
            AkitaValue::List(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            AkitaValue::Text(s) => Some(s.as_str()),
            AkitaValue::Char(c) => {
                Some(Box::leak(c.to_string().into_boxed_str()))
            }
            _ => None,
        }
    }

    pub fn to_str(&self) -> Option<&str> {
        match self {
            AkitaValue::Text(s) => Some(s.as_str()),
            AkitaValue::Char(c) => {
                Some(Box::leak(c.to_string().into_boxed_str()))
            }
            AkitaValue::Bool(b) => Some(Box::leak(b.to_string().into_boxed_str())),
            AkitaValue::Tinyint(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Smallint(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Int(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Bigint(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Float(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Double(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::BigDecimal(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Json(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Uuid(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Date(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::Time(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            AkitaValue::DateTime(v) => Some(Box::leak(v.format("%Y-%m-%d %H:%M:%S").to_string().into_boxed_str())),
            AkitaValue::Timestamp(v) => Some(Box::leak(v.to_string().into_boxed_str())),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AkitaValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            AkitaValue::Null => Some(()),
            _ => None,
        }
    }

    // ========== Numeric type conversion ==========

    pub fn as_i8(&self) -> Option<i8> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v),
            AkitaValue::Smallint(v) => Some(*v as i8),
            AkitaValue::Int(v) => Some(*v as i8),
            AkitaValue::Bigint(v) => Some(*v as i8),
            AkitaValue::Float(v) => Some(*v as i8),
            AkitaValue::Double(v) => Some(*v as i8),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v as i16),
            AkitaValue::Smallint(v) => Some(*v),
            AkitaValue::Int(v) => Some(*v as i16),
            AkitaValue::Bigint(v) => Some(*v as i16),
            AkitaValue::Float(v) => Some(*v as i16),
            AkitaValue::Double(v) => Some(*v as i16),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v as i32),
            AkitaValue::Smallint(v) => Some(*v as i32),
            AkitaValue::Int(v) => Some(*v),
            AkitaValue::Bigint(v) => Some(*v as i32),
            AkitaValue::Float(v) => Some(*v as i32),
            AkitaValue::Double(v) => Some(*v as i32),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v as i64),
            AkitaValue::Smallint(v) => Some(*v as i64),
            AkitaValue::Int(v) => Some(*v as i64),
            AkitaValue::Bigint(v) => Some(*v),
            AkitaValue::Float(v) => Some(*v as i64),
            AkitaValue::Double(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v as f32),
            AkitaValue::Smallint(v) => Some(*v as f32),
            AkitaValue::Int(v) => Some(*v as f32),
            AkitaValue::Bigint(v) => Some(*v as f32),
            AkitaValue::Float(v) => Some(*v),
            AkitaValue::Double(v) => Some(*v as f32),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AkitaValue::Tinyint(v) => Some(*v as f64),
            AkitaValue::Smallint(v) => Some(*v as f64),
            AkitaValue::Int(v) => Some(*v as f64),
            AkitaValue::Bigint(v) => Some(*v as f64),
            AkitaValue::Float(v) => Some(*v as f64),
            AkitaValue::Double(v) => Some(*v),
            _ => None,
        }
    }

    // ========== Object manipulation method ==========

    pub fn take(&mut self) -> AkitaValue {
        mem::replace(self, AkitaValue::Null)
    }

    pub fn new_object() -> Self {
        AkitaValue::Object(IndexMap::new())
    }

    pub fn insert_obj<K, V>(&mut self, k: K, v: V)
    where
        K: ToString,
        V: IntoAkitaValue,
    {
        match self {
            AkitaValue::Object(data) => {
                data.insert(k.to_string().replace("r#",""), v.into_value());
            },
            _ => (),
        }
    }

    pub fn insert_obj_value<K>(&mut self, k: K, value: &AkitaValue)
    where
        K: ToString,
    {
        match self {
            AkitaValue::Object(v) => {
                v.insert(k.to_string(), value.clone());
            },
            _ => (),
        }
    }

    pub fn get_obj<T>(&self, s: &str) -> Result<T, AkitaDataError>
    where
        T: FromAkitaValue,
    {
        match self {
            AkitaValue::Object(data) => match data.get(&s.replace("r#","")) {
                Some(v) => T::from_value_opt(v),
                None => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                    message: format!("No such key: {}", s),
                })),
            },
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: "Not an object".to_string(),
            })),
        }
    }

    pub fn get_obj_len(&self) -> usize {
        match self {
            AkitaValue::Object(data) => data.len(),
            _ => 0,
        }
    }

    pub fn get_obj_opt<T>(&self, s: &str) -> Result<Option<T>, AkitaDataError>
    where
        T: FromAkitaValue,
    {
        match self {
            AkitaValue::Object(data) => match data.get(&s.replace("r#","")) {
                Some(v) => {
                    match v {
                        AkitaValue::Null => Ok(None),
                        _ => Ok(Some(T::from_value_opt(v)?)),
                    }
                }
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    pub fn get_obj_value(&self, s: &str) -> Option<&AkitaValue> {
        match self {
            AkitaValue::Object(data) => data.get(s),
            _ => None,
        }
    }

    pub fn get_obj_value_mut(&mut self, s: &str) -> Option<&mut AkitaValue> {
        match self {
            AkitaValue::Object(data) => data.get_mut(s),
            _ => None,
        }
    }

    pub fn remove_obj(&mut self, s: &str) -> Option<AkitaValue> {
        match self {
            AkitaValue::Object(v) => v.shift_remove(s),
            _ => None,
        }
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take_obj_raw<T>(&self, index: usize) -> Option<T>
    where
        T: FromAkitaValue,
    {
        match self {
            AkitaValue::Object(v) => {
                v.values().nth(index).and_then(|v| {
                    T::from_value_opt(v).ok()
                })
            },
            _ => None,
        }
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_obj_raw_opt<T>(&self, index: usize) -> Option<Result<T, AkitaDataError>>
    where
        T: FromAkitaValue,
    {
        match self {
            AkitaValue::Object(v) => {
                v.values().nth(index).map(|v| {
                    T::from_value_opt(v)
                })
            },
            _ => Some(Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: "Not an object".to_string(),
            }))),
        }
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: AkitaValue) {
        match self {
            AkitaValue::Object(v) => {
                if let Some(key) = v.keys().nth(index).cloned() {
                    v.insert(key, value);
                }
            }
            _ => (),
        }
    }

    // ========== Added a convenient method ==========

    /// Create a new array
    pub fn new_array() -> Self {
        AkitaValue::List(Vec::new())
    }

    /// Add elements to the array
    pub fn push_array<V: IntoAkitaValue>(&mut self, value: V) {
        match self {
            AkitaValue::List(vec) => {
                vec.push(value.into_value());
            }
            _ => (),
        }
    }

    /// Pop elements from the array
    pub fn pop_array(&mut self) -> Option<AkitaValue> {
        match self {
            AkitaValue::List(vec) => vec.pop(),
            _ => None,
        }
    }

    /// Get the array length
    pub fn array_len(&self) -> usize {
        match self {
            AkitaValue::List(vec) => vec.len(),
            _ => 0,
        }
    }

    /// Check if it is empty
    pub fn is_empty(&self) -> bool {
        match self {
            AkitaValue::Null => true,
            AkitaValue::Text(s) => s.is_empty(),
            AkitaValue::List(vec) => vec.is_empty(),
            AkitaValue::Object(map) => map.is_empty(),
            AkitaValue::Blob(data) => data.is_empty(),
            _ => false,
        }
    }

    /// Convert to JSON values
    pub fn to_json(&self) -> Result<JsonValue, AkitaDataError> {
        JsonValue::from_value_opt(self)
    }

    /// Created from a JSON value
    pub fn from_json(json: JsonValue) -> Self {
        AkitaValue::Json(json)
    }

    /// Secure type conversion (no loss of information)
    pub fn coerce_to_string(&self) -> String {
        match self {
            AkitaValue::Text(s) => s.clone(),
            AkitaValue::Char(c) => c.to_string(),
            AkitaValue::Int(i) => i.to_string(),
            AkitaValue::Bigint(i) => i.to_string(),
            AkitaValue::Float(f) => f.to_string(),
            AkitaValue::Double(f) => f.to_string(),
            AkitaValue::Bool(b) => b.to_string(),
            AkitaValue::Date(d) => d.to_string(),
            AkitaValue::Time(t) => t.to_string(),
            AkitaValue::DateTime(dt) => dt.to_string(),
            AkitaValue::Timestamp(ts) => ts.to_rfc3339(),
            AkitaValue::Uuid(u) => u.to_string(),
            AkitaValue::BigDecimal(bd) => bd.to_string(),
            AkitaValue::Null => "".to_string(),
            AkitaValue::Blob(_) => "[BLOB]".to_string(),
            AkitaValue::List(_) => "[ARRAY]".to_string(),
            AkitaValue::Object(_) => "[OBJECT]".to_string(),
            AkitaValue::Column(c) => c.clone(),
            AkitaValue::RawSql(sql) => sql.clone(),
            AkitaValue::Wrapper(_) => "[WRAPPER]".to_string(),
            AkitaValue::Tinyint(v) => v.to_string(),
            AkitaValue::Smallint(v) => v.to_string(),
            AkitaValue::Json(v) => v.to_string(),
            AkitaValue::Interval(v) => v.to_string(),
            AkitaValue::Array(v) => v.to_string(),
        }
    }

    /// Deep cloning (can be slower for large objects)
    pub fn deep_clone(&self) -> Self {
        match self {
            AkitaValue::Object(map) => {
                AkitaValue::Object(map.clone())
            }
            AkitaValue::List(vec) => {
                let cloned_vec: Vec<AkitaValue> = vec
                    .iter()
                    .map(|v| v.deep_clone())
                    .collect();
                AkitaValue::List(cloned_vec)
            }
            other => other.clone(),
        }
    }
}

impl fmt::Display for AkitaValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // 保持原有实现，但可以优化一些显示
            AkitaValue::Null => write!(f, "null"),
            AkitaValue::Bool(v) => write!(f, "{}", v),
            AkitaValue::Tinyint(v) => write!(f, "{}", v),
            AkitaValue::Smallint(v) => write!(f, "{}", v),
            AkitaValue::Int(v) => write!(f, "{}", v),
            AkitaValue::Bigint(v) => write!(f, "{}", v),
            AkitaValue::Float(v) => write!(f, "{:.6}", v),
            AkitaValue::Double(v) => write!(f, "{:.6}", v),
            AkitaValue::BigDecimal(v) => write!(f, "{}", v),
            AkitaValue::Char(v) => write!(f, "'{}'", v),
            AkitaValue::Text(v) => write!(f, "'{}'", v.replace("'", "''")),
            AkitaValue::Json(v) => {
                let json_str = serde_json::to_string(v).unwrap_or_default();
                if json_str.len() > 100 {
                    write!(f, "JSON(truncated)")
                } else {
                    write!(f, "{}", json_str)
                }
            }
            AkitaValue::Uuid(v) => write!(f, "'{}'", v),
            AkitaValue::Date(v) => write!(f, "DATE '{}'", v),
            AkitaValue::Time(v) => write!(f, "TIME '{}'", v.format("%H:%M:%S%.3f")),
            AkitaValue::DateTime(v) => write!(f, "'{}'", v.format("%Y-%m-%d %H:%M:%S")),
            AkitaValue::Timestamp(v) => write!(f, "'{}'", v.to_rfc3339()),
            AkitaValue::Array(array) => write!(f, "ARRAY({})", array),
            AkitaValue::Blob(v) => {
                if v.len() > 20 {
                    write!(f, "BLOB({} bytes, truncated)", v.len())
                } else {
                    write!(f, "{}", String::from_utf8_lossy(v))
                }
            }
            AkitaValue::Interval(v) => write!(f, "{}", v.to_string()),
            AkitaValue::Object(v) => {
                let map: HashMap<_, _> = v.iter().collect();
                write!(f, "OBJECT({} fields)", map.len())
            }
            AkitaValue::Column(v) => write!(f, "[COLUMN] {}", v),
            AkitaValue::RawSql(v) => write!(f, "[RAW_SQL] {}", v),
            AkitaValue::Wrapper(_v) => write!(f, "[WRAPPER]"),
            AkitaValue::List(v) => {
                if v.is_empty() {
                    write!(f, "[]")
                } else if v.len() <= 5 {
                    let items: Vec<String> = v.iter().map(|item| item.to_string()).collect();
                    write!(f, "[{}]", items.join(", "))
                } else {
                    write!(f, "[{} items]", v.len())
                }
            }
        }
    }
}

