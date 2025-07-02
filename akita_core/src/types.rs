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

use serde::{Deserialize, Serialize};
use crate::value::{Array, Value};


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Real,
    Float,
    Double,
    Numeric,
    Tinyblob,
    Mediumblob,
    Blob,
    Longblob,
    Varbinary,
    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,
    Json,
    TsVector,
    Uuid,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Interval,
    IpAddress,
    Point,
    // enum list with the choices value
    Enum(String, Vec<String>),
    Array(Box<SqlType>),
}

impl SqlType {
    pub fn is_array_type(&self) -> bool {
        match *self {
            SqlType::Array(_) => true,
            _ => false,
        }
    }

    pub fn is_integer_type(&self) -> bool {
        match *self {
            SqlType::Int => true,
            SqlType::Tinyint => true,
            SqlType::Smallint => true,
            SqlType::Bigint => true,
            _ => false,
        }
    }

    pub fn is_decimal_type(&self) -> bool {
        match *self {
            SqlType::Real => true,
            SqlType::Float => true,
            SqlType::Double => true,
            SqlType::Numeric => true,
            _ => false,
        }
    }

    pub fn cast_as(&self) -> Option<SqlType> {
        match *self {
            SqlType::TsVector => Some(SqlType::Text),
            _ => None,
        }
    }

    pub fn name(&self) -> String {
        match *self {
            SqlType::Text => "text".into(),
            SqlType::TsVector => "tsvector".into(),
            SqlType::Array(ref ty) => match ty.as_ref() {
                SqlType::Text => "text[]".into(),
                _ => panic!("not yet dealt {:?}", self),
            },
            _ => panic!("not yet dealt {:?}", self),
        }
    }

    pub fn as_string(&self) -> String {
        match *self {
            SqlType::Text => "text".into(),
            SqlType::Bool => "bool".into(),
            SqlType::Tinyint => "tinyint".into(),
            SqlType::Smallint => "smallint".into(),
            SqlType::Int => "int".into(),
            SqlType::Bigint => "bigint".into(),
            SqlType::Real => "real".into(),
            SqlType::Float => "float".into(),
            SqlType::Double => "double".into(),
            SqlType::Numeric => "numeric".into(),
            SqlType::Tinyblob => "tinyblob".into(),
            SqlType::Mediumblob => "mediumblob".into(),
            SqlType::Blob => "blob".into(),
            SqlType::Longblob => "longblob".into(),
            SqlType::Varbinary => "varbinary".into(),
            SqlType::Char => "char".into(),
            SqlType::Varchar => "varchar".into(),
            SqlType::Tinytext => "tinytext".into(),
            SqlType::Mediumtext => "mediumtext".into(),
            SqlType::Json => "json".into(),
            SqlType::Date => "date".into(),
            SqlType::Timestamp => "timestamp".into(),
            SqlType::TimestampTz => "timestamp".into(),
            SqlType::Time => "time".into(),
            SqlType::TimeTz => "time".into(),
            SqlType::Point => "point".into(),
            SqlType::Enum(_, _) => "enum".into(),
            _ => String::default(),
        }
    }

    pub fn from_str(sql_type: &str) -> Self {
        match sql_type {
            "text" => SqlType::Text,
            "bool"=> SqlType::Bool ,
            "tinyint" => SqlType::Tinyint,
            "smallint" => SqlType::Smallint,
            "int" => SqlType::Int,
            "bigint" => SqlType::Bigint,
            "real" => SqlType::Real,
            "float" => SqlType::Float,
            "double" =>SqlType::Double ,
            "numeric" => SqlType::Numeric,
            "tinyblob" => SqlType::Tinyblob,
            "mediumblob" => SqlType::Mediumblob,
            "blob" => SqlType::Blob,
            "longblob" => SqlType::Longblob,
            "varbinary" => SqlType::Varbinary,
            "char" => SqlType::Char,
            "varchar" => SqlType::Varchar,
            "tinytext"=> SqlType::Tinytext,
            "mediumtext" => SqlType::Mediumtext,
            "json" => SqlType::Json,
            "date" => SqlType::Date,
            "timestamp" => SqlType::Timestamp,
            "time" => SqlType::Time,
            "point" => SqlType::Point,
            _ => SqlType::Text,
        }
    }
}

#[allow(unused)]
#[derive(Debug, PartialEq, Clone)]
pub enum ArrayType {
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,

    Real,
    Float,
    Double,
    Numeric,

    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    Enum(String, Vec<String>),
}

trait HasType {
    fn get_type(&self) -> Option<SqlType>;
}

impl HasType for Value {
    fn get_type(&self) -> Option<SqlType> {
        match self {
            Value::Null => None,
            Value::Bool(_) => Some(SqlType::Bool),
            Value::Tinyint(_) => Some(SqlType::Tinyint),
            Value::Smallint(_) => Some(SqlType::Smallint),
            Value::Int(_) => Some(SqlType::Int),
            Value::Bigint(_) => Some(SqlType::Bigint),
            Value::Float(_) => Some(SqlType::Float),
            Value::Double(_) => Some(SqlType::Double),
            Value::BigDecimal(_) => Some(SqlType::Numeric),
            Value::Blob(_) => Some(SqlType::Blob),
            Value::Char(_) => Some(SqlType::Char),
            Value::Text(_) => Some(SqlType::Text),
            Value::Json(_) => Some(SqlType::Json),
            Value::Uuid(_) => Some(SqlType::Uuid),
            Value::Date(_) => Some(SqlType::Date),
            Value::Time(_) => Some(SqlType::Time),
            Value::DateTime(_) => Some(SqlType::Timestamp),
            Value::Timestamp(_) => Some(SqlType::Timestamp),
            Value::Interval(_) => Some(SqlType::Interval),
            // Value::Point(_) => Some(SqlType::Point),
            Value::Array(Array::Int(_)) => Some(SqlType::Array(Box::new(SqlType::Int))),
            Value::Array(Array::Float(_)) => Some(SqlType::Array(Box::new(SqlType::Float))),
            Value::Array(Array::Text(_)) => Some(SqlType::Array(Box::new(SqlType::Text))),
            Value::Array(Array::Json(_)) => Some(SqlType::Array(Box::new(SqlType::Json))),
            Value::Array(Array::Bool(_)) => Some(SqlType::Array(Box::new(SqlType::Bool))),
            Value::Array(Array::Tinyint(_)) => Some(SqlType::Array(Box::new(SqlType::Tinyint))),
            Value::Array(Array::Smallint(_)) => Some(SqlType::Array(Box::new(SqlType::Smallint))),
            Value::Array(Array::Bigint(_)) => Some(SqlType::Array(Box::new(SqlType::Bigint))),
            Value::Array(Array::BigDecimal(_)) => Some(SqlType::Array(Box::new(SqlType::Float))),
            Value::Array(Array::Date(_)) => Some(SqlType::Array(Box::new(SqlType::Date))),
            Value::Array(Array::Timestamp(_)) => Some(SqlType::Array(Box::new(SqlType::Timestamp))),
            Value::Array(Array::Uuid(_)) => Some(SqlType::Array(Box::new(SqlType::Uuid))),
            Value::Array(Array::Double(_)) => Some(SqlType::Array(Box::new(SqlType::Double))),
            Value::Array(Array::Char(_)) => Some(SqlType::Array(Box::new(SqlType::Char))),
            // Value::SerdeJson(_) => Some(SqlType::Json),
            Value::Object(_) => Some(SqlType::Json),
        }
    }
}

impl SqlType {
    pub fn same_type(&self, value: &Value) -> bool {
        if let Some(simple_type) = value.get_type() {
            if simple_type == *self {
                return true;
            }
            match (self, value) {
                (SqlType::Varchar, Value::Text(_)) => true,
                (SqlType::TimestampTz, Value::Timestamp(_)) => true,
                (_, _) => false,
            }
        } else {
            false
        }
    }
}
