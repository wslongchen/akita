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
use std::fmt;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::{AkitaDataError, AkitaValue, ConversionError, FromAkitaValue, IntoAkitaValue};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Array {
    Bool(Vec<bool>),
    Tinyint(Vec<i8>),
    Smallint(Vec<i16>),
    Int(Vec<i32>),
    Float(Vec<f32>),
    Bigint(Vec<i64>),
    Double(Vec<f64>),
    BigDecimal(Vec<BigDecimal>),
    Text(Vec<String>),
    Json(Vec<serde_json::Value>),
    Char(Vec<char>),
    Uuid(Vec<Uuid>),
    Date(Vec<NaiveDate>),
    Timestamp(Vec<DateTime<Utc>>),
    Blob(Vec<Vec<u8>>),
    Value(Vec<AkitaValue>),
}

impl IntoAkitaValue for Array {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Array(self.clone())
    }
}

// Implement FromAkitaValue for the Array itself
impl FromAkitaValue for Array {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Array(array) => Ok(array.clone()),
            AkitaValue::List(list) => {
                // Try to convert from List to Array
                if list.is_empty() {
                    return Ok(Array::Value(vec![]));
                }

                // Check that all elements are of the same type
                let first_type = match &list[0] {
                    AkitaValue::Bool(_) => "bool",
                    AkitaValue::Int(_) => "i32",
                    AkitaValue::Text(_) => "String",
                    AkitaValue::Blob(_) => "Vec<u8>",
                    _ => "Value",
                };

                // Construct an array of the appropriate type
                match first_type {
                    "bool" => {
                        let mut arr = Vec::with_capacity(list.len());
                        for item in list {
                            if let AkitaValue::Bool(b) = item {
                                arr.push(*b);
                            } else {
                                return Err(AkitaDataError::ParseError("The type does not match".to_string()));
                            }
                        }
                        Ok(Array::Bool(arr))
                    }
                    "String" => {
                        let mut arr = Vec::with_capacity(list.len());
                        for item in list {
                            if let AkitaValue::Text(s) = item {
                                arr.push(s.clone());
                            } else {
                                return Err(AkitaDataError::ParseError("The type does not match".to_string()));
                            }
                        }
                        Ok(Array::Text(arr))
                    }
                    "Vec<u8>" => {
                        let mut arr = Vec::with_capacity(list.len());
                        for item in list {
                            if let AkitaValue::Blob(b) = item {
                                arr.push(b.clone());
                            } else {
                                return Err(AkitaDataError::ParseError("The type does not match".to_string()));
                            }
                        }
                        Ok(Array::Blob(arr))
                    }
                    _ => Ok(Array::Value(list.clone())),
                }
            }
            _ =>  Err(AkitaDataError::ParseError("The type does not match".to_string()))
        }
    }
}

impl Array {
    pub fn new<T>(values: Vec<T>) -> Self
    where
        T: IntoArrayElement,
    {
        T::into_array(values)
    }
    
    pub fn from_vec<T: IntoArrayElement>(vec: Vec<T>) -> Self {
        T::into_array(vec)
    }

    // Convenient way to convert to VEC
    pub fn to_vec<T>(&self) -> Result<Vec<T>, AkitaDataError>
    where
        Vec<T>: FromAkitaValue,
    {
        Vec::<T>::from_value_opt(&AkitaValue::Array(self.clone()))
    }
    
    // Convert to List
    pub fn to_list(&self) -> Result<Vec<AkitaValue>, AkitaDataError> {
        array_to_list(self)
    }
    
    pub fn len(&self) -> usize {
        match self {
            Array::Bool(v) => v.len(),
            Array::Tinyint(v) => v.len(),
            Array::Smallint(v) => v.len(),
            Array::Int(v) => v.len(),
            Array::Bigint(v) => v.len(),
            Array::Float(v) => v.len(),
            Array::Double(v) => v.len(),
            Array::BigDecimal(v) => v.len(),
            Array::Char(v) => v.len(),
            Array::Text(v) => v.len(),
            Array::Json(v) => v.len(),
            Array::Uuid(v) => v.len(),
            Array::Date(v) => v.len(),
            Array::Timestamp(v) => v.len(),
            Array::Blob(v) => v.len(),
            Array::Value(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// trait is used to convert Rust types to Array elements
pub trait IntoArrayElement {
    fn into_array(values: Vec<Self>) -> Array where Self: Sized;
}
macro_rules! impl_into_array_element {
    ($type:ty, $variant:ident) => {
        impl IntoArrayElement for $type {
            fn into_array(values: Vec<Self>) -> Array {
                Array::$variant(values)
            }
        }
    };
}

macro_rules! impl_into_akita_value_array_element {
    ($type:ty, $variant:ident) => {
        impl IntoAkitaValue for Vec<$type> {
            fn into_value(&self) -> AkitaValue {
                // Array::$variant(values)
                AkitaValue::Array(IntoArrayElement::into_array(self.clone()))
            }
        }
    };
}



impl_into_akita_value_array_element!(bool, Bool);
impl_into_akita_value_array_element!(i8, Tinyint);
impl_into_akita_value_array_element!(i16, Smallint);
impl_into_akita_value_array_element!(i32, Int);
impl_into_akita_value_array_element!(i64, Bigint);
impl_into_akita_value_array_element!(f32, Float);
impl_into_akita_value_array_element!(f64, Double);
impl_into_akita_value_array_element!(String, Text);
impl_into_akita_value_array_element!(char, Char);
impl_into_akita_value_array_element!(Vec<u8>, Blob);



impl_into_array_element!(bool, Bool);
impl_into_array_element!(i8, Tinyint);
impl_into_array_element!(i16, Smallint);
impl_into_array_element!(i32, Int);
impl_into_array_element!(i64, Bigint);
impl_into_array_element!(f32, Float);
impl_into_array_element!(f64, Double);
impl_into_array_element!(String, Text);
impl_into_array_element!(char, Char);
impl_into_array_element!(Vec<u8>, Blob);


impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Array::Text(texts) => {
                write!(f, "[{}]", texts.join(","))
            }
            Array::Float(floats) => {
                write!(f, "[{}]", serde_json::to_string(&floats).unwrap_or_default())
            }
            Array::Json(json) => {
                write!(f, "{}", serde_json::to_string(&json).unwrap_or_default())
            }
            Array::Bool(bools) => {
                write!(f, "[{}]", serde_json::to_string(&bools).unwrap_or_default())
            }
            Array::Tinyint(tinyints) => {
                write!(f, "[{}]", serde_json::to_string(&tinyints).unwrap_or_default())
            }
            Array::Smallint(smallints) => {
                write!(f, "[{}]", serde_json::to_string(&smallints).unwrap_or_default())
            }
            Array::Int(ints) => {
                write!(f, "[{}]", serde_json::to_string(&ints).unwrap_or_default())
            }
            Array::Bigint(bigints) => {
                write!(f, "[{}]", serde_json::to_string(&bigints).unwrap_or_default())
            }
            Array::Double(doubles) => {
                write!(f, "[{}]", serde_json::to_string(&doubles).unwrap_or_default())
            }
            Array::BigDecimal(bigdecimals) => {
                let fmt = bigdecimals.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "[{}]", fmt)
            }
            Array::Char(chars) => {
                write!(f, "[{}]", serde_json::to_string(&chars).unwrap_or_default())
            }
            Array::Uuid(uuids) => {
                let fmt = uuids.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "[{}]", fmt)
            }
            Array::Date(dates) => {
                write!(f, "[{}]", serde_json::to_string(&dates).unwrap_or_default())
            }
            Array::Timestamp(timestamps) => {
                write!(f, "[{}]", serde_json::to_string(&timestamps).unwrap_or_default())
            }
            Array::Value(v) => {
                write!(f, "[{}]", serde_json::to_string(&v).unwrap_or_default())
            }
            Array::Blob(v) => {
                write!(f, "[{}]", serde_json::to_string(&v).unwrap_or_default())
            }
            // _ => panic!("not yet implemented: {:?}", self),
        }
    }
}


/// <u8> Implemented separately for Vec
impl FromAkitaValue for Vec<u8> {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Blob(data) => Ok(data.clone()),
            AkitaValue::Text(s) => Ok(s.to_string().into_bytes()),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "Vec<u8>".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}
// Use macros to implement in bulk for other types
macro_rules! impl_vec_from_akita_value {
    ($type:ty, $array_variant:ident) => {
        impl FromAkitaValue for Vec<$type> {
            fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
                match value {
                    AkitaValue::List(list) => {
                        let mut result = Vec::with_capacity(list.len());
                        for item in list {
                            result.push(<$type>::from_value_opt(item)?);
                        }
                        Ok(result)
                    }
                    AkitaValue::Array(Array::$array_variant(arr)) => {
                        Ok(arr.clone())
                    }
                    _ => Err(AkitaDataError::ConversionError(
                        ConversionError::TypeMismatch {
                            expected: format!("Vec<{}>", stringify!($type)),
                            found: format!("{:?}", value),
                        }
                    )),
                }
            }
        }
    };
}


impl_vec_from_akita_value!(bool, Bool);
impl_vec_from_akita_value!(i8, Tinyint);
impl_vec_from_akita_value!(i16, Smallint);
impl_vec_from_akita_value!(i32, Int);
impl_vec_from_akita_value!(i64, Bigint);
impl_vec_from_akita_value!(f32, Float);
impl_vec_from_akita_value!(f64, Double);
impl_vec_from_akita_value!(String, Text);
impl_vec_from_akita_value!(char, Char);

// Auxiliary function: Convert Array to List
fn array_to_list(array: &Array) -> Result<Vec<AkitaValue>, AkitaDataError> {
    match array {
        Array::Bool(v) => Ok(v.iter().map(|&x| AkitaValue::Bool(x)).collect()),
        Array::Int(v) => Ok(v.iter().map(|&x| AkitaValue::Int(x)).collect()),
        Array::Text(v) => Ok(v.iter().map(|x| AkitaValue::Text(x.clone())).collect()),
        Array::Blob(v) => Ok(v.iter().map(|x| AkitaValue::Blob(x.clone())).collect()),
        Array::Value(v) => Ok(v.clone()),
        // ... Other types
        _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
            expected: "Vec<T>".to_string(),
            found: format!("{:?}", array),
        })),
    }
}