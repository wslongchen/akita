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
use crate::{AkitaValue, Array, Params};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub trait IntoAkitaValue {
    fn into_value(&self) -> AkitaValue;
}

// Implement conversion for base types
macro_rules! impl_into_akita_value {
    ($($ty:ty => $variant:ident),*) => {
        $(
            impl IntoAkitaValue for $ty {
                fn into_value(&self) -> AkitaValue {
                    AkitaValue::$variant(self.to_owned())
                }
            }
        )*
    };
}

macro_rules! impl_usined_to_value {
    ($ty:ty, $variant:ident, $target_variant:ident) => {
        impl IntoAkitaValue for $ty {
            fn into_value(&self) -> AkitaValue {
                AkitaValue::$variant(self.to_owned() as $target_variant)
            }
        }
    };
}

impl_usined_to_value!(u8, Tinyint, i8);
impl_usined_to_value!(u16, Smallint, i16);
impl_usined_to_value!(u32, Int, i32);
impl_usined_to_value!(u64, Bigint, i64);
impl_usined_to_value!(usize, Bigint, i64);
impl_usined_to_value!(isize, Bigint, i64);

impl_into_akita_value! {
    i8 => Tinyint,
    bool => Bool,
    Vec<u8> => Blob,
    i16 => Smallint,
    i32 => Int,
    i64 => Bigint,
    String => Text,
    f32 => Float,
    f64 => Double,
    BigDecimal => BigDecimal,
    char => Char,
    Uuid => Uuid,
    NaiveDate => Date,
    NaiveTime => Time,
    NaiveDateTime => DateTime,
    DateTime<Utc> => Timestamp
}

impl IntoAkitaValue for &str {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Text(self.to_string())
    }
}

impl IntoAkitaValue for serde_json::Value {
    fn into_value(&self) -> AkitaValue {
        match self {
            serde_json::Value::Null => AkitaValue::Null,
            serde_json::Value::Bool(v) => AkitaValue::Bool(v.to_owned()),
            serde_json::Value::Number(v) => {
                if v.is_f64() {
                    AkitaValue::Double(v.as_f64().unwrap_or_default())
                } else if v.is_i64() {
                    AkitaValue::Bigint(v.as_i64().unwrap_or_default())
                } else if v.is_u64() {
                    AkitaValue::Bigint(v.as_u64().unwrap_or_default() as i64)
                } else {
                    AkitaValue::Int(0)
                }
            },
            serde_json::Value::String(v) => AkitaValue::Text(v.to_owned()),
            serde_json::Value::Array(v) => v.clone().into_value(),
            serde_json::Value::Object(data) => {
                let mut map: IndexMap<String, AkitaValue> = IndexMap::new();
                for key in data.keys() {
                    if let Some(v) = self.get(key) {
                        map.insert(key.to_string(), serde_json::Value::into_value(v));
                    }
                }
                AkitaValue::Object(map)
            },
        }
    }
}


impl IntoAkitaValue for Vec<serde_json::Value> {
    fn into_value(&self) -> AkitaValue {
        if self.is_empty() {
            return AkitaValue::Null
        }
        let mut int_values = Vec::new();
        let mut float_values = Vec::new();
        let mut text_values = Vec::new();
        let mut obj_values = Vec::new();
        for v in self {
            if v.is_f64() {
                float_values.push(v.as_f64().unwrap_or_default());
            } else if v.is_i64() {
                int_values.push(v.as_i64().unwrap_or_default());
            } else if v.is_u64() {
                int_values.push(v.as_i64().unwrap_or_default());
            } else if v.is_string() {
                text_values.push(v.as_str().unwrap_or_default().to_string());
            } else if v.is_object() {
                obj_values.push(v.to_owned());
            } else {
                text_values.push(v.to_string());
            }
        }
        if !int_values.is_empty() {
            AkitaValue::Array(Array::Bigint(int_values))
        } else if !float_values.is_empty() {
            AkitaValue::Array(Array::Double(float_values))
        } else if !obj_values.is_empty() {
            AkitaValue::Array(Array::Json(obj_values))
        } else{
            AkitaValue::Array(Array::Text(text_values))
        }
    }
}

impl IntoAkitaValue for i128 {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Text(self.to_string())
    }
}

impl IntoAkitaValue for u128 {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Text(self.to_string())
    }
}


impl IntoAkitaValue for () {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Null
    }
}

// Option Type support
impl<T: IntoAkitaValue> IntoAkitaValue for Option<T> {
    fn into_value(&self) -> AkitaValue {
        match self {
            Some(val) => val.into_value(),
            None => AkitaValue::Null,
        }
    }
}

impl<K, V> IntoAkitaValue for IndexMap<K, V>
where
    K: Into<String> + Clone,
    V: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        let converted: IndexMap<String, AkitaValue> = self
            .into_iter()
            .map(|(k, v)| (k.clone().into(), v.into_value()))
            .collect();
        AkitaValue::Object(converted)
    }
}

impl<K, V> IntoAkitaValue for HashMap<K, V>
where
    K: Into<String> + Clone,
    V: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        let converted: IndexMap<String, AkitaValue> = self
            .into_iter()
            .map(|(k, v)| (k.clone().into(), v.into_value()))
            .collect();
        AkitaValue::Object(converted)
    }
}


impl<V> IntoAkitaValue for HashSet<V>
where
    V: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        let converted = self.into_iter().map(|v| v.into_value()).collect();
        AkitaValue::List(converted)
    }
}



#[allow(suspicious_double_ref_op)]
impl<T: IntoAkitaValue> IntoAkitaValue for &T {
    fn into_value(&self) -> AkitaValue {
        let v = self.clone();
        v.into_value()
    }
}

impl IntoAkitaValue for Params {
    fn into_value(&self) -> AkitaValue {
        match self {
            Params::None => AkitaValue::Null,
            Params::Positional(v) => AkitaValue::Array(Array::Value(v.clone())),
            Params::Named(v) => {
                let mut imp = IndexMap::new();
                for (k,v) in v.iter() {
                    imp.insert(k.clone(), v.clone());
                }
                AkitaValue::Object(imp)
            },
        }
    }
}
