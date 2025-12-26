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
    JsonValue => Json,
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
