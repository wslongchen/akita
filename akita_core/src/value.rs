use std::{any::type_name, fmt, mem};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Serialize, Deserialize};
use serde_json::Map;
use uuid::Uuid;
use indexmap::{IndexMap};

use crate::error::{ConvertError, AkitaDataError};
use crate::{Row};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Nil, // no value
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
    Json(serde_json::Value),

    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Interval(Interval),
    // SerdeJson(serde_json::Value),
    Object(IndexMap<String, Value>),
    Array(Array),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    pub microseconds: i64,
    pub days: i32,
    pub months: i32,
}

impl Interval {
    pub fn new(microseconds: i64, days: i32, months: i32) -> Self {
        Interval {
            microseconds,
            days,
            months,
        }
    }
    
}

impl Value {
    pub fn is_nil(&self) -> bool {
        *self == Value::Nil
    }

    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn is_object(&self) -> bool {
        self.as_object().is_some()
    }

    pub fn as_object(&self) -> Option<&IndexMap<String, Value>> {
        match *self {
            Value::Object(ref map) => Some(map),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut IndexMap<String, Value>> {
        match *self {
            Value::Object(ref mut map) => Some(map),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn as_array(&self) -> Option<&Array> {
        match *self {
            Value::Array(ref array) => Some(array),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match *self {
            Value::Text(ref s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_number(&self) -> bool {
        match *self {
            Value::Tinyint(_) | Value::Smallint(_) | Value::Int(_)  
            | Value::Bigint(_)  | Value::Float(_) | Value::BigDecimal(_) | Value::Double(_)  => true,
            _ => false,
        }
    }

    pub fn is_boolean(&self) -> bool {
        self.as_bool().is_some()
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        self.as_null().is_some()
    }

    pub fn as_null(&self) -> Option<()> {
        match *self {
            Value::Nil => Some(()),
            _ => None,
        }
    }

    pub fn take(&mut self) -> Value {
        mem::replace(self, Value::Nil)
    }

    pub fn new_object() -> Self { Value::Object(IndexMap::new()) }

    pub fn insert_obj<K, V>(&mut self, k: K, v: V)
    where
        K: ToString,
        V: ToValue,
    {
        match self {
            Value::Object(data) => {data.insert(k.to_string().replace("r#",""), v.to_value());},
            _ => (),
        }
    }

    pub fn insert_obj_value<K>(&mut self, k: K, value: &Value)
    where
        K: ToString,
    {
        match self {
            Value::Object(v) => {v.insert(k.to_string(), value.clone());},
            _ => (),
        }
    }

    pub fn get_obj<'a, T>(&'a self, s: &str) -> Result<T, AkitaDataError>
    where
        T: FromValue,
    {
        match self {
            Value::Object(data) => match data.get(&s.replace("r#","")) {
                Some(v) => {
                    let s = FromValue::from_value_opt(v);
                    s
                },
                None => Err(AkitaDataError::NoSuchValueError(s.into())),
            },
            _ => Err(AkitaDataError::ObjectValidError("Unsupported type".to_string())),
        }
    }

    pub fn get_obj_len<'a>(&'a self) -> usize
    {
        match self {
            Value::Object(data) => data.len(),
            _ => 0,
        }
    }

    pub fn get_obj_opt<'a, T>(&'a self, s: &str) -> Result<Option<T>, AkitaDataError>
    where
        T: FromValue,
    {
        match self {
            Value::Object(data) => match data.get(&s.replace("r#","")) {
                Some(v) => {
                    match v {
                        Value::Nil => Ok(None),
                        _ => {
                            Ok(Some(
                                FromValue::from_value(v)
                            ))
                        }
                    }
                }
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    pub fn get_obj_value(&self, s: &str) -> Option<&Value> { 
        match self {
            Value::Object(data) => data.get(s),
            _ => None,
        }
     }

    pub fn remove_obj(&mut self, s: &str) -> Option<Value> { 
        match self {
            Value::Object(v) => v.remove(s),
            _ => None,
        } 
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take_obj_raw<'a, T>(&'a self, index: usize) -> Option<T>
    where
        T: FromValue,
    {
        match self {
            Value::Object(v) => v.get_index(index).and_then(|(_k, v)| Some(FromValue::from_value(v))),
            _ => None,
        }
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_obj_raw_opt<'a, T>(&'a self, index: usize) -> Option<Result<T, AkitaDataError>>
    where
        T: FromValue,
    {
        match self {
            Value::Object(v) => v.get_index(index).and_then(|(_k, v)| Some(FromValue::from_value_opt(v))),
            _ => Some(Err(AkitaDataError::ObjectValidError("Unsupported type".to_string()))),
        }
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        match self {
            Value::Object(v) => match v.get_index_mut(index) {
                Some((_k, v)) => {
                    *v = value;
                }
                None => ()
            }
            _ => (),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Nil => write!(f, "Null"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Tinyint(v) => write!(f, "{}", v),
            Value::Smallint(v) => write!(f, "{}", v),
            Value::Int(v) => write!(f, "{}", v),
            Value::Bigint(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::BigDecimal(v) => write!(f, "{}", v),
            Value::Char(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", serde_json::to_string(v).unwrap_or_default()),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S").to_string()),
            Value::Timestamp(v) => write!(f, "{}", v.to_rfc3339()),
            Value::Array(array) => array.fmt(f),
            Value::Blob(v) => {
                write!(f, "{}", String::from_utf8_lossy(v))
            }
            _ => panic!("not yet implemented: {:?}", self),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Array {
    Bool(Vec<bool>),
    Tinyint(Vec<i8>),
    Smallint(Vec<i16>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Bigint(Vec<i64>),
    Double(Vec<f64>),
    BigDecimal(Vec<BigDecimal>),
    Text(Vec<String>),
    Json(Vec<serde_json::Value>),
    Char(Vec<char>),
    Uuid(Vec<Uuid>),
    Date(Vec<NaiveDate>),
    Timestamp(Vec<DateTime<Utc>>),
}

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Array::Text(texts) => {
                let json_arr = serde_json::to_string(texts).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Float(floats) => {
                let json_arr = serde_json::to_string(floats).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Json(json) => {
                let json_arr = serde_json::to_string(json).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Bool(bools) => {
                let json_arr = serde_json::to_string(bools).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Tinyint(tinyints) => {
                let json_arr = serde_json::to_string(tinyints).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Smallint(smallints) => {
                let json_arr = serde_json::to_string(smallints).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Int(ints) => {
                let json_arr = serde_json::to_string(ints).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Bigint(bigints) => {
                let json_arr = serde_json::to_string(bigints).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Double(doubles) => {
                let json_arr = serde_json::to_string(doubles).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::BigDecimal(bigdecimals) => {
                let json_arr = bigdecimals.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "{}", json_arr)
            }
            Array::Char(chars) => {
                let json_arr = serde_json::to_string(chars).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Uuid(uuids) => {
                let json_arr = uuids.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "{}", json_arr)
            }
            Array::Date(dates) => {
                let json_arr = dates.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "{}", json_arr)
            }
            Array::Timestamp(timestamps) => {
                let json_arr = timestamps.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "{}", json_arr)
            }
            // _ => panic!("not yet implemented: {:?}", self),
        }
    }
}

/// A trait to allow passing of parameters ergonomically
/// in em.execute_sql_with_return
pub trait ToValue {
    fn to_value(&self) -> Value;
}

macro_rules! impl_to_value {
    ($ty:ty, $variant:ident) => {
        impl ToValue for $ty {
            fn to_value(&self) -> Value {
                Value::$variant(self.to_owned())
            }
        }
    };
}

macro_rules! impl_usined_to_value {
    ($ty:ty, $variant:ident, $target_variant:ident) => {
        impl ToValue for $ty {
            fn to_value(&self) -> Value {
                Value::$variant(self.to_owned() as $target_variant)
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


impl_to_value!(bool, Bool);
impl_to_value!(i8, Tinyint);
impl_to_value!(i16, Smallint);
impl_to_value!(i32, Int);
impl_to_value!(i64, Bigint);
impl_to_value!(f32, Float);
impl_to_value!(f64, Double);
impl_to_value!(Vec<u8>, Blob);
impl_to_value!(char, Char);
impl_to_value!(String, Text);
impl_to_value!(Uuid, Uuid);
impl_to_value!(NaiveDate, Date);
impl_to_value!(NaiveTime, Time);
impl_to_value!(DateTime<Utc>, Timestamp);
impl_to_value!(NaiveDateTime, DateTime);

impl ToValue for &str {
    fn to_value(&self) -> Value {
        Value::Text(self.to_string())
    }
}

impl ToValue for serde_json::Value {
    fn to_value(&self) -> Value {
        match self {
            serde_json::Value::Null => Value::Nil,
            serde_json::Value::Bool(v) => Value::Bool(v.to_owned()),
            serde_json::Value::Number(v) => {
                if v.is_f64() {
                    Value::Double(v.as_f64().unwrap_or_default())
                } else if v.is_i64() {
                    Value::Bigint(v.as_i64().unwrap_or_default())
                } else if v.is_u64() {
                    Value::Bigint(v.as_u64().unwrap_or_default() as i64)
                } else {
                    Value::Int(0)
                }
            },
            serde_json::Value::String(v) => Value::Text(v.to_owned()),
            serde_json::Value::Array(v) => v.to_value(),
            serde_json::Value::Object(data) => {
                let mut map: IndexMap<String, Value> = IndexMap::new();
                for key in data.keys() {
                    if let Some(v) = self.get(key) {
                        map.insert(key.to_string(), serde_json::Value::to_value(v));
                    }
                }
                Value::Object(map)
            },
        }
    }
}

impl ToValue for Vec<String> {
    fn to_value(&self) -> Value {
        Value::Array(Array::Text(self.to_owned()))
    }
}

impl ToValue for Vec<serde_json::Value> {
    fn to_value(&self) -> Value {
        if self.is_empty() {
            return Value::Nil
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
            Value::Array(Array::Int(int_values))
        } else if !float_values.is_empty() {
            Value::Array(Array::Float(float_values))
        } else if !obj_values.is_empty() {
            Value::Array(Array::Json(obj_values))
        } else{
            Value::Array(Array::Text(text_values))
        }
    }
}

impl<T> ToValue for Option<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        
        match self {
            Some(v) => v.to_value(),
            None => Value::Nil,
        }
    }
}

impl ToValue for () {
    fn to_value(&self) -> Value {
        Value::Nil
    }
}

impl <T> ToValue for &T
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        (*self).to_value()
    }
}

impl ToValue for Row {
    fn to_value(&self) -> Value {
        let mut data = IndexMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            data.insert(col.to_string(), self.data.get(i).map(|v| v.clone()).unwrap_or(Value::Nil));
        }
        Value::Object(data)
    }
}

impl <'a> From<&'a Value> for Value {
    fn from(v: &'a Value) -> Value {
        v.to_owned()
    }
}

impl<T> From<T> for Value
where
    T: ToValue,
{
    fn from(v: T) -> Value {
        v.to_value()
    }
}

// impl<'a, T: ToValue> From<&'a T> for Value {
//     fn from(x: &'a T) -> Value {
//         x.to_value()
//     }
// }


pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Self {
        match Self::from_value_opt(v) {
            Ok(x) => x,
            Err(_err) => panic!(
                "Couldn't from {:?} to type {}. (see FromValue documentation)",
                v,
                type_name::<Self>(),
            ),
        }
    }

    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError>;
}

macro_rules! impl_from_value {
    ($ty: ty, $ty_name: tt, $($variant: ident),*) => {
        /// try from to owned
        impl FromValue for $ty {
            fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(format!("{:?}",v), $ty_name.into()))),
                }
            }
        }
    }
}

macro_rules! impl_from_value_numeric {
    ($ty: ty, $method:ident, $ty_name: tt, $($variant: ident),*) => {
        impl FromValue for $ty {
            fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    Value::BigDecimal(ref v) => Ok(v.$method().unwrap_or_default()),
                    Value::Object(ref v) => {
                        let (_, v) = v.first().unwrap_or((&String::default(), &Value::Nil));
                        Ok(<$ty>::from_value(v))
                    },
                    _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(format!("{:?}", v), $ty_name.into()))),
                }
            }
        }
    }
}

impl_from_value!(Vec<u8>, "Vec<u8>", Blob);
impl_from_value!(char, "char", Char);
impl_from_value!(Uuid, "Uuid", Uuid);
impl_from_value!(NaiveDate, "NaiveDate", Date);

impl_from_value_numeric!(i8, to_i8, "i8", Tinyint);
impl_from_value_numeric!(isize, to_isize, "isize", Tinyint, Bigint, Int);
impl_from_value_numeric!(u8, to_u8, "u8", Tinyint, Bigint, Int);
impl_from_value_numeric!(u16, to_u16, "u16", Tinyint, Bigint, Int);
impl_from_value_numeric!(u32, to_u32, "u32", Tinyint, Bigint, Int);
impl_from_value_numeric!(u64, to_u64, "u64", Tinyint, Bigint, Int);
impl_from_value_numeric!(usize, to_usize, "usize", Tinyint, Bigint, Int);
impl_from_value_numeric!(i16, to_i16, "i16", Tinyint, Smallint);
impl_from_value_numeric!(i32, to_i32, "i32", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(i64, to_i64, "i64", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(f32, to_f32, "f32", Float);
impl_from_value_numeric!(f64, to_f64, "f64", Float, Double);

/// Char can be casted into String
/// and they havea separate implementation for extracting data
impl FromValue for String {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Text(ref v) => Ok(v.to_owned()),
            Value::Char(ref v) => {
                let mut s = String::new();
                s.push(*v);
                Ok(s)
            }
            Value::Blob(ref v) => String::from_utf8(v.to_owned()).map_err(|e| {
                AkitaDataError::ConvertError(ConvertError::NotSupported(format!("{:?}", v), format!("String: {}", e)))
            }),
            Value::Bool(ref v) => Ok(v.to_string()),
            Value::Tinyint(ref v) => Ok(v.to_string()),
            Value::Smallint(ref v) => Ok(v.to_string()),
            Value::Int(ref v) => Ok(v.to_string()),
            Value::Bigint(ref v) => Ok(v.to_string()),
            Value::Float(ref v) => Ok(v.to_string()),
            Value::Double(ref v) => Ok(v.to_string()),
            Value::BigDecimal(ref v) => Ok(v.to_string()),
            Value::Json(ref v) => Ok(serde_json::to_string(v).unwrap_or_default()),
            Value::Uuid(ref v) => Ok(v.to_string()),
            Value::Date(ref v) => Ok(v.to_string()),
            Value::Time(ref v) => Ok(v.to_string()),
            Value::DateTime(ref v) => Ok(v.to_string()),
            Value::Timestamp(ref v) => Ok(v.to_string()),
            Value::Array(ref v) => {
                match v {
                    Array::Int(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Float(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Text(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Json(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Bool(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Tinyint(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Smallint(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Bigint(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::BigDecimal(vv) =>  Ok(vv.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")),
                    Array::Date(vv) =>  Ok(vv.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")),
                    Array::Timestamp(vv) =>  Ok(vv.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")),
                    Array::Uuid(vv) =>  Ok(vv.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")),
                    Array::Double(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                    Array::Char(vv) =>  Ok(serde_json::to_string(vv).unwrap_or_default()),
                }
            }
            Value::Object(ref obj) => {
                let data: IndexMap<String, Value> = obj.to_owned();
                if data.len() > 0 {
                    let (_k, v) = data.get_index(0).unwrap();
                    Ok(v.as_str().to_owned().unwrap_or_default().to_string())
                } else {
                    Ok(String::default())
                }
            }
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "String".to_string(),
            ))),

        }
    }
}

impl FromValue for Vec<String> {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Array(Array::Text(ref t)) => Ok(t.to_owned()),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "Vec<String>".to_string(),
            ))),
        }
    }
}

impl FromValue for () {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Nil => Ok(()),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "Vec<String>".to_string(),
            ))),
        }
    }
}

impl FromValue for bool {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Bool(v) => Ok(v),
            Value::Tinyint(v) => Ok(v == 1),
            Value::Smallint(v) => Ok(v == 1),
            Value::Int(v) => Ok(v == 1),
            Value::Bigint(v) => Ok(v == 1),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "bool".to_string(),
            ))),
        }
    }
}

impl FromValue for serde_json::Value {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match v.clone() {
            Value::Bool(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Nil => Ok(serde_json::Value::Null),
            Value::Tinyint(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Smallint(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Int(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Bigint(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Float(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Double(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Blob(v) => serde_json::from_slice(&v).map_err(AkitaDataError::from),
            Value::Char(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Text(v) => serde_json::from_str(&v).map_err(AkitaDataError::from),
            Value::Json(v) => Ok(v.clone()), //serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Uuid(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Date(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Time(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::DateTime(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            Value::Timestamp(v) => serde_json::to_value(v).map_err(AkitaDataError::from),
            // Value::SerdeJson(v) => Ok(v.clone()),
            Value::Object(v) => {
                let mut data = Map::new();
                for (k, v) in v.into_iter() {
                    data.insert(k, serde_json::Value::from_value(&v));
                }
                Ok(serde_json::Value::Object(data))
            },
            Value::Array(v) => serde_json::to_value(v).map_err(|err| AkitaDataError::from(err)),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "SerdeJson".to_string(),
            ))),
        }
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Text(ref v) => Ok(DateTime::<Utc>::from_utc(parse_naive_date_time(v), Utc)),
            Value::DateTime(v) => Ok(DateTime::<Utc>::from_utc(v, Utc)),
            Value::Timestamp(v) => Ok(v),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "DateTime".to_string(),
            ))),
        }
    }
}

impl FromValue for NaiveDateTime {
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Text(ref v) => Ok(parse_naive_date_time(v)),
            Value::DateTime(v) => Ok(v),
            _ => Err(AkitaDataError::ConvertError(ConvertError::NotSupported(
                format!("{:?}", v),
                "NaiveDateTime".to_string(),
            ))),
        }
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Nil => Ok(None),
            _ => FromValue::from_value_opt(v).map(Some),
        }
    }
}

impl<T> FromValue for &T
where
    T: FromValue,
{
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        match *v {
            Value::Nil => Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", v))),
            _ => FromValue::from_value_opt(v),
        }
        
    }
}

impl FromValue for Value
{
    fn from_value_opt(v: &Value) -> Result<Self, AkitaDataError> {
        Ok(v.to_owned())
    }
}

fn parse_naive_date_time(v: &str) -> NaiveDateTime {
    let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S");
    if let Ok(ts) = ts {
        ts
    } else {
        let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.3f");
        if let Ok(ts) = ts {
            ts
        } else {
            panic!("unable to parse timestamp: {}", v);
        }
    }
}


macro_rules! take_or_place {
    ($row:expr, $index:expr, $t:ident) => (
        match $row.take_obj_raw($index) {
            Some(v) => v,
            None => return Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", $row))),
        }
    );
    ($row:expr, $index:expr, $t:ident, $( [$idx:expr, $ir:expr] ),*) => (
        match $row.take_obj_raw($index) {
            Some(v) => v,
            None => return Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", $row))),
        }
    );
}


impl <T1> FromValue for (T1,)
where  T1: FromValue {
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        T1::from_value_opt(data).map(|t| (t,))
    }
}

impl<T1, T2> FromValue for (T1, T2)
where
    T1: FromValue,
    T2: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 2 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        Ok((ir1, ir2))
    }
}

impl<T1, T2, T3> FromValue for (T1, T2, T3)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 3 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        Ok((ir1, ir2, ir3))
    }
}

impl<T1, T2, T3, T4> FromValue for (T1, T2, T3, T4)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 4 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        Ok((ir1, ir2, ir3, ir4))
    }
}

impl<T1, T2, T3, T4, T5> FromValue for (T1, T2, T3, T4, T5)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 5 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        Ok((ir1, ir2, ir3, ir4, ir5))
    }
}


impl<T1, T2, T3, T4, T5, T6> FromValue for (T1, T2, T3, T4, T5, T6)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 6 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6))
    }
}


impl<T1, T2, T3, T4, T5, T6, T7> FromValue for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() != 7 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7))
    }
}




impl<T1, T2, T3, T4, T5, T6, T7, T8> FromValue for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() !=8 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        let ir8 = take_or_place!(data, 7, T8, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8))
    }
}


impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> FromValue for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() !=9 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        let ir8 = take_or_place!(data, 7, T8, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
        let ir9 = take_or_place!(data, 8, T9, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9))
    }
}



impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> FromValue for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() !=10 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        let ir8 = take_or_place!(data, 7, T8, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
        let ir9 = take_or_place!(data, 8, T9, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
        let ir10 = take_or_place!(data, 9, T10, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10))
    }
}




impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> FromValue for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
    T11: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() !=11 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        let ir8 = take_or_place!(data, 7, T8, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
        let ir9 = take_or_place!(data, 8, T9, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
        let ir10 = take_or_place!(data, 9, T10, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
        let ir11 = take_or_place!(data, 10, T11, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11))
    }
}


impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> FromValue for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
    T11: FromValue,
    T12: FromValue,
{
    fn from_value_opt(data: &Value) -> Result<Self, AkitaDataError> {
        if data.get_obj_len() !=12 {
            return Err(AkitaDataError::NoSuchValueError(format!("Can not convert row with {:?}", data)))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(data, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(data, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(data, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(data, 6, T7, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
        let ir8 = take_or_place!(data, 7, T8, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
        let ir9 = take_or_place!(data, 8, T9, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
        let ir10 = take_or_place!(data, 9, T10, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
        let ir11 = take_or_place!(data, 10, T11, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
        let ir12 = take_or_place!(data, 11, T12, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
        Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12))
    }
}

impl <V> ToValue for IndexMap<String, V> where V: ToValue {
    fn to_value(&self) -> Value {
        let mut map: IndexMap<String, Value> = IndexMap::new();
        for key in self.keys() {
            if let Some(v) = self.get(key) {
                map.insert(key.to_string(), V::to_value(v));
            }
        }
        Value::Object(map)

    }
}

/// Will panic if could not convert `v` to `T`
#[inline]
pub fn from_value<T: FromValue>(v: Value) -> T {
    FromValue::from_value(&v)
}

/// Will return `Err(FromValueError(v))` if could not convert `v` to `T`
#[inline]
pub fn from_value_opt<T: FromValue>(v: Value) -> Result<T, AkitaDataError> {
    FromValue::from_value_opt(&v)
}