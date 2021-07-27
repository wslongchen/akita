// pub enum Value {
//     Float32(f32),
//     Float64(f64),
//     Text(String),
//     Int8(i8),
//     Int16(i16),
//     Int32(i32),
//     Int64(i64),
//     Int128(i128),
//     Isize(isize),
//     Usize(usize),
//     U32(u32),
//     U8(u8),
//     U16(u16),
//     U64(u64),
//     U128(u128),
//     Str(&'static str),
//     Nil,
// }

use std::fmt;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

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
    // BigDecimal(BigDecimal),

    Blob(Vec<u8>),
    Char(char),
    Text(String),
    Json(String),

    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Interval(Interval),

    // Point(Point<f64>),

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
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Nil => write!(f, ""),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Tinyint(v) => write!(f, "{}", v),
            Value::Smallint(v) => write!(f, "{}", v),
            Value::Int(v) => write!(f, "{}", v),
            Value::Bigint(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            // Value::BigDecimal(v) => write!(f, "{}", v),
            Value::Char(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", v),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S").to_string()),
            Value::Timestamp(v) => write!(f, "{}", v.to_rfc3339()),
            Value::Array(array) => array.fmt(f),
            Value::Blob(v) => {
                let encoded = base64::encode_config(&v, base64::MIME);
                write!(f, "{}", encoded)
            }
            _ => panic!("not yet implemented: {:?}", self),
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum Array {
    /*
    Bool(Vec<bool>),

    Tinyint(Vec<i8>),
    Smallint(Vec<i16>),
    */
    Int(Vec<i32>),
    Float(Vec<f32>),
    /*
    Bigint(Vec<i64>),

    Double(Vec<f64>),
    BigDecimal(Vec<BigDecimal>),
    */
    Text(Vec<String>),
    /*
    Char(Vec<char>),
    Uuid(Vec<Uuid>),
    Date(Vec<NaiveDate>),
    Timestamp(Vec<DateTime<Utc>>),
    */
}

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Array::Text(texts) => {
                let json_arr = "";//serde_json::to_string(texts).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Float(floats) => {
                let json_arr = "";//serde_json::to_string(floats).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            _ => panic!("not yet implemented: {:?}", self),
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

impl ToValue for Vec<String> {
    fn to_value(&self) -> Value {
        Value::Array(Array::Text(self.to_owned()))
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

impl<T> ToValue for &T
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        (*self).to_value()
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

#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self, ConvertError>;
}

macro_rules! impl_from_value {
    ($ty: ty, $ty_name: tt, $($variant: ident),*) => {
        /// try from to owned
        impl FromValue for $ty {
            fn from_value(v: &Value) -> Result<Self, ConvertError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    _ => Err(ConvertError::NotSupported(format!("{:?}",v), $ty_name.into())),
                }
            }
        }
    }
}

macro_rules! impl_from_value_numeric {
    ($ty: ty, $method:ident, $ty_name: tt, $($variant: ident),*) => {
        impl FromValue for $ty {
            fn from_value(v: &Value) -> Result<Self, ConvertError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    // Value::BigDecimal(ref v) => Ok(v.$method().unwrap()),
                    _ => Err(ConvertError::NotSupported(format!("{:?}", v), $ty_name.into())),
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
impl_from_value_numeric!(i16, to_i16, "i16", Tinyint, Smallint);
impl_from_value_numeric!(i32, to_i32, "i32", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(i64, to_i64, "i64", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(f32, to_f32, "f32", Float);
impl_from_value_numeric!(f64, to_f64, "f64", Float, Double);

/// Char can be casted into String
/// and they havea separate implementation for extracting data
impl FromValue for String {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(v.to_owned()),
            Value::Char(ref v) => {
                let mut s = String::new();
                s.push(*v);
                Ok(s)
            }
            Value::Blob(ref v) => String::from_utf8(v.to_owned()).map_err(|e| {
                ConvertError::NotSupported(format!("{:?}", v), format!("String: {}", e))
            }),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "String".to_string(),
            )),
        }
    }
}

impl FromValue for Vec<String> {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Array(Array::Text(ref t)) => Ok(t.to_owned()),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "Vec<String>".to_string(),
            )),
        }
    }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Bool(v) => Ok(v),
            Value::Tinyint(v) => Ok(v == 1),
            Value::Smallint(v) => Ok(v == 1),
            Value::Int(v) => Ok(v == 1),
            Value::Bigint(v) => Ok(v == 1),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "bool".to_string(),
            )),
        }
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(DateTime::<Utc>::from_utc(parse_naive_date_time(v), Utc)),
            Value::DateTime(v) => Ok(DateTime::<Utc>::from_utc(v, Utc)),
            Value::Timestamp(v) => Ok(v),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "DateTime".to_string(),
            )),
        }
    }
}

impl FromValue for NaiveDateTime {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(parse_naive_date_time(v)),
            Value::DateTime(v) => Ok(v),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "NaiveDateTime".to_string(),
            )),
        }
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Nil => Ok(None),
            _ => FromValue::from_value(v).map(Some),
        }
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

// impl Into<Value> for i16 {
//     fn into(self) -> Value {
//         Value::Int16(self)
//     }
// }

// impl Into<Value> for i32 {
//     fn into(self) -> Value {
//         Value::Int32(self)
//     }
// }

// impl Into<Value> for i64 {
//     fn into(self) -> Value {
//         Value::Int64(self)
//     }
// }

// impl Into<Value> for i128 {
//     fn into(self) -> Value {
//         Value::Int128(self)
//     }
// }

// impl Into<Value> for u128 {
//     fn into(self) -> Value {
//         Value::U128(self)
//     }
// }

// impl Into<Value> for u64 {
//     fn into(self) -> Value {
//         Value::U64(self)
//     }
// }


// impl Into<Value> for u32 {
//     fn into(self) -> Value {
//         Value::U32(self)
//     }
// }

// impl Into<Value> for u16 {
//     fn into(self) -> Value {
//         Value::U16(self)
//     }
// }


// impl Into<Value> for String {
//     fn into(self) -> Value {
//         Value::Text(self)
//     }
// }


// impl Into<Value> for usize {
//     fn into(self) -> Value {
//         Value::Usize(self)
//     }
// }

// impl Into<Value> for isize {
//     fn into(self) -> Value {
//         Value::Isize(self)
//     }
// }

// impl Into<Value> for f64 {
//     fn into(self) -> Value {
//         Value::Float64(self)
//     }
// }

// impl Into<Value> for f32 {
//     fn into(self) -> Value {
//         Value::Float32(self)
//     }
// }

// impl Into<Value> for &'static str {
//     fn into(self) -> Value {
//         Value::Str(self)
//     }
// }