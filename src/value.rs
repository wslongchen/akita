use std::fmt;
use bigdecimal::{BigDecimal, ToPrimitive};
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
    BigDecimal(BigDecimal),

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
    SerdeJson(serde_json::Value),
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
            Value::BigDecimal(v) => write!(f, "{}", v),
            Value::Char(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", v),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::SerdeJson(v) => write!(f, "{}", serde_json::to_string(v).unwrap_or_default()),
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
            Array::Text(_texts) => {
                let json_arr = "";//serde_json::to_string(texts).expect("must serialize");
                write!(f, "{}", json_arr)
            }
            Array::Float(_floats) => {
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


impl<'a, T> ToValue for &'a T
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        (*self).to_value()
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


#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

impl From<serde_json::Error> for ConvertError {
    fn from(err: serde_json::Error) -> Self {
        ConvertError::NotSupported(err.to_string(), "SerdeJson".to_string())
    }
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
                    Value::BigDecimal(ref v) => Ok(v.$method().unwrap()),
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

impl FromValue for serde_json::Value {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match v.clone() {
            Value::Bool(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Tinyint(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Smallint(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Int(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Bigint(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Float(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Double(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Blob(v) => serde_json::to_value(String::from_utf8_lossy(&v)).map_err(ConvertError::from),
            Value::Char(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Text(v) => serde_json::from_str(&v).map_err(ConvertError::from),
            Value::Json(v) => serde_json::from_str(&v).map_err(ConvertError::from),
            Value::Uuid(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Date(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Time(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::DateTime(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::Timestamp(v) => serde_json::to_value(v).map_err(ConvertError::from),
            Value::SerdeJson(v) => Ok(v.clone()),
            // Value::Array(v) => serde_json::to_value(v).map_err(|err| ConvertError::from(err)),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", v),
                "SerdeJson".to_string(),
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


#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    Nil, // no params
    Vector(Vec<Value>), // vec
    Custom(Vec<(String, Value)>), // custom params
}
// pub trait ToParam {
//     fn to_param(&self) -> Params;
// }

impl From<Vec<Value>> for Params {
    fn from(x: Vec<Value>) -> Params {
        Params::Vector(x)
    }
}

impl<'a> From<&'a [&'a dyn ToValue]> for Params {
    fn from(x: &'a [&'a dyn ToValue]) -> Params {
        let values = x.iter().map(|p| p.to_value()).collect::<Vec<Value>>();
        Params::Vector(values)
    }
}

impl<N, V> From<Vec<(N, V)>> for Params
where
    String: From<N>,
    Value: From<V>,
{
    fn from(x: Vec<(N, V)>) -> Params {
        let mut params = Vec::new();
        for (name, value) in x.into_iter() {
            let name = String::from(name);
            params.push((name, Value::from(value)));
        }
        Params::Custom(params)
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::Nil
    }
}

macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Into<Value>,)*> From<($($A,)*)> for Params {
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                let mut params = Vec::new();
                $(params.push($a.into());)*
                Params::Vector(params)
            }
        }
    );
}

impl<'a, T: Into<Params> + Clone> From<&'a T> for Params {
    fn from(x: &'a T) -> Params {
        x.clone().into()
    }
}

into_params_impl!([A, a]);
into_params_impl!([A, a], [B, b]);
into_params_impl!([A, a], [B, b], [C, c]);
into_params_impl!([A, a], [B, b], [C, c], [D, d]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y], [Z, z]);
