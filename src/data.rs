use std::{any::type_name, collections::{BTreeMap, btree_map::{Keys, Values}}, slice};

use crate::{AkitaError, value::{ConvertError, FromValue, ToValue, Value}};


#[derive(Debug, PartialEq, Clone, Default)]
pub struct AkitaData(BTreeMap<String, Value>, Vec<Value>);

pub trait FromAkita : Sized {
    /// convert akita to an instance of the corresponding struct of the model
    /// taking into considerating the renamed columns
    fn from_data(data: &AkitaData) -> Self {
        match Self::from_data_opt(data) {
            Ok(v) => v,
            Err(_err) => panic!(
                "Couldn't from_data {:?} to type {}. (see FromRow documentation)",
                data,
                type_name::<Self>(),
            ),
        }
    }

    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError>;
}

pub trait ToAkita {
    /// convert from an instance of the struct to a akita representation
    /// to be saved into the database
    fn to_data(&self) -> AkitaData;
}

#[derive(Debug)]
pub enum AkitaDataError {
    ConvertError(ConvertError),
    NoSuchValueError(String),
}

impl AkitaData {
    pub fn new() -> Self { AkitaData::default() }

    pub fn from_raw(row: &Vec<Value>) -> Self { AkitaData(BTreeMap::new(), row.to_owned()) }

    pub fn insert<K, V>(&mut self, k: K, v: V)
    where
        K: ToString,
        V: ToValue,
    {
        self.0.insert(k.to_string(), v.to_value());
    }

    pub fn insert_value<K>(&mut self, k: K, value: &Value)
    where
        K: ToString,
    {
        self.0.insert(k.to_string(), value.clone());
    }

    pub fn get<'a, T>(&'a self, s: &str) -> Result<T, AkitaDataError>
    where
        T: FromValue,
    {
        let value: Option<&'a Value> = self.0.get(s);
        match value {
            Some(v) => FromValue::from_value_opt(v).map_err(AkitaDataError::ConvertError),
            None => Err(AkitaDataError::NoSuchValueError(s.into())),
        }
    }

    pub fn get_opt<'a, T>(&'a self, s: &str) -> Result<Option<T>, AkitaDataError>
    where
        T: FromValue,
    {
        let value: Option<&'a Value> = self.0.get(s);
        match value {
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
        }
    }

    pub fn get_value(&self, s: &str) -> Option<&Value> { self.0.get(s) }

    pub fn values<'a>(&'a self) -> Values<'a, String, Value> { self.0.values() }

    pub fn keys<'a>(&'a self) -> Keys<'a, String, Value> { self.0.keys() }

    pub fn remove(&mut self, s: &str) -> Option<Value> { self.0.remove(s) }


    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take_raw<'a, T>(&'a self, index: usize) -> Option<T>
    where
        T: FromValue,
    {
        self.1.get(index).and_then(|v| Some(FromValue::from_value(v)))
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_raw_opt<'a, T>(&'a self, index: usize) -> Option<Result<T, AkitaError>>
    where
        T: FromValue,
    {
        self.1.get(index).and_then(|v| Some(FromValue::from_value_opt(v).map_err(AkitaError::from)))
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<Value> {
        self.1
            .into_iter()
            .collect()
    }

    /// Unwraps values as is (taken cells will be `None`).
    #[doc(hidden)]
    pub fn unwrap_raw(self) -> Vec<Value> {
        self.1
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        self.1[index] = value;
    }
}



/// use this to store data retrieved from the database
/// This is also slimmer than Vec<Dao> when serialized
#[derive(Debug, PartialEq, Clone)]
pub struct Rows {
    pub columns: Vec<String>,
    pub data: Vec<Vec<Value>>,
    /// can be optionally set, indicates how many total rows are there in the table
    pub count: Option<usize>,
}

impl Rows {
    pub fn empty() -> Self { Rows::new(vec![]) }

    pub fn new(columns: Vec<String>) -> Self {
        Rows {
            columns,
            data: vec![],
            count: None,
        }
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn push(&mut self, row: Vec<Value>) { self.data.push(row) }

    /// Returns an iterator over the `Row`s.
    pub fn iter(&self) -> Iter {
        Iter {
            columns: self.columns.clone(),
            iter: self.data.iter(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

/// An iterator over `Row`s.
pub struct Iter<'a> {
    columns: Vec<String>,
    iter: slice::Iter<'a, Vec<Value>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = AkitaData;

    fn next(&mut self) -> Option<AkitaData> {
        let next_row = self.iter.next();
        if let Some(row) = next_row {
            if !row.is_empty() {
                let mut dao = AkitaData::from_raw(row);
                for (i, column) in self.columns.iter().enumerate() {
                    if let Some(value) = row.get(i) {
                        dao.insert_value(column, value);
                    }
                }
                Some(dao)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) { self.iter.size_hint() }
}

impl<'a> ExactSizeIterator for Iter<'a> {}


macro_rules! impl_to_segment {
    ($ty:ty) => {
        impl ToAkita for $ty {
            fn to_data(&self) -> AkitaData {
                let mut data = AkitaData::new();
                data.insert("0", self);
                data
            }
        }

        // impl FromAkita for $ty {
        //     fn from_data(data: &AkitaData) -> Self {
        //         if let Some(value) = data.0.values().next() {
        //             FromValue::from_value(value).unwrap_or_default()
        //         } else {
        //             FromValue::from_value(&Value::Nil).unwrap_or_default()
        //         }
        //     }
        // }

    };
}

macro_rules! take_or_place {
    ($row:expr, $index:expr, $t:ident) => (
        match $row.take_raw($index) {
            Some(v) => v,
            None => return Err(ConvertError::FromAkitaError($row.to_owned())),
        }
    );
    ($row:expr, $index:expr, $t:ident, $( [$idx:expr, $ir:expr] ),*) => (
        match $row.take_raw($index) {
            Some(v) => v,
            None => return Err(ConvertError::FromAkitaError($row.to_owned())),
        }
    );
}




impl <T> FromAkita for T
where  T: FromValue 
{
    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError> {
        if data.keys().len() == 1 {
            Ok(take_or_place!(data, 0, T))
        } else {
            Err(ConvertError::FromAkitaError(data.to_owned()))
        }
    }
}

impl FromAkita for AkitaData {
    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError> {
        Ok(data.to_owned())
    }
}


impl <T1> FromAkita for (T1,)
where  T1: FromValue {
    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError> {
        T1::from_data_opt(data).map(|t| (t,))
    }
}

impl<T1, T2> FromAkita for (T1, T2)
where
    T1: FromValue,
    T2: FromValue,
{
    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError> {
        if data.keys().len() != 2 {
            return Err(ConvertError::FromAkitaError(data.to_owned()))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        Ok((ir1, ir2))
    }
}

impl<T1, T2, T3> FromAkita for (T1, T2, T3)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
{
    fn from_data_opt(data: &AkitaData) -> Result<Self, ConvertError> {
        if data.keys().len() != 2 {
            return Err(ConvertError::FromAkitaError(data.to_owned()))
        }
        let ir1 = take_or_place!(data, 0, T1);
        let ir2 = take_or_place!(data, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(data, 2, T3, [0, ir1], [1, ir2]);
        Ok((ir1, ir2, ir3))
    }
}

impl <V> ToAkita for BTreeMap<String, V> where V: ToValue {
    fn to_data(&self) -> AkitaData {
        let values = self.values().into_iter().map(|v| V::to_value(v)).collect::<Vec<Value>>();
        let mut bt_map: BTreeMap<String, Value> = BTreeMap::new();
        for key in self.keys() {
            if let Some(v) = self.get(key) {
                bt_map.insert(key.to_string(), V::to_value(v));
            }
        }
        AkitaData(bt_map,values)

    }
}

impl ToAkita for serde_json::Value {
    fn to_data(&self) -> AkitaData {
        if self.is_object() {
            if let Some(data) = self.as_object() {
                let values = data.values().into_iter().map(|v| serde_json::Value::to_value(v)).collect::<Vec<Value>>();
                let mut bt_map: BTreeMap<String, Value> = BTreeMap::new();
                for key in data.keys() {
                    if let Some(v) = self.get(key) {
                        bt_map.insert(key.to_string(), serde_json::Value::to_value(v));
                    }
                }
                AkitaData(bt_map,values)
            } else {
                AkitaData::new()
            }
        } else {
            AkitaData::new()
        }
        
    }
}
impl_to_segment!(i8);
impl_to_segment!(bool);
impl_to_segment!(isize);
impl_to_segment!(i16);
impl_to_segment!(i32);
impl_to_segment!(i64);
impl_to_segment!(u8);
impl_to_segment!(u16);
impl_to_segment!(u32);
impl_to_segment!(u64);
impl_to_segment!(usize);