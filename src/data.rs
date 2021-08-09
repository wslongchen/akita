use std::{collections::BTreeMap, slice};

use crate::{value::{ConvertError, FromValue, ToValue, Value}};


#[derive(Debug, PartialEq, Clone, Default)]
pub struct AkitaData(pub BTreeMap<String, Value>);

pub trait FromAkita {
    /// convert akita to an instance of the corresponding struct of the model
    /// taking into considerating the renamed columns
    fn from_data(data: &AkitaData) -> Self;
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
            Some(v) => FromValue::from_value(v).map_err(AkitaDataError::ConvertError),
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
                            FromValue::from_value(v).map_err(AkitaDataError::ConvertError)?,
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub fn get_value(&self, s: &str) -> Option<&Value> { self.0.get(s) }

    pub fn remove(&mut self, s: &str) -> Option<Value> { self.0.remove(s) }
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

    pub fn push(&mut self, row: Vec<Value>) { self.data.push(row) }

    /// Returns an iterator over the `Row`s.
    pub fn iter(&self) -> Iter {
        Iter {
            columns: self.columns.clone(),
            iter: self.data.iter(),
        }
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
                let mut dao = AkitaData::new();
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

impl ToAkita for u8 {
    fn to_data(&self) -> AkitaData {
        let mut data = AkitaData::new();
        data.insert("0", *self);
        data
    }
}
impl FromAkita for u8 {
    fn from_data(data: &AkitaData) -> Self {
        let value = data.get("0").unwrap_or(0);
        value
    }
}