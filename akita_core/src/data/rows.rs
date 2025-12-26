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
use std::fmt::Formatter;
use std::slice;
use crate::{AkitaDataError, Row, AkitaValue};

/// use this to store data retrieved from the database
#[derive(Debug, PartialEq, Clone)]
pub struct Rows {
    pub data: Vec<Row>,
    /// can be optionally set, indicates how many total rows are there in the table
    pub count: Option<usize>,
}

impl Default for Rows {
    fn default() -> Self {
        Self {
            data: vec![],
            count: None,
        }
    }
}

impl std::fmt::Display for Rows {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        write!(f, "<==    Columns: {}\n", self.data.iter().next().map(|v| v.columns.join(", ")).unwrap_or("[]".to_string()))?;
        for data in self.data.iter() {
            write!(f, "<==        Row: {}\n", data.data.iter().map(|v| format!("{}",v)).collect::<Vec<String>>().join(", "))?;
        }
        write!(f, "<==      Total: {}", self.count.unwrap_or(self.data.len()))
    }
}

impl Rows {
    pub fn empty() -> Self { Rows::new() }

    pub fn new() -> Self {
        Rows {
            data: vec![],
            count: None,
        }
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn push(&mut self, row: Row) { self.data.push(row) }

    /// Returns an iterator over the `Row`s.
    pub fn iter(&self) -> RowsIter<'_> {
        RowsIter {
            inner: self.data.iter(),
        }
    }

    pub fn iter_mut(&mut self) -> RowsIterMut<'_> {
        RowsIterMut {
            inner: self.data.iter_mut(),
        }
    }

    pub fn first(&self) -> Option<&Row> {
        self.data.first()
    }

    pub fn first_as_object(&self) -> Option<AkitaValue> {
        self.first().map(|row| row.as_object())
    }

    pub fn last(&self) -> Option<&Row> {
        self.data.last()
    }

    pub fn get(&self, index: usize) -> Option<&Row> {
        self.data.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Row> {
        self.data.get_mut(index)
    }

    pub fn to_objects(&self) -> Vec<AkitaValue> {
        self.iter().map(|row| row.as_object()).collect()
    }

    pub fn set_count(&mut self, count: usize) -> &mut Self {
        self.count = Some(count);
        self
    }

    pub fn into_inner(self) -> Vec<Row> {
        self.data
    }

    pub fn into_objects(self) -> Vec<AkitaValue> {
        self.data.into_iter().map(|row| row.into_object()).collect()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn object_iter(&self) -> ObjectIter<'_> {
        ObjectIter {
            inner: self.data.iter(),
        }
    }

    pub fn to_json(&self) -> Result<String, AkitaDataError> {
        let objects: Vec<serde_json::Value> = self
            .iter()
            .map(|row| {
                let mut map = serde_json::Map::new();
                for (column, value) in row.iter() {
                    map.insert(column.clone(), serde_json::Value::String(format!("{}", value)));
                }
                serde_json::Value::Object(map)
            })
            .collect();
        Ok(serde_json::to_string(&objects).map_err(|err|AkitaDataError::ObjectValidError(err.to_string()))?)
    }
}



/// An iterator over `Row`s.
pub struct RowsIter<'a> {
    inner: slice::Iter<'a, Row>,
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}


impl<'a> ExactSizeIterator for RowsIter<'a> {}

pub struct RowsIterMut<'a> {
    inner: slice::IterMut<'a, Row>,
}

impl<'a> Iterator for RowsIterMut<'a> {
    type Item = &'a mut Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> ExactSizeIterator for RowsIterMut<'a> {}


pub struct ObjectIter<'a> {
    inner: slice::Iter<'a, Row>,
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = AkitaValue;

    fn next(&mut self) -> Option<AkitaValue> {
        self.inner.next().map(|row| row.as_object())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> ExactSizeIterator for ObjectIter<'a> {}


// Implement IntoIterator for Rows
impl<'a> IntoIterator for &'a Rows {
    type Item = &'a Row;
    type IntoIter = RowsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}