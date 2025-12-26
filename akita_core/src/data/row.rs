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
use std::ops::Index;
use crate::{AkitaDataError, ColumnIndex, AkitaValue, FromAkitaValue};

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    pub columns: Vec<String>,
    pub data: Vec<AkitaValue>,
}



impl Row {

    pub fn new(columns: Vec<String>, data: Vec<AkitaValue>) -> Self {
        Self { columns, data }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns columns of this row.
    pub fn columns_ref(&self) -> &[String] {
        &self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&AkitaValue> {
        self.data.get(index)
    }

    pub fn iter(&self) -> RowIter<'_> {
        RowIter {
            columns: &self.columns,
            data: &self.data,
            index: 0,
        }
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` earlier,
    /// then will convert it to `T`.
    pub fn get<T, I>(&self, index: I) -> Option<T>
    where
        T: FromAkitaValue,
        I: ColumnIndex,
    {
        index.idx(&*self.columns).and_then(|idx| {
            self.data
                .get(idx)
                .map(|x| T::from_value(x))
        })
    }

    pub fn get_by_column<T>(&self, column: &str) -> Option<T>
    where
        T: FromAkitaValue,
    {
        self.get_value_by_column(column)
            .map(|value| T::from_value(value))
    }
    
    pub fn get_by_column_opt<T>(&self, column: &str) -> Option<Result<T, AkitaDataError>>
    where
        T: FromAkitaValue,
    {
        self.get_value_by_column(column)
            .map(|value| T::from_value_opt(value))
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` or `Row::take_opt`
    /// earlier, then will attempt convert it to `T`. Unlike `Row::get`, `Row::get_opt` will
    /// allow you to directly handle errors if the value could not be converted to `T`.
    pub fn get_opt<T, I>(&self, index: I) -> Option<Result<T, AkitaDataError>>
    where
        T: FromAkitaValue,
        I: ColumnIndex,
    {
        index
            .idx(&*self.columns)
            .and_then(|idx| self.data.get(idx))
            .map(|x| T::from_value_opt(x))
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take<T, I>(&mut self, index: I) -> Option<T>
    where
        T: FromAkitaValue,
        I: ColumnIndex,
    {
        index.idx(&*self.columns).and_then(|idx| {
            self.data
                .get_mut(idx)
                .map(|x| x.take()).as_ref()
                .map(T::from_value)
        })
    }

    pub fn take_by_column<T>(&mut self, column: &str) -> Option<T>
    where
        T: FromAkitaValue,
    {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|idx| self.data.get_mut(idx))
            .map(|value| value.take()).as_ref()
            .map(T::from_value)
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_opt<T, I>(&mut self, index: I) -> Option<Result<T, AkitaDataError>>
    where
        T: FromAkitaValue,
        I: ColumnIndex,
    {
        index
            .idx(&*self.columns)
            .and_then(|idx| self.data.get_mut(idx))
            .map(|x| x.take()).as_ref()
            .map(T::from_value_opt)
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<AkitaValue> {
        self.data
            .into_iter()
            .collect()
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: AkitaValue) {
        self.data[index] = value;
    }

    pub fn get_value(&self, index: usize) -> Option<&AkitaValue> {
        self.data.get(index)
    }

    pub fn get_value_by_column(&self, column: &str) -> Option<&AkitaValue> {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|idx| self.data.get(idx))
    }

    pub fn as_object(&self) -> AkitaValue {
        let mut object = AkitaValue::new_object();
        for (column, value) in self.iter() {
            object.insert_obj_value(column, value);
        }
        object
    }

    pub fn into_object(self) -> AkitaValue {
        let mut object = AkitaValue::new_object();
        for (column, value) in self.columns.into_iter().zip(self.data.into_iter()) {
            object.insert_obj_value(&column, &value);
        }
        object
    }

    pub fn into_data(self) -> Vec<AkitaValue> {
        self.data
    }

    pub fn set_value(&mut self, index: usize, value: AkitaValue) -> Result<(), AkitaDataError> {
        if index < self.data.len() {
            self.data[index] = value;
            Ok(())
        } else {
            Err(AkitaDataError::IndexOutOfBounds(index, self.data.len()))
        }
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c == column)
    }
}



impl Index<usize> for Row {
    type Output = AkitaValue;

    fn index(&self, index: usize) -> &AkitaValue {
        &self.data[index]
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = AkitaValue;

    fn index(&self, column: &'a str) -> &AkitaValue {
        for (i, col) in self.columns.iter().enumerate() {
            if col == column {
                return &self.data[i];
            }
        }
        panic!("No such column: `{}` in row with columns: {:?}", column, self.columns);
    }
}


/// Iterator of the line
pub struct RowIter<'a> {
    columns: &'a [String],
    data: &'a [AkitaValue],
    index: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = (&'a String, &'a AkitaValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.columns.len() && self.index < self.data.len() {
            let column = &self.columns[self.index];
            let value = &self.data[self.index];
            self.index += 1;
            Some((column, value))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.columns.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for RowIter<'a> {}