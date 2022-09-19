use std::fmt::Formatter;
use std::slice;
use std::ops::Index;
use crate::{AkitaDataError, from_value, from_value_opt, FromValue};
use crate::value::Value;

/// use this to store data retrieved from the database
/// This is also slimmer than Vec<Dao> when serialized
#[derive(Debug, PartialEq, Clone)]
pub struct Rows {
    pub data: Vec<Row>,
    /// can be optionally set, indicates how many total rows are there in the table
    pub count: Option<usize>,
}

impl std::fmt::Display for Rows {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "data: {:?}, count: {}", self.data, self.count.unwrap_or_default())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    pub columns: Vec<String>,
    pub data: Vec<Value>,
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
    pub fn iter(&self) -> Iter {
        Iter {
            iter: self.data.iter(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

/// An iterator over `Row`s.
pub struct Iter<'a> {
    iter: slice::Iter<'a, Row>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        let next_row = self.iter.next();
        if let Some(row) = next_row {
            if !row.data.is_empty() {
                let mut v = Value::new_object();
                for (i, column) in row.columns.iter().enumerate() {
                    if let Some(value) = row.data.get(i) {
                        v.insert_obj_value(column, value);
                    }
                }

                Some(v)
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



impl Row {
    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns columns of this row.
    pub fn columns_ref(&self) -> &[String] {
        &*self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&Value> {
        self.data.get(index)
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` earlier,
    /// then will convert it to `T`.
    pub fn get<T, I>(&self, index: I) -> Option<T>
        where
            T: FromValue,
            I: ColumnIndex,
    {
        index.idx(&*self.columns).and_then(|idx| {
            self.data
                .get(idx)
                .map(|x| from_value::<T>(x.clone()))
        })
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` or `Row::take_opt`
    /// earlier, then will attempt convert it to `T`. Unlike `Row::get`, `Row::get_opt` will
    /// allow you to directly handle errors if the value could not be converted to `T`.
    pub fn get_opt<T, I>(&self, index: I) -> Option<Result<T, AkitaDataError>>
        where
            T: FromValue,
            I: ColumnIndex,
    {
        index
            .idx(&*self.columns)
            .and_then(|idx| self.data.get(idx))
            .map(|x| from_value_opt::<T>(x.clone()))
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take<T, I>(&mut self, index: I) -> Option<T>
        where
            T: FromValue,
            I: ColumnIndex,
    {
        index.idx(&*self.columns).and_then(|idx| {
            self.data
                .get_mut(idx)
                .map(|x| x.take())
                .map(from_value::<T>)
        })
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_opt<T, I>(&mut self, index: I) -> Option<Result<T, AkitaDataError>>
        where
            T: FromValue,
            I: ColumnIndex,
    {
        index
            .idx(&*self.columns)
            .and_then(|idx| self.data.get_mut(idx))
            .map(|x| x.take())
            .map(from_value_opt::<T>)
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<Value> {
        self.data
            .into_iter()
            .collect()
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        self.data[index] = value;
    }
}


impl Index<usize> for Row {
    type Output = Value;

    fn index(&self, index: usize) -> &Value {
        &self.data[index]
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = Value;

    fn index<'r>(&'r self, index: &'a str) -> &'r Value {
        for (i, column) in self.columns.iter().enumerate() {
            if column.as_bytes() == index.as_bytes() {
                return &self.data[i];
            }
        }
        panic!("No such column: `{}` in row {:?}", index, self);
    }
}

/// Things that may be used as an index of a row column.
pub trait ColumnIndex {
    fn idx(&self, columns: &[String]) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn idx(&self, columns: &[String]) -> Option<usize> {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> ColumnIndex for &'a str {
    fn idx(&self, columns: &[String]) -> Option<usize> {
        for (i, c) in columns.iter().enumerate() {
            if c.as_bytes() == self.as_bytes() {
                return Some(i);
            }
        }
        None
    }
}
