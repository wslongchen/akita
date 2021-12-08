use std::slice;

use crate::value::Value;

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
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        let next_row = self.iter.next();
        if let Some(row) = next_row {
            if !row.is_empty() {
                let mut v = Value::new_object();
                for (i, column) in self.columns.iter().enumerate() {
                    if let Some(value) = row.get(i) {
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