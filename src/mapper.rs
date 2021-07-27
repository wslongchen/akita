use std::{collections::BTreeMap, convert::TryFrom};

use crate::{AkitaError, UpdateWrapper, Wrapper};


#[derive(Clone)]
pub struct IPage <T> 
    where T: Sized + Clone {
    pub total: usize,
    pub size: usize,
    pub current: usize,
    pub records: Vec<T>
}

impl <T> IPage <T> 
where T: Sized + Clone{
    pub fn new(current: usize, size: usize, total: usize, records: Vec<T>) -> Self {
        Self {
            total,
            size,
            current,
            records,
        }
    }

    pub fn offset(&self) -> usize {
        if self.current > 0 { (self.current - 1) * self.size } else { 0 }
    }
}


pub trait BaseMapper{
    type Item;

    // /// Insert Data.
    // fn insert<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<u64>, AkitaError>;

    // /// Update Data With Wrapper.
    // fn update<'a, 'b, 'c, 'p>(&self, wrapper: &mut UpdateWrapper, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError>;

    // fn list<'a, 'b, 'c, 'p, W: Wrapper>(wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Vec<Self::Item>, AkitaError> where Self::Item: Clone;

    // fn page<'a, 'b, 'c, 'p, W: Wrapper>(page: usize, size: usize, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<IPage<Self::Item>, AkitaError> where Self::Item: Clone;

    // /// Find One With Wrapper.
    // fn find_one<'a, 'b, 'c, 'p, W: Wrapper>(wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<Self::Item>, AkitaError>;

    // /// Find Data With Table's Ident.
    // fn find_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<Self::Item>, AkitaError>;

    // /// Update Data With Table's Ident.
    // fn update_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError>;

    // /// Delete Data With Wrapper.
    // fn delete<'a, 'b, 'c, 'p, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError>;

    // /// Delete Data With Table's Ident.
    // fn delete_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError>;

    /// Get the Table Fields.
    fn get_table_fields() -> Result<String, AkitaError>;

    /// Get Table Idents.
    fn get_table_idents(&self) -> Result<String, AkitaError>;

    /// Get Condition Fields.
    fn get_update_fields(&self, set_sql: Option<String>) -> Result<String, AkitaError>;

    /// Get Table Name.
    fn get_table_name() -> Result<String, AkitaError>;
}


