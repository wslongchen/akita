use crate::{AkitaError, UpdateWrapper, Wrapper, data::{FromAkita, ToAkita}, value::ToValue, information::{GetFields, GetTableName}};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IPage <T> 
    where T: Sized  {
    pub total: usize,
    pub size: usize,
    pub current: usize,
    pub records: Vec<T>
}

impl <T> IPage <T> 
where T: Sized {
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

    /// Insert Data.
    fn insert<M: AkitaMapper>(&self, entity_manager: &mut M) -> Result<usize, AkitaError> where Self::Item : GetTableName + GetFields;

    /// Insert Data Batch.
    fn insert_batch<M: AkitaMapper>(datas: &[&Self::Item], entity_manager: &mut M) -> Result<Vec<usize>, AkitaError> where Self::Item : GetTableName + GetFields;

    /// Update Data With Wrapper.
    fn update<W: Wrapper,M: AkitaMapper>(&self, wrapper: &mut UpdateWrapper, entity_manager: &mut M) -> Result<(), AkitaError> where Self::Item : GetTableName + GetFields;

    fn list<W: Wrapper,M: AkitaMapper>(wrapper: &mut W, entity_manager: &mut M) -> Result<Vec<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    fn page<W: Wrapper,M: AkitaMapper>(page: usize, size: usize, wrapper: &mut W, entity_manager: &mut M) -> Result<IPage<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Find One With Wrapper.
    fn find_one<W: Wrapper,M: AkitaMapper>(wrapper: &mut W, entity_manager: &mut M) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Find Data With Table's Ident.
    fn find_by_id<I: ToValue,M: AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Update Data With Table's Ident.
    fn update_by_id<M: AkitaMapper>(&self, entity_manager: &mut M) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Delete Data With Wrapper.
    fn delete<W: Wrapper,M: AkitaMapper>(&self, wrapper: &mut W, entity_manager: &mut M) -> Result<(), AkitaError>where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Delete Data With Table's Ident.
    fn delete_by_id<I: ToValue,M: AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Get the Table Count.
    fn count<T, W: Wrapper,M: AkitaMapper>(&mut self, wrapper: &mut W, entity_manager: &mut M) -> Result<usize, AkitaError>;

}

pub trait AkitaMapper {
    /// Get all the table of records
    fn list<T, W>(&mut self, wrapper: &mut W) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper;

    /// Get one the table of records
    fn select_one<T, W>(&mut self, wrapper: &mut W) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper;

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        I: ToValue;

    /// Get table of records with page
    fn page<T, W>(&mut self, page: usize, size: usize, wrapper: &mut W) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper;

    /// Get the total count of records
    fn count<T, W>(&mut self, wrapper: &mut W) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper;

    /// Remove the records by wrapper.
    fn remove<T, W>(&mut self, wrapper: &mut W) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper;

    /// Remove the records by id.
    fn remove_by_id<T, I>(&mut self, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields;
    

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, wrapper: &mut UpdateWrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToAkita;

    /// Update the records by id.
    fn update_by_id<T>(&mut self, entity: &T) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToAkita;

    #[allow(unused_variables)]
    fn save_batch<T>(&mut self, entities: &[&T]) -> Result<Vec<usize>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita;

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T>(&mut self, entity: &T) -> Result<usize, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita;

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R>(
        &mut self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromAkita;

    fn execute_drop<'a, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<(), AkitaError>;

    fn execute_first<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<R, AkitaError>
    where
        R: FromAkita;

    fn execute_result_opt<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita;
}
