use crate::{AkitaError, Wrapper, FromValue, ToValue, Params, GetTableName, GetFields};
use serde::{Serialize, Deserialize};

#[derive(Clone, Deserialize, Serialize)]
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

    /// Insert a record
    fn insert<I, M: AkitaMapper>(&self, entity_manager: &mut M) -> Result<Option<I>, AkitaError> where Self::Item : GetTableName + GetFields, I: FromValue;

    /// Insert Data Batch.
    fn insert_batch<I, M: AkitaMapper>(datas: &[&Self::Item], entity_manager: &mut M) -> Result<Vec<Option<I>>, AkitaError> where Self::Item : GetTableName + GetFields, I: FromValue;

    /// Update Data With Wrapper.
    fn update<M: AkitaMapper>(&self, wrapper: Wrapper, entity_manager: &mut M) -> Result<(), AkitaError> where Self::Item : GetTableName + GetFields;

    /// Query all records according to the entity condition
    fn list<M: AkitaMapper>(wrapper: Wrapper, entity_manager: &mut M) -> Result<Vec<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromValue;

    /// Query all records (and turn the page) according to the entity condition
    fn page<M: AkitaMapper>(page: usize, size: usize, wrapper: Wrapper, entity_manager: &mut M) -> Result<IPage<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromValue;

    /// Find One With Wrapper.
    fn find_one<M: AkitaMapper>(wrapper: Wrapper, entity_manager: &mut M) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromValue;

    /// Find Data With Table's Ident.
    fn find_by_id<I: ToValue, M: AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromValue;

    /// Update Data With Table's Ident.
    fn update_by_id<M: AkitaMapper>(&self, entity_manager: &mut M) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToValue ;

    /// Delete Data With Wrapper.
    fn delete<M: AkitaMapper>(&self, wrapper: Wrapper, entity_manager: &mut M) -> Result<(), AkitaError>where Self::Item : GetFields + GetTableName + ToValue ;

    /// Delete by ID
    fn delete_by_id<I: ToValue, M: AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToValue ;

    /// Get the Table Count.
    fn count<M: AkitaMapper>(&mut self, wrapper: Wrapper, entity_manager: &mut M) -> Result<usize, AkitaError>;

}

pub trait AkitaMapper {
    /// Get all the table of records
    fn list<T>(&mut self, wrapper: Wrapper) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get one the table of records
    fn select_one<T>(&mut self, wrapper: Wrapper) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue,
        I: ToValue;

    /// Get table of records with page
    fn page<T>(&mut self, page: usize, size: usize, wrapper: Wrapper) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get the total count of records
    fn count<T>(&mut self, wrapper: Wrapper) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove<T>(&mut self, wrapper: Wrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove_by_ids<T, I>(&mut self, ids: Vec<I>) -> Result<(), AkitaError>
        where
            I: ToValue,
            T: GetTableName + GetFields;

    /// Remove the records by id.
    fn remove_by_id<T, I>(&mut self, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields;
    

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, wrapper: Wrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToValue;

    /// Update the records by id.
    fn update_by_id<T>(&mut self, entity: &T) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToValue;

    #[allow(unused_variables)]
    fn save_batch<T, I>(&mut self, entities: &[&T]) -> Result<Vec<Option<I>>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue;

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue;

    /// save or update
    fn save_or_update<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue;

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromValue;

    fn execute_drop<'a, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>;

    fn execute_first<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<R, AkitaError>
    where
        R: FromValue;

    fn execute_result_opt<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromValue;
}