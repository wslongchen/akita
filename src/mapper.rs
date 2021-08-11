use crate::{AkitaError, UpdateWrapper, Wrapper, data::{FromAkita, ToAkita}, manager::AkitaEntityManager, value::ToValue, information::{GetFields, GetTableName}};
use serde::{Deserialize, Serialize};

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

    /// Insert Data.
    fn insert(&self, entity_manager: &mut AkitaEntityManager) -> Result<(), AkitaError> where Self::Item : GetTableName + GetFields;

    /// Insert Data Batch.
    fn insert_batch(datas: &[&Self::Item], entity_manager: &mut AkitaEntityManager) -> Result<(), AkitaError> where Self::Item : GetTableName + GetFields;

    /// Update Data With Wrapper.
    fn update<W: Wrapper>(&self, wrapper: &mut UpdateWrapper, entity_manager: &mut AkitaEntityManager) -> Result<(), AkitaError> where Self::Item : GetTableName + GetFields;

    fn list<W: Wrapper>(wrapper: &mut W, entity_manager: &mut AkitaEntityManager) -> Result<Vec<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    fn page<W: Wrapper>(page: usize, size: usize, wrapper: &mut W, entity_manager: &mut AkitaEntityManager) -> Result<IPage<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Find One With Wrapper.
    fn find_one<W: Wrapper>(wrapper: &mut W, entity_manager: &mut AkitaEntityManager) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Find Data With Table's Ident.
    fn find_by_id<I: ToValue>(&self, entity_manager: &mut AkitaEntityManager, id: I) -> Result<Option<Self::Item>, AkitaError> where Self::Item : GetTableName + GetFields + FromAkita;

    /// Update Data With Table's Ident.
    fn update_by_id<I: ToValue>(&self, entity_manager: &mut AkitaEntityManager, id: I) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Delete Data With Wrapper.
    fn delete<W: Wrapper>(&self, wrapper: &mut W, entity_manager: &mut AkitaEntityManager) -> Result<(), AkitaError>where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Delete Data With Table's Ident.
    fn delete_by_id<I: ToValue>(&self, entity_manager: &mut AkitaEntityManager, id: I) -> Result<(), AkitaError> where Self::Item : GetFields + GetTableName + ToAkita ;

    /// Get the Table Count.
    fn count<T, W: Wrapper>(&mut self, wrapper: &mut W, entity_manager: &mut AkitaEntityManager) -> Result<usize, AkitaError>;

}


