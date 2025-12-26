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
use std::sync::Arc;
use crate::errors::Result;
use crate::prelude::{AkitaError};
use akita_core::{FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, Params, Rows, Wrapper};
use crate::mapper::blocking::AkitaMapper;
use crate::mapper::IPage;

/// Provide type safety for each entity type Repository
pub struct EntityRepository<M, T>{
    mapper: Arc<M>,
    _phantom: std::marker::PhantomData<T>,
}


impl<M, T> EntityRepository<M, T> {
    pub fn new(mapper: M) -> Self {
        Self { mapper: Arc::new(mapper) ,_phantom: std::marker::PhantomData, }
    }
}

#[allow(unused)]
impl<M, T> EntityRepository<M, T>
where
    M: AkitaMapper,
{
    
    /// Get all the table of records
    pub fn list(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue {
        self.mapper.list(wrapper)
    }

    /// Get one the table of records
    pub fn select_one(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue {
        self.mapper.select_one(wrapper)
    }

    /// Get one the table of records by id
    pub fn select_by_id<I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
        I: IntoAkitaValue {
        self.mapper.select_by_id(id)
    }

    /// Get table of records with page
    pub fn page(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue {
        self.mapper.page(page, size, wrapper)
    }

    /// Get the total count of records
    pub fn count(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields {
        self.mapper.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    pub fn remove(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields {
        self.mapper.remove::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    pub fn remove_by_ids<I>(&self, ids: Vec<I>) -> Result<u64>
    where
        I: IntoAkitaValue,
        T: GetTableName + GetFields {
        self.mapper.remove_by_ids::<T, I>(ids)
    }

    /// Remove the records by id.
    pub fn remove_by_id<I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue,
        T: GetTableName + GetFields {
        self.mapper.remove_by_id::<T, I>(id)
    }


    /// Update the records by wrapper.
    pub fn update(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue {
        self.mapper.update(entity, wrapper)
    }

    /// Update the records by id.
    pub fn update_by_id(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue {
        self.mapper.update_by_id(entity)
    }

    #[allow(unused_variables)]
    pub fn update_batch_by_id(&self, entities: &Vec<T>) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue {
        self.mapper.update_batch_by_id(entities)
    }

    #[allow(unused_variables)]
    pub fn save_batch<E>(&self, entities: E) -> Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        E: IntoIterator<Item = T> {
        self.mapper.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save<I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue {
        self.mapper.save(entity)
    }

    /// save or update
    pub fn save_or_update<I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue {
        self.mapper.save_or_update(entity)
    }

    pub fn query_iter<S: Into<String>>(
        &self,
        sql: S,
    ) -> Result<Rows>
    {
        self.exec_iter(sql, ())
    }

    pub fn exec_iter<S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Rows> {
        self.mapper.exec_iter(sql, params)
    }

    #[allow(clippy::redundant_closure)]
    pub fn exec_raw<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>>
    where
        R: FromAkitaValue,
    {
        let rows = self.exec_iter(&sql.into(), params.into())?;
        Ok(rows.object_iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    pub fn exec_first<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<R>
    where
        R: FromAkitaValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>> = self.exec_raw(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Empty record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn exec_drop<S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<()>
    {
        let sql: String = sql.into();
        let _result: Vec<()> = self.exec_raw(&sql, params)?;
        Ok(())
    }

    pub fn exec_first_opt<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Option<R>>
    where
        R: FromAkitaValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>> = self.exec_raw(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Ok(None),
                1 => Ok(Some(result.remove(0))),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }
}