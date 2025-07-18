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

use akita_core::{from_value, from_value_opt, Rows};
use crate::{AkitaError, Wrapper, FromValue, ToValue, Params, GetTableName, GetFields, errors::Result};
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
    fn insert<I, M: AkitaMapper>(&self, entity_manager: &M) -> Result<Option<I>> where Self::Item : GetTableName + GetFields, I: FromValue;

    /// Insert Data Batch.
    fn insert_batch<M: AkitaMapper>(datas: &[&Self::Item], entity_manager: &M) -> Result<()> where Self::Item : GetTableName + GetFields;

    /// Update Data With Wrapper.
    fn update<M: AkitaMapper>(&self, wrapper: Wrapper, entity_manager: &M) -> Result<u64> where Self::Item : GetTableName + GetFields;

    /// Query all records according to the entity condition
    fn list<M: AkitaMapper>(wrapper: Wrapper, entity_manager: &M) -> Result<Vec<Self::Item>> where Self::Item : GetTableName + GetFields + FromValue;

    /// Query all records (and turn the page) according to the entity condition
    fn page<M: AkitaMapper>(page: usize, size: usize, wrapper: Wrapper, entity_manager: &M) -> Result<IPage<Self::Item>> where Self::Item : GetTableName + GetFields + FromValue;

    /// Find One With Wrapper.
    fn find_one<M: AkitaMapper>(wrapper: Wrapper, entity_manager: &M) -> Result<Option<Self::Item>> where Self::Item : GetTableName + GetFields + FromValue;

    /// Find Data With Table's Ident.
    fn find_by_id<I: ToValue, M: AkitaMapper>(&self, entity_manager: &M, id: I) -> Result<Option<Self::Item>> where Self::Item : GetTableName + GetFields + FromValue;

    /// Update Data With Table's Ident.
    fn update_by_id<M: AkitaMapper>(&self, entity_manager: &M) -> Result<u64> where Self::Item : GetFields + GetTableName + ToValue ;

    /// Delete Data With Wrapper.
    fn delete<M: AkitaMapper>(&self, wrapper: Wrapper, entity_manager: &M) -> Result<u64>where Self::Item : GetFields + GetTableName + ToValue ;

    /// Delete by ID
    fn delete_by_id<I: ToValue, M: AkitaMapper>(&self, entity_manager: &M, id: I) -> Result<u64> where Self::Item : GetFields + GetTableName + ToValue ;

    /// Get the Table Count.
    fn count<M: AkitaMapper>(&mut self, wrapper: Wrapper, entity_manager: &M) -> Result<usize>;

}

pub trait AkitaMapper {
    /// Get all the table of records
    fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get one the table of records
    fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromValue,
        I: ToValue;

    /// Get table of records with page
    fn page<T>(&self, page: usize, size: usize, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromValue;

    /// Get the total count of records
    fn count<T>(&self, wrapper: Wrapper) -> Result<usize>
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
        where
            I: ToValue,
            T: GetTableName + GetFields;

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: ToValue,
        T: GetTableName + GetFields;
    

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue;

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue;

    #[allow(unused_variables)]
    fn save_batch<T>(&self, entities: &[&T]) -> Result<()>
    where
        T: GetTableName + GetFields + ToValue;

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue;

    /// save or update
    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue;

    fn query<T, Q>(&self, query: Q) -> Result<Vec<T>>
        where
            Q: Into<String>,
            T: FromValue,
    {
        self.query_map(query, from_value)
    }

    fn query_opt<T, Q>(&self, query: Q) -> Result<Vec<Result<T>>>
        where
            Q: Into<String>,
            T: FromValue,
    {
        self.query_map(query, from_value_opt).map(|v| v.into_iter().map(|v| v.map_err(AkitaError::from)).collect())
    }

    fn query_first<S: Into<String>, R>(
        &self, sql: S
    ) -> Result<R>
        where
            R: FromValue,
    {
        self.exec_first(sql, ())
    }

    fn query_first_opt<R, S: Into<String>>(
        &self, sql: S,
    ) -> Result<Option<R>>
        where
            R: FromValue,
    {
        self.exec_first_opt(sql, ())
    }


    fn query_map<T, F, Q, U>(&self, query: Q, mut f: F) -> Result<Vec<U>>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    fn query_fold<T, F, Q, U>(&self, query: Q, init: U, mut f: F) -> Result<U>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(U, T) -> U,
    {
        self.exec_iter::<_, _>(query, ()).map(|r| r.iter().map(|data| T::from_value(&data))
            .fold(init, |acc, row| f(acc, row)))
    }


    fn query_drop<Q>(&mut self, query: Q) -> Result<()>
        where
            Q: Into<String>,
    {
        self.query_iter(query).map(drop)
    }

    fn exec_map<T, F, Q, U>(&self, query: Q, mut f: F) -> Result<Vec<U>>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    fn query_iter<S: Into<String>>(
        &self,
        sql: S,
    ) -> Result<Rows>
    {
        self.exec_iter(sql, ())
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Rows>;

    #[allow(clippy::redundant_closure)]
    fn exec_raw<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>>
        where
            R: FromValue,
    {
        let rows = self.exec_iter(&sql.into(), params.into())?;
        Ok(rows.iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    fn exec_first<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<R>
        where
            R: FromValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>> = self.exec_raw(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    fn exec_drop<S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<()>
    {
        let sql: String = sql.into();
        let _result: Vec<()> = self.exec_raw(&sql, params)?;
        Ok(())
    }

    fn exec_first_opt<R, S: Into<String>, P: Into<Params>>(
        &self,
        sql: S,
        params: P,
    ) -> Result<Option<R>>
        where
            R: FromValue,
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