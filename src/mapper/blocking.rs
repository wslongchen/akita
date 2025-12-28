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
use crate::mapper::IPage;
use crate::prelude::{AkitaError, GetFields, GetTableName, Params};
use crate::errors::Result;
use akita_core::{from_akita_value, from_akita_value_opt, AkitaValue, FromAkitaValue, IntoAkitaValue, Rows, Wrapper};

pub trait AkitaMapper {
    /// Get all the table of records
    fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue;

    /// Get one the table of records
    fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue;

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
        I: IntoAkitaValue;

    /// Get table of records with page
    fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue;

    /// Get the total count of records
    fn count<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields;

    /// Remove the records by wrapper.
    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
        where
            I: IntoAkitaValue,
            T: GetTableName + GetFields;

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue + Into<AkitaValue>,
        T: GetTableName + GetFields;
    

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue;

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue;

    #[allow(unused_variables)]
    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue;

    #[allow(unused_variables)]
    fn save_batch<T, E>(&self, entities: E) -> Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        E: IntoIterator<Item = T>;

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue;

    /// save or update
    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
        where
            T: GetTableName + GetFields + IntoAkitaValue,
            I: FromAkitaValue;

    fn simple_query<T, Q>(&self, query: Q) -> Result<Vec<T>>
        where
            Q: Into<String>,
            T: FromAkitaValue,
    {
        self.query_map(query, from_akita_value)
    }

    fn query_opt<T, Q>(&self, query: Q) -> Result<Vec<Result<T>>>
        where
            Q: Into<String>,
            T: FromAkitaValue,
    {
        self.query_map(query, from_akita_value_opt).map(|v| v.into_iter().map(|v| v.map_err(AkitaError::from)).collect())
    }

    fn query_first<S: Into<String>, R>(
        &self, sql: S
    ) -> Result<R>
        where
            R: FromAkitaValue,
    {
        self.exec_first(sql, ())
    }

    fn query_first_opt<R, S: Into<String>>(
        &self, sql: S,
    ) -> Result<Option<R>>
        where
            R: FromAkitaValue,
    {
        self.exec_first_opt(sql, ())
    }


    fn query_map<T, F, Q, U>(&self, query: Q, mut f: F) -> Result<Vec<U>>
        where
            Q: Into<String>,
            T: FromAkitaValue,
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
            T: FromAkitaValue,
            F: FnMut(U, T) -> U,
    {
        self.exec_iter::<_, _>(query, ()).map(|r| r.object_iter().map(|data| T::from_value(&data))
            .fold(init, |acc, row| f(acc, row)))
    }


    fn query_drop<Q>(&self, query: Q) -> Result<()>
        where
            Q: Into<String>,
    {
        self.query_iter(query).map(drop)
    }

    fn exec_map<T, F, Q, U>(&self, query: Q, mut f: F) -> Result<Vec<U>>
        where
            Q: Into<String>,
            T: FromAkitaValue,
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
            R: FromAkitaValue,
    {
        let rows = self.exec_iter(&sql.into(), params.into())?;
        Ok(rows.object_iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    fn exec_first<R, S: Into<String>, P: Into<Params>>(
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