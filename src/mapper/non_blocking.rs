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

use async_trait::async_trait;
use crate::prelude::*;
use std::marker::Sync;
use crate::mapper::IPage;

#[async_trait]
pub trait AsyncAkitaMapper {
    /// Get all the table of records
    async fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send;

    /// Get one the table of records
    async fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send;

    /// Get one the table of records by id
    async fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send,
        I: IntoAkitaValue + Sync + Send;

    /// Get table of records with page
    async fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Sync + Send;

    /// Get the total count of records
    async fn count<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Sync + Send;

    /// Remove the records by wrapper.
    async fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Sync + Send;

    /// Remove the records by wrapper.
    async fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
    where
        I: IntoAkitaValue + Sync + Send,
        T: GetTableName + GetFields + Sync + Send;

    /// Remove the records by id.
    async fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue + Into<AkitaValue> + Sync + Send,
        T: GetTableName + GetFields + Sync + Send;

    /// Update the records by wrapper.
    async fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send;

    /// Update the records by id.
    async fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send;

    async fn update_batch_by_id<T>(&self, entities: &[T]) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send;

    async fn save_batch<T, E>(&self, entities: E) -> Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send,
        E: IntoIterator<Item = T> + Send + Sync;

    async fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send,
        I: FromAkitaValue + Sync + Send;

    /// save or update
    async fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Sync + Send,
        I: FromAkitaValue + Sync + Send;

    async fn exec_iter<S: Into<String> + Send + Sync, P: Into<Params> + Send + Sync>(&self, sql: S, params: P) -> Result<Rows>;

    async fn simple_query<T, Q>(&self, query: Q) -> crate::errors::Result<Vec<T>>
    where
        Q: Into<String> + Send + Sync,
        T: FromAkitaValue + Send + Sync,
    {
        self.query_map(query, T::from_value).await
    }

    async fn query_opt<T, Q>(&self, query: Q) -> crate::errors::Result<Vec<crate::errors::Result<T>>>
    where
        Q: Into<String> + Send + Sync,
        T: FromAkitaValue + Send + Sync,
    {
        self.query_map(query, T::from_value_opt).await.map(|v| v.into_iter().map(|v| v.map_err(AkitaError::from)).collect())
    }

    async fn query_first<S: Into<String> + Send + Sync, R: Sync + Send>(
        &self, sql: S
    ) -> crate::errors::Result<R>
    where
        R: FromAkitaValue + Send + Sync,
    {
        self.exec_first(sql, ()).await
    }

    async fn query_first_opt<R, S: Into<String> + Send + Sync>(
        &self, sql: S,
    ) -> crate::errors::Result<Option<R>>
    where
        R: FromAkitaValue + Send + Sync,
    {
        self.exec_first_opt(sql, ()).await
    }

    async fn query_map<T, F, Q, U>(&self, query: Q, mut f: F) -> crate::errors::Result<Vec<U>>
    where
        Q: Into<String> + Send + Sync,
        T: FromAkitaValue + Send + Sync,
        U: Send + Sync,  
        F: FnMut(T) -> U + Send + Sync,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        }).await
    }

    async fn query_fold<T, F, Q, U>(&self, query: Q, init: U, mut f: F) -> crate::errors::Result<U>
    where
        Q: Into<String> + Send + Sync,
        T: FromAkitaValue + Send + Sync,
        U: Send + Sync, 
        F: FnMut(U, T) -> U + Send + Sync,
    {
        self.exec_iter::<_, _>(query, ()).await.map(|r| r.object_iter().map(|data| T::from_value(&data))
            .fold(init, |acc, row| f(acc, row)))
    }

    async fn query_drop<Q>(&self, query: Q) -> crate::errors::Result<()>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_iter(query).await.map(drop)
    }

    async fn exec_map<T, F, Q, U>(&self, query: Q, mut f: F) -> crate::errors::Result<Vec<U>>
    where
        Q: Into<String> + Send + Sync,
        T: FromAkitaValue + Send + Sync,
        U: Send + Sync,
        F: FnMut(T) -> U + Send + Sync,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        }).await
    }

    async fn query_iter<S: Into<String> + Send + Sync>(
        &self,
        sql: S,
    ) -> crate::errors::Result<Rows>
    {
        self.exec_iter(sql, ()).await
    }

    #[allow(clippy::redundant_closure)]
    async fn exec_raw<R, S: Into<String> + Send + Sync, P: Into<Params> + Send + Sync>(
        &self,
        sql: S,
        params: P,
    ) -> crate::errors::Result<Vec<R>>
    where
        R: FromAkitaValue + Send + Sync,
    {
        let rows = self.exec_iter(&sql.into(), params.into()).await?;
        Ok(rows.object_iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    async fn exec_first<R, S: Into<String> + Send + Sync, P: Into<Params> + Send + Sync>(
        &self,
        sql: S,
        params: P,
    ) -> crate::errors::Result<R>
    where
        R: FromAkitaValue + Send + Sync,
    {
        let sql: String = sql.into();
        let result: crate::errors::Result<Vec<R>> = self.exec_raw(&sql, params).await;
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Empty record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    async fn exec_drop<S: Into<String> + Send + Sync, P: Into<Params> + Send + Sync>(
        &self,
        sql: S,
        params: P,
    ) -> crate::errors::Result<()>
    {
        let sql: String = sql.into();
        let _result: crate::errors::Result<Vec<()>> = self.exec_raw(&sql, params).await;
        Ok(())
    }

    async fn exec_first_opt<R, S: Into<String> + Send + Sync, P: Into<Params> + Send + Sync>(
        &self,
        sql: S,
        params: P,
    ) -> crate::errors::Result<Option<R>>
    where
        R: FromAkitaValue + Send + Sync,
    {
        let sql: String = sql.into();
        let result: crate::errors::Result<Vec<R>> = self.exec_raw(&sql, params).await;
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