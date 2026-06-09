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
use crate::driver::non_blocking::{AsyncDbDriver, AsyncDbExecutor};
use crate::errors::Result;
use crate::mapper::non_blocking::AsyncAkitaMapper;
use crate::mapper::IPage;
use akita_core::{FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, Params, Rows, Wrapper};

/// Asynchronous database transaction.
///
/// This struct represents an active database transaction. It automatically
/// logs a warning if dropped without being explicitly committed or rolled back.
///
/// For automatic lifecycle management, use `AkitaAsync::transaction()` instead
/// of manually calling `start_transaction()`.
pub struct AsyncAkitaTransaction {
    pub(crate) conn: AsyncDbDriver,
    pub(crate) committed: bool,
    pub(crate) rolled_back: bool,
}

impl Drop for AsyncAkitaTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            tracing::warn!(
                "AsyncAkitaTransaction dropped without commit or rollback. \
                 Consider using AkitaAsync::transaction() for automatic lifecycle management."
            );
        }
    }
}

impl AsyncAkitaTransaction {
    /// Commit the transaction.
    #[track_caller]
    pub async fn commit(&mut self) -> crate::prelude::Result<()> {
        self.conn.commit().await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction.
    #[track_caller]
    pub async fn rollback(&mut self) -> crate::prelude::Result<()> {
        self.conn.rollback().await?;
        self.rolled_back = true;
        Ok(())
    }

    /// Get the last insert ID.
    pub async fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id().await
    }

    /// Get the number of affected rows.
    pub async fn affected_rows(&self) -> u64 {
        self.conn.affected_rows().await
    }
}

#[async_trait::async_trait]
impl AsyncAkitaMapper for AsyncAkitaTransaction {
    /// Get all the table of records
    async fn list<T>(&self, wrapper: Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        self.conn.list(wrapper).await
    }

    /// Get one the table of records
    async fn select_one<T>(&self, wrapper: Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        self.conn.select_one(wrapper).await
    }

    /// Get one the table of records by id
    async fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
        I: IntoAkitaValue + Send + Sync,
    {
        self.conn.select_by_id(id).await
    }

    /// Get table of records with page
    async fn page<T>(&self, page: u64, size: u64, wrapper: Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue + Send + Sync,
    {
        self.conn.page(page, size, wrapper).await
    }

    /// Get the total count of records
    async fn count<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        self.conn.count::<T>(wrapper).await
    }

    /// Remove the records by wrapper.
    async fn remove<T>(&self, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + Send + Sync,
    {
        self.conn.remove::<T>(wrapper).await
    }

    /// Remove the records by ids.
    async fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        self.conn.remove_by_ids::<T, I>(ids).await
    }

    /// Remove the records by id.
    async fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: IntoAkitaValue + Send + Sync,
        T: GetTableName + GetFields + Send + Sync,
    {
        self.conn.remove_by_id::<T, I>(id).await
    }

    /// Update the records by wrapper.
    async fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        self.conn.update(entity, wrapper).await
    }

    /// Update the records by id.
    async fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        self.conn.update_by_id(entity).await
    }

    async fn update_batch_by_id<T>(&self, entities: &[T]) -> Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
    {
        self.conn.update_batch_by_id(entities).await
    }

    #[allow(unused_variables)]
    async fn save_batch<T, E>(&self, entities: E) -> Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        E: IntoIterator<Item = T> + Send + Sync,
    {
        self.conn.save_batch(entities).await
    }

    /// called multiple times when using database platform that doesn't support multiple value
    async fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        self.conn.save(entity).await
    }

    async fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue + Send + Sync,
        I: FromAkitaValue + Send + Sync,
    {
        self.conn.save_or_update(entity).await
    }

    async fn exec_iter<S, P>(&self, sql: S, params: P) -> Result<Rows>
    where
        S: Into<String> + Send + Sync,
        P: Into<Params> + Send + Sync,
    {
        self.conn.exec_iter(sql, params).await
    }
}
