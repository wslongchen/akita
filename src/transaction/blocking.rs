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
use akita_core::{FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, Params, Rows, Wrapper};
use crate::driver::blocking::DbDriver;
use crate::mapper::blocking::AkitaMapper;
use crate::mapper::IPage;

pub struct AkitaTransaction {
    pub(crate) conn: DbDriver,
    pub(crate) committed: bool,
    pub(crate) rolled_back: bool,
}


#[allow(unused)]
impl AkitaTransaction {
    pub fn commit(&mut self) -> crate::prelude::Result<()> {
        self.conn.commit()?;
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> crate::prelude::Result<()> {
        self.conn.rollback()?;
        self.rolled_back = true;
        Ok(())
    }
    
    pub fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id()
    }
    
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows()
    }
}

impl<'a> Drop for AkitaTransaction {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            self.conn.rollback().unwrap_or_default();
        }
    }
}


#[allow(unused)]
impl AkitaMapper for AkitaTransaction {

    /// Get all the table of records
    fn list<T>(&self, wrapper:Wrapper) -> crate::prelude::Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper:Wrapper) -> crate::prelude::Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> crate::prelude::Result<Option<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,
        I: IntoAkitaValue
    {
        self.conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T>(&self, page: u64, size: u64, wrapper:Wrapper) -> crate::prelude::Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromAkitaValue,

    {
        self.conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper:Wrapper) -> crate::prelude::Result<u64>
    where
        T: GetTableName + GetFields,
    {
        self.conn.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper:Wrapper) -> crate::prelude::Result<u64>
    where
        T: GetTableName + GetFields,
    {
        self.conn.remove::<T>(wrapper)
    }

    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> crate::prelude::Result<u64>
    where I: IntoAkitaValue, T: GetTableName + GetFields {
        self.conn.remove_by_ids::<T,I>(ids)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> crate::prelude::Result<u64>
    where
        I: IntoAkitaValue,
        T: GetTableName + GetFields {
        self.conn.remove_by_id::<T, I>(id)

    }

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> crate::prelude::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue {
        self.conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> crate::prelude::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue {
        self.conn.update_by_id(entity)

    }

    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> crate::prelude::Result<u64>
    where
        T: GetTableName + GetFields + IntoAkitaValue
    {
        self.conn.update_batch_by_id(entities)
    }

    #[allow(unused_variables)]
    fn save_batch<T, E>(&self, entities: E) -> crate::prelude::Result<()>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        E: IntoIterator<Item = T>,
    {
        self.conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> crate::prelude::Result<Option<I>>
    where
        T: GetTableName + GetFields + IntoAkitaValue,
        I: FromAkitaValue,
    {
        self.conn.save(entity)
    }

    fn save_or_update<T, I>(&self, entity: &T) -> crate::prelude::Result<Option<I>>
    where T: GetTableName + GetFields + IntoAkitaValue, I: FromAkitaValue {
        self.conn.save_or_update(entity)
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> crate::prelude::Result<Rows> {
        self.conn.exec_iter(sql, params)
    }
}
