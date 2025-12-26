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
use akita_core::{FromAkitaValue, GetFields, GetTableName, IntoAkitaValue, Wrapper};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::core::blocking::Akita;
use crate::ext::Request;
use crate::mapper::blocking::AkitaMapper;
use crate::mapper::IPage;
use crate::prelude::{AkitaError};

pub trait Mapper<Entity, Dto, Params>: Sync + Send
where
    Entity: GetTableName + GetFields + FromAkitaValue + IntoAkitaValue + DeserializeOwned + Clone,
    Dto: From<Entity> + Send + Sync + Serialize,
    Params: Request,
{
    fn get_akita(&self) -> Result<Arc<Akita>, AkitaError>;

    /// Public pagination query method
    fn page(&self, arg: &Params) -> Result<IPage<Dto>, AkitaError> {
        let ak = self.get_akita()?;
        //Construct query conditions
        let mut wrapper = arg.get_wrapper();

        if let Some(sort_field) = arg.desc_fields() {
            if !sort_field.is_empty() {
                wrapper = wrapper.order_by_desc(sort_field.split(",").collect())
            }
        }
        if let Some(sort_field) = arg.asc_fields() {
            if !sort_field.is_empty() {
                wrapper = wrapper.order_by_asc(sort_field.split(",").collect())
            }
        }
        
        let page_no = arg.get_page_no();
        let page_size = arg.get_page_size();
        // Perform a paginated query
        let data_page = ak.page::<Entity>(page_no, page_size, wrapper)?;
        let vos = data_page
            .records
            .into_iter()
            .map(|e| Dto::from(e.clone()))
            .collect::<Vec<Dto>>();

        Ok(IPage::<Dto> {
            records: vos,
            total: data_page.total,
            size: data_page.size,
            current: data_page.current,
        })
    }

    ///
    /// Get statistics
    ///
    fn count(&self, arg: &Params) -> Result<u64, AkitaError> {
        let ak = self.get_akita()?;
        let wrapper = arg.get_wrapper();
        //执行查询
        let count = ak.count::<Entity>(wrapper)?;
        Ok(count)
    }

    ///
    /// Public list query method
    ///
    fn list(&self, arg: &Params) -> Result<Vec<Dto>, AkitaError> {
        let ak = self.get_akita()?;
        //构建查询条件
        let wrapper = arg.get_wrapper();
        //执行查询
        let list: Vec<Entity> = ak.list(wrapper)?;
        let result = list
            .into_iter()
            .map(|e| Dto::from(e.clone()))
            .collect::<Vec<Dto>>();
        Ok(result)
    }

    ///
    /// Update the entity based on the id
    ///
    fn update_by_id(&self, data: &Entity) -> Result<bool, AkitaError> {
        let ak = self.get_akita()?;
        if let Ok(res) = ak.update_by_id(data) {
            Ok(res > 0)
        } else {
            Ok(false)
        }
    }

    ///
    /// Query a single value based on the ID query criteria
    ///
    fn select_by_id(&self, id: String) -> Result<Option<Dto>, AkitaError> {
        let ak = self.get_akita()?;
        let detail: Option<Entity> = ak.select_by_id(&id)?;
        let vo = detail.map(Dto::from);
        return Ok(vo);
    }

    ///
    /// Query individual values based on query criteria
    ///
    fn select_one(&self, arg: &Params) -> Result<Option<Dto>, AkitaError> {
        let ak = self.get_akita()?;
        //构建查询条件
        let wrapper = arg.get_wrapper();
        let detail: Option<Entity> = ak.select_one(wrapper)?;
        let vo = detail.map(Dto::from);
        return Ok(vo);
    }


    ///
    /// Save the entity
    ///
    fn save(&self, data: &Entity) -> Result<i64, AkitaError> {
        let ak = self.get_akita()?;
        let last_insert_id = ak.save_or_update::<_, i64>(data)?;
        return Ok(last_insert_id.unwrap_or_default());
    }

    ///
    /// Bulk save entities
    ///
    fn save_batch(&self, list: Vec<Entity>) -> Result<(), AkitaError> {
        let ak = self.get_akita()?;
        let _ = ak.save_batch::<Entity, _>(list).ok();
        Ok(())
    }

    ///
    /// Delete the entity
    ///
    fn remove_by_id(&self, id: &String) -> Result<bool, AkitaError> {
        let ak = self.get_akita()?;
        let res = ak.remove_by_id::<Entity, _>(id).ok();
        Ok(res.unwrap_or_default() > 0)
    }

    ///
    /// Bulk deletion of entities
    ///
    fn remove_batch(&self, ids: Vec<u64>) -> Result<(), AkitaError> {
        let ak = self.get_akita()?;
        let _ = ak.remove_by_ids::<Entity, u64>(ids).ok();
        Ok(())
    }
}

pub trait IService<Entity, Dto, Params, M: Mapper<Entity, Dto, Params> + 'static>
where
    Entity: GetTableName + GetFields + FromAkitaValue + IntoAkitaValue + DeserializeOwned + Clone,
    Dto: From<Entity> + Send + Sync + Serialize,
    Params: Request {

    // 获取Mapper的引用
    fn get_mapper(&self) -> &M;

    fn get_akita(&self) -> Result<Arc<Akita>, AkitaError> {
        self.get_mapper().get_akita()
    }

    /// Public pagination query method
    fn page(&self, arg: &Params) -> Result<IPage<Dto>, AkitaError> {
        self.get_mapper().page(arg)
    }

    ///
    /// Get statistics
    ///
    fn count(&self, arg: &Params) -> Result<u64, AkitaError> {
        self.get_mapper().count(arg)
    }

    ///
    /// Public list query method
    ///
    fn list(&self, arg: &Params) -> Result<Vec<Dto>, AkitaError> {
        self.get_mapper().list(arg)
    }

    ///
    /// Update the entity based on the id
    ///
    fn update_by_id(&self, data: &Entity) -> Result<bool, AkitaError> {
        self.get_mapper().update_by_id(data)
    }

    ///
    /// Query a single value based on the ID query criteria
    ///
    fn select_by_id(&self, id: String) -> Result<Option<Dto>, AkitaError> {
        self.get_mapper().select_by_id(id)
    }

    ///
    /// Query individual values based on query criteria
    ///
    fn select_one(&self, arg: &Params) -> Result<Option<Dto>, AkitaError> {
        self.get_mapper().select_one(arg)
    }


    ///
    /// Save the entity
    ///
    fn save(&self, data: &Entity) -> Result<i64, AkitaError> {
        self.get_mapper().save(data)
    }

    ///
    /// Bulk save entities
    ///
    fn save_batch(&self, list: Vec<Entity>) -> Result<(), AkitaError> {
        self.get_mapper().save_batch(list)
    }

    ///
    /// Delete the entity
    ///
    fn remove_by_id(&self, id: &String) -> Result<bool, AkitaError> {
        self.get_mapper().remove_by_id(id)
    }

    ///
    /// Bulk deletion of entities
    ///
    fn remove_batch(&self, ids: Vec<u64>) -> Result<(), AkitaError> {
        self.get_mapper().remove_batch(ids)
    }
}