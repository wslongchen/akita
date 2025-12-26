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
#[cfg(feature = "mysql-sync")]


use std::time::Duration;
use akita::prelude::*;
use chrono::{NaiveDate, NaiveDateTime};
use uuid::Uuid;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct SysUser {
    pub id: i64,
    pub pk: String,
    pub user_type: Option<String>,
    pub name: Option<String>,
    pub headline: Option<NaiveDateTime>,
    pub tenant_id: i64,
    pub status: u8,
    pub level: u8,
    pub age: Option<u8>,
    pub birthday: Option<NaiveDate>,
    pub gender: u8,
    pub is_org: bool,
    pub url_token: String,
}

impl SysUser
{
    #[doc = r" 查询单个实体"] pub fn select_one < M >
    (mapper : & M, wrapper : Wrapper) -> Result < Option <
        Self >, AkitaError> where M : AkitaMapper, { mapper.select_one(wrapper) }
    #[doc = r" 根据ID查找实体"] pub fn select_by_id < M, I >
    (mapper : & M, id : I) -> Result < Option < Self >, AkitaError> where M :
    AkitaMapper, I : IntoAkitaValue,
    { mapper.select_by_id(id) } #[doc = r" 分页查询"] pub fn page < M >
(mapper : & M, page : u64, size : u64, wrapper : Wrapper) ->
    Result < IPage < Self >, AkitaError> where M : AkitaMapper,
{ mapper.page(page, size, wrapper) } #[doc = r" 计数"] pub fn count < M
> (mapper : & M, wrapper : Wrapper) -> Result < u64 , AkitaError>
where M : AkitaMapper, { mapper.count :: < Self > (wrapper) }
    #[doc = r" 删除当前实体（根据ID）"] pub fn remove_by_id < M, I >
    (& self, mapper : & M, id : I) -> Result < u64 , AkitaError> where M : AkitaMapper, I : IntoAkitaValue,
    { mapper.remove_by_id :: < Self, I > (id) }
    #[doc = r" 根据条件删除"] pub fn remove < M >
    (& self, mapper : & M, wrapper : Wrapper) -> Result <
        u64, AkitaError > where M : AkitaMapper,
    { mapper.remove :: < Self > (wrapper) }
    #[doc = r" 批量删除（根据ID列表）"] pub fn remove_by_ids < M, I
    > (mapper : & M, ids : Vec < I >) -> Result < u64, AkitaError > where M :
    AkitaMapper, I : IntoAkitaValue,
    { mapper.remove_by_ids :: < Self, I > (ids) }
    #[doc = r" 更新当前实体（根据ID）"] pub fn update_by_id < M >
    (& self, mapper : & M) -> Result < u64 , AkitaError> where M : AkitaMapper, { mapper.update_by_id(self) }
    #[doc = r" 保存或更新当前实体"] pub fn save_or_update < M, I >
    (& self, mapper : & M) -> Result < Option < I >, AkitaError> where M : AkitaMapper, I : FromAkitaValue,
    { mapper.save_or_update(self) } #[doc = r" 批量更新（根据ID）"]
pub fn update_batch_by_id < M > (mapper : & M, entities : & Vec < Self >)
                                 -> Result < u64 , AkitaError> where M : AkitaMapper,
{ mapper.update_batch_by_id(entities) } #[doc = r" 根据条件更新"]
pub fn update < M > (& self, mapper : & M, wrapper : Wrapper) ->
    Result < u64 , AkitaError> where M : AkitaMapper,
{ mapper.update(self, wrapper) }
    #[doc = r" 保存当前实体（插入）"] pub fn save < M, I >
    (& self, mapper : & M) -> Result < Option < I >, AkitaError> where M : AkitaMapper, I : FromAkitaValue, { mapper.save(self) }
    #[doc = r" 批量插入"] pub fn save_batch < M, E >
    (mapper : & M, entities : E) -> Result < () , AkitaError>
    where M : AkitaMapper, E :
    IntoIterator < Item = Self > { mapper.save_batch(entities) }
    #[doc = r" 查询所有记录"] pub fn list < M >
    (mapper : & M, wrapper : Wrapper) -> Result < Vec < Self
    >, AkitaError> where M : AkitaMapper, { mapper.list(wrapper) }
    #[doc = r" 创建查询包装器"] pub fn query() -> Wrapper
    { Wrapper :: new() }
} impl IntoAkitaValue for SysUser
{
    fn into_value(& self) -> AkitaValue
    {
        let mut data = AkitaValue ::
    new_object(); data.insert_obj("id", & self.id);
        data.insert_obj("pk", & self.pk);
        data.insert_obj("user_type", & self.user_type);
        data.insert_obj("name", & self.name);
        data.insert_obj("headline", & self.headline);
        data.insert_obj("tenant_id", & self.tenant_id);
        data.insert_obj("status", & self.status);
        data.insert_obj("level", & self.level);
        data.insert_obj("age", & self.age);
        data.insert_obj("birthday", & self.birthday);
        data.insert_obj("gender", & self.gender);
        data.insert_obj("token", & self.url_token); data
    }
} impl FromAkitaValue for SysUser
{
    fn from_value_opt(data : & AkitaValue) -> std :: result :: Result
    < Self, AkitaDataError >
    {
        use Converter; let mut data = SysUser
    {
        id : match data.get_obj("id")
        {
            Ok(v) => v, Err(err) =>
            {
                tracing :: error!
                ("err to get_obj:{}-{}", "id", err.to_string()); 0
            }
        }, pk : match data.get_obj("pk")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "pk", err.to_string()); String ::
        default()
        }
    }, user_type : match data.get_obj("user_type")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "user_type", err.to_string()); None
        }
    }, name : match data.get_obj("name")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "name", err.to_string()); None
        }
    }, headline : match data.get_obj("headline")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "headline", err.to_string()); None
        }
    }, tenant_id : match data.get_obj("tenant_id")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "tenant_id", err.to_string()); 0
        }
    }, status : match data.get_obj("status")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "status", err.to_string()); 0
        }
    }, level : match data.get_obj("level")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "level", err.to_string()); 0
        }
    }, age : match data.get_obj("age")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "age", err.to_string()); None
        }
    }, birthday : match data.get_obj("birthday")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "birthday", err.to_string()); None
        }
    }, gender : match data.get_obj("gender")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "gender", err.to_string()); 0
        }
    }, is_org : false, url_token : match data.get_obj("token")
    {
        Ok(v) => v, Err(err) =>
        {
            tracing :: error!
            ("err to get_obj:{}-{}", "token", err.to_string()); String
        :: default()
        }
    },
    }; Ok(data)
    }
} impl GetTableName for SysUser
{
    fn table_name() -> TableName
    {
        TableName
        {
            name : "t_system_user".to_string(), schema : None, alias :
        "SysUser".to_lowercase().into(), ignore_interceptors : std ::
        collections :: HashSet :: new(),
        }
    }
} impl GetTableName for & SysUser
{
    fn table_name() -> TableName
    {
        TableName
        {
            name : "t_system_user".to_string(), schema : None, alias :
        "SysUser".to_lowercase().into(), ignore_interceptors : std ::
        collections :: HashSet :: new(),
        }
    }
} impl GetFields for SysUser
{
    fn fields() -> Vec < FieldName >
    {
        vec!
        [FieldName
         {
             name : "id".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "id".to_string().into(), field_type : FieldType ::
         TableId(IdentifierType :: None), fill : None, select :
         true, exist : true,
         }, FieldName
         {
             name : "pk".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "pk".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "user_type".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "user_type".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "name".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "name".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "headline".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "headline".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "tenant_id".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "tenant_id".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "status".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "status".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "level".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "level".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "age".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "age".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "birthday".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "birthday".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "gender".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "gender".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "is_org".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "is_org".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : false,
         }, FieldName
         {
             name : "url_token".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "token".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         },]
    }
} impl GetFields for & SysUser
{
    fn fields() -> Vec < FieldName >
    {
        vec!
        [FieldName
         {
             name : "id".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "id".to_string().into(), field_type : FieldType ::
         TableId(IdentifierType :: None), fill : None, select :
         true, exist : true,
         }, FieldName
         {
             name : "pk".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "pk".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "user_type".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "user_type".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "name".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "name".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "headline".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "headline".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "tenant_id".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "tenant_id".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "status".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "status".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "level".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "level".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "age".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "age".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "birthday".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "birthday".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "gender".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "gender".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         }, FieldName
         {
             name : "is_org".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "is_org".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : false,
         }, FieldName
         {
             name : "url_token".to_string(), table :
         "t_system_user".to_string().into(), alias :
         "token".to_string().into(), field_type : FieldType ::
         TableField, fill : None, select : true, exist : true,
         },]
    }
} impl SysUser
{
    pub fn id() -> String { "id".to_string() }
    pub fn pk() -> String
    { "pk".to_string() }
    pub fn user_type() -> String
    { "user_type".to_string() }
    pub fn name() -> String { "name".to_string() }
    pub fn headline() -> String { "headline".to_string() }
    pub fn tenant_id()
        -> String { "tenant_id".to_string() }
    pub fn status() -> String
    { "status".to_string() }
    pub fn level() -> String { "level".to_string() }
    pub fn age() -> String { "age".to_string() }
    pub fn birthday() -> String
    { "birthday".to_string() }
    pub fn gender() -> String
    { "gender".to_string() }
    pub fn url_token() -> String
    { "token".to_string() }
    #[doc = r" 获取主键字段名"]
    pub fn
    primary_key_field() -> String { "id".to_string() }
}

pub fn create_test_akita_cfg() -> AkitaConfig {
    AkitaConfig::default()
        .max_size(10)
        .connection_timeout(Duration::from_secs(2))
        .idle_timeout(Duration::from_secs(30)) // 设置空闲超时
        .platform(DriverType::MySQL)
        .password("password")
        .username("root")
        .port(3306)
        .database("test")
        .hostname("127.0.0.1")
}

pub fn create_test_akita() -> Result<Akita, AkitaError> {
    let cfg = create_test_akita_cfg();
    Akita::new(cfg).map_err(|e| e.into())
}


pub fn create_test_user() -> SysUser {
    SysUser {
        id: 0,
        pk: Uuid::new_v4().simple().to_string(),
        user_type: Some("super".to_string()),
        name: Some(format!("NAME{}", Uuid::new_v4().simple().to_string())),
        headline: Some(NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
        tenant_id: 0,
        status: 1,
        level: 0,
        age: Some(18),
        birthday: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        gender: 1,
        is_org: false,
        url_token: "test_token".to_string(),
    }
}

pub fn create_bench_akita() -> Akita {
    create_test_akita().expect("Failed to create Akita instance for benchmarking")
}