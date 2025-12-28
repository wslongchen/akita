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

#[doc(inline)]
pub use crate::errors::*;
pub use crate::xml::*;
pub use crate::comm::*;
pub use crate::converter::*;
pub use crate::key::*;
pub use crate::config::*;
pub use crate::ext::Request;
pub use crate::driver::DriverType;
pub use crate::interceptor::LoggingInterceptor;
pub use crate::mapper::IPage;
#[doc(inline)]
pub use chrono::{Local, NaiveDate, NaiveDateTime};

// re-export
pub use akita_core::*;
pub use akita_derive::{query,insert, update,select_one, ToValue, FromValue, sql, sql_xml, delete, list,AkitaEnum};

cfg_if! {if #[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync", 
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async", 
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))] {
    pub use crate::transaction::blocking::AkitaTransaction;
    pub use crate::interceptor::blocking::{InterceptorChain, InterceptorBuilder, AkitaInterceptor};
    pub use crate::driver::blocking::DbDriver;
    pub use crate::repository::EntityRepository;
    pub use crate::pool::blocking::{DBPool, DBPoolWrapper};
    pub use crate::core::blocking::{Akita as AkitaSync, UpdateBuilder, QueryBuilder};
    pub use crate::mapper::blocking::AkitaMapper;
    pub use crate::ext::blocking::{IService, Mapper};
    pub use akita_derive::Entity;
}}

cfg_if! {if #[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))] {
    pub use crate::transaction::non_blocking::AsyncAkitaTransaction;
    pub use crate::interceptor::non_blocking::{AsyncInterceptorChain, AsyncInterceptorBuilder, AsyncAkitaInterceptor};
    pub use crate::driver::non_blocking::{AsyncDbDriver};
    pub use crate::pool::non_blocking::{AsyncDBPool, AsyncDBPoolWrapper};
    pub use crate::core::non_blocking::{AkitaAsync, AsyncUpdateBuilder, AsyncQueryBuilder};
    pub use crate::mapper::non_blocking::AsyncAkitaMapper;
    pub use crate::ext::non_blocking::{AsyncService, AsyncMapper};
    pub use akita_derive::AsyncEntity;
}}

// ==================== Safe type aliases (collision avoidance) ====================

// These aliases are defined only when synchronization is enabled and asynchrony is not

cfg_if! {if #[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync", 
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async", 
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))] {
    /// Synchronize AKita exports
    pub type Akita = AkitaSync;
}}



// These aliases are defined only when asynchrony is enabled and synchronization is not
cfg_if! {if #[cfg(all(
            any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "oracle-async",
                feature = "mssql-async"
            ),
            not(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "mssql-sync",
                feature = "oracle-sync"
            ))
        ))] {
    pub use AkitaAsync as Akita;
    pub use AsyncDBPool as DBPool;
    pub use AsyncDbDriver as DbDriver;
    pub use AsyncUpdateBuilder as UpdateBuilder;
    pub use AsyncQueryBuilder as QueryBuilder;
    pub use AsyncInterceptorChain as InterceptorChain;
    pub use AsyncAkitaTransaction as AkitaTransaction;
    pub use AsyncAkitaMapper as AkitaMapper;
    pub use AsyncAkitaInterceptor as AkitaInterceptor;
}}
