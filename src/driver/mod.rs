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
use akita_core::cfg_if;

cfg_if! {if #[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync", 
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))] {
    pub mod blocking;
}}

cfg_if! {if #[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async", 
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))] {
    pub mod non_blocking;
}}

#[derive(Debug, Clone, PartialEq)]
pub enum DriverType {
    MySQL,
    Sqlite,
    Oracle,
    Postgres,
    Mssql,
    Unsupported,
}

impl Default for DriverType {
    fn default() -> Self {
        DriverType::Unsupported
    }
}