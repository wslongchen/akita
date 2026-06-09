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

//! Shared pool utilities for both sync and async implementations.
//!
//! This module contains common types and helper functions used by both
//! `DBPoolWrapper` (blocking) and `AsyncDBPoolWrapper` (non-blocking).

/// Database driver types supported by the pool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolDriverType {
    MySQL,
    PostgreSQL,
    SQLite,
    Oracle,
    MSSQL,
}

impl PoolDriverType {
    /// Get a human-readable name for the driver type
    pub fn display_name(&self) -> &'static str {
        match self {
            PoolDriverType::MySQL => "MySQL",
            PoolDriverType::PostgreSQL => "PostgreSQL",
            PoolDriverType::SQLite => "SQLite",
            PoolDriverType::Oracle => "Oracle",
            PoolDriverType::MSSQL => "SQL Server",
        }
    }
}
