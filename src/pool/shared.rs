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

/// Database driver types supported by the connection pool.
///
/// Each variant corresponds to a specific database backend that Akita can
/// connect to through the pool abstraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolDriverType {
    /// MySQL / MariaDB database driver.
    MySQL,
    /// PostgreSQL database driver.
    PostgreSQL,
    /// SQLite embedded database driver.
    SQLite,
    /// Oracle database driver.
    Oracle,
    /// Microsoft SQL Server database driver.
    MSSQL,
}

impl PoolDriverType {
    /// Get a human-readable display name for the driver type.
    ///
    /// # Returns
    /// A static string slice with the canonical name of the database driver
    /// (e.g., `"MySQL"`, `"PostgreSQL"`, `"SQL Server"`).
    ///
    /// # Example
    /// ```ignore
    /// let driver = PoolDriverType::PostgreSQL;
    /// assert_eq!(driver.display_name(), "PostgreSQL");
    /// ```
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
