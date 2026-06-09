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

//! Soft Delete Interceptor - Automatically rewrites DELETE to UPDATE.
//!
//! This interceptor automatically rewrites DELETE statements to UPDATE statements
//! that set a "deleted" flag instead of actually deleting rows. It also adds
//! WHERE conditions to exclude soft-deleted rows from SELECT queries.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::soft_delete::{SoftDeleteInterceptor, SoftDeleteConfig};
//!
//! let config = SoftDeleteConfig {
//!     column: "deleted".to_string(),
//!     deleted_value: 1.into(),
//!     not_deleted_value: 0.into(),
//! };
//!
//! let interceptor = SoftDeleteInterceptor::new(config);
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};
use akita_core::AkitaValue;
use std::collections::HashSet;

/// Configuration for soft delete behavior.
#[derive(Debug, Clone)]
pub struct SoftDeleteConfig {
    /// The column name used for soft delete flag (default: "deleted")
    pub column: String,
    /// The value indicating a deleted record (default: 1)
    pub deleted_value: AkitaValue,
    /// The value indicating a non-deleted record (default: 0)
    pub not_deleted_value: AkitaValue,
    /// Tables to exclude from soft delete behavior
    pub ignore_tables: HashSet<String>,
}

impl Default for SoftDeleteConfig {
    fn default() -> Self {
        Self {
            column: "deleted".to_string(),
            deleted_value: AkitaValue::Int(1),
            not_deleted_value: AkitaValue::Int(0),
            ignore_tables: HashSet::new(),
        }
    }
}

impl SoftDeleteConfig {
    /// Create a new config with custom column name.
    pub fn with_column(mut self, column: &str) -> Self {
        self.column = column.to_string();
        self
    }

    /// Add a table to the ignore list.
    pub fn ignore_table(mut self, table: &str) -> Self {
        self.ignore_tables.insert(table.to_string());
        self
    }
}

/// Soft Delete Interceptor - Automatically rewrites DELETE to UPDATE.
///
/// This interceptor:
/// - Rewrites DELETE statements to UPDATE with deleted flag
/// - Adds WHERE conditions to exclude soft-deleted rows from SELECT/UPDATE
/// - Supports configurable column name and values
pub struct SoftDeleteInterceptor {
    config: SoftDeleteConfig,
}

impl SoftDeleteInterceptor {
    pub fn new(config: SoftDeleteConfig) -> Self {
        Self { config }
    }

    /// Create a new interceptor with default configuration.
    pub fn with_default() -> Self {
        Self::new(SoftDeleteConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &SoftDeleteConfig {
        &self.config
    }

    /// Check if a table should be ignored.
    pub(crate) fn should_ignore_table(&self, table: &str) -> bool {
        self.config.ignore_tables.contains(table)
    }
}

impl InterceptorBase for SoftDeleteInterceptor {
    fn name(&self) -> &'static str {
        "soft_delete"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::SoftDelete
    }

    fn order(&self) -> i32 {
        20 // Execute after field fill
    }

    fn will_ignore_table(&self, table_name: &str) -> bool {
        self.should_ignore_table(table_name)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for SoftDeleteInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let table = ctx.table_info().name.clone();

        // Skip if table is in ignore list
        if self.should_ignore_table(&table) {
            return Ok(());
        }

        match ctx.operation_type() {
            OperationType::Delete => {
                // Rewrite DELETE to UPDATE
                // Store the soft delete info in metadata for the driver to use
                ctx.set_metadata("soft_delete_rewrite".to_string(), "true".to_string());
                ctx.set_metadata("soft_delete_column".to_string(), self.config.column.clone());
            }
            OperationType::Select => {
                // Add WHERE deleted = 0 condition
                ctx.set_metadata("soft_delete_filter".to_string(), "true".to_string());
                ctx.set_metadata("soft_delete_column".to_string(), self.config.column.clone());
            }
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_soft_delete_config_default() {
        let config = SoftDeleteConfig::default();
        assert_eq!(config.column, "deleted");
        assert_eq!(config.deleted_value, AkitaValue::Int(1));
        assert_eq!(config.not_deleted_value, AkitaValue::Int(0));
    }

    #[test]
    fn test_soft_delete_config_custom() {
        let config = SoftDeleteConfig::default()
            .with_column("is_deleted")
            .ignore_table("logs");

        assert_eq!(config.column, "is_deleted");
        assert!(config.ignore_tables.contains("logs"));
    }

    #[test]
    fn test_soft_delete_interceptor() {
        let interceptor = SoftDeleteInterceptor::with_default();
        assert_eq!(interceptor.name(), "soft_delete");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::SoftDelete);
        assert_eq!(interceptor.order(), 20);
    }

    #[test]
    fn test_should_ignore_table() {
        let interceptor =
            SoftDeleteInterceptor::new(SoftDeleteConfig::default().ignore_table("logs"));

        assert!(interceptor.should_ignore_table("logs"));
        assert!(!interceptor.should_ignore_table("users"));
    }
}
