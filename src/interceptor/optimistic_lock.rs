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

//! Optimistic Locking Interceptor - Automatically adds version checks.
//!
//! This interceptor automatically adds WHERE version = ? conditions to UPDATE
//! statements and increments the version field. This prevents lost updates in
//! concurrent scenarios.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::optimistic_lock::{OptimisticLockerInterceptor, OptimisticLockConfig};
//!
//! let config = OptimisticLockConfig {
//!     column: "version".to_string(),
//! };
//!
//! let interceptor = OptimisticLockerInterceptor::new(config);
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};
use std::collections::HashSet;

/// Configuration for optimistic locking behavior.
#[derive(Debug, Clone)]
pub struct OptimisticLockConfig {
    /// The column name used for version tracking (default: "version")
    pub column: String,
    /// Tables to exclude from optimistic locking
    pub ignore_tables: HashSet<String>,
}

impl Default for OptimisticLockConfig {
    fn default() -> Self {
        Self {
            column: "version".to_string(),
            ignore_tables: HashSet::new(),
        }
    }
}

impl OptimisticLockConfig {
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

/// Optimistic Locking Interceptor - Automatically adds version checks.
///
/// This interceptor:
/// - Adds WHERE version = ? conditions to UPDATE statements
/// - Increments the version field in SET clause
/// - Supports configurable column name
pub struct OptimisticLockerInterceptor {
    config: OptimisticLockConfig,
}

impl OptimisticLockerInterceptor {
    /// Create a new optimistic lock interceptor with the given configuration.
    ///
    /// # Arguments
    /// * `config` - The optimistic lock configuration specifying the version column and ignored tables.
    ///
    /// # Returns
    /// A new `OptimisticLockerInterceptor` using the provided configuration.
    ///
    /// # Example
    /// ```ignore
    /// use akita::interceptor::optimistic_lock::{OptimisticLockerInterceptor, OptimisticLockConfig};
    ///
    /// let config = OptimisticLockConfig::default().with_column("lock_version");
    /// let interceptor = OptimisticLockerInterceptor::new(config);
    /// ```
    pub fn new(config: OptimisticLockConfig) -> Self {
        Self { config }
    }

    /// Create a new interceptor with default configuration.
    pub fn with_default() -> Self {
        Self::new(OptimisticLockConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &OptimisticLockConfig {
        &self.config
    }

    /// Check if a table should be ignored.
    pub(crate) fn should_ignore_table(&self, table: &str) -> bool {
        self.config.ignore_tables.contains(table)
    }
}

impl InterceptorBase for OptimisticLockerInterceptor {
    fn name(&self) -> &'static str {
        "optimistic_lock"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::OptimisticLock
    }

    fn order(&self) -> i32 {
        25 // Execute after soft delete, before pagination
    }

    fn will_ignore_table(&self, table_name: &str) -> bool {
        self.should_ignore_table(table_name)
    }

    fn supports_operation(&self, operation: &OperationType) -> bool {
        matches!(operation, OperationType::Update)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for OptimisticLockerInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let table = ctx.table_info().name.clone();

        // Skip if table is in ignore list
        if self.should_ignore_table(&table) {
            return Ok(());
        }

        // Only apply to UPDATE operations
        if !matches!(ctx.operation_type(), OperationType::Update) {
            return Ok(());
        }

        // Store optimistic lock info in metadata for the driver to use
        ctx.set_metadata(
            "optimistic_lock_column".to_string(),
            self.config.column.clone(),
        );
        ctx.set_metadata("optimistic_lock_active".to_string(), "true".to_string());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimistic_lock_config_default() {
        let config = OptimisticLockConfig::default();
        assert_eq!(config.column, "version");
        assert!(config.ignore_tables.is_empty());
    }

    #[test]
    fn test_optimistic_lock_config_custom() {
        let config = OptimisticLockConfig::default()
            .with_column("lock_version")
            .ignore_table("logs");

        assert_eq!(config.column, "lock_version");
        assert!(config.ignore_tables.contains("logs"));
    }

    #[test]
    fn test_optimistic_lock_interceptor() {
        let interceptor = OptimisticLockerInterceptor::with_default();
        assert_eq!(interceptor.name(), "optimistic_lock");
        assert_eq!(
            interceptor.interceptor_type(),
            InterceptorType::OptimisticLock
        );
        assert_eq!(interceptor.order(), 25);
    }

    #[test]
    fn test_should_ignore_table() {
        let interceptor =
            OptimisticLockerInterceptor::new(OptimisticLockConfig::default().ignore_table("logs"));

        assert!(interceptor.should_ignore_table("logs"));
        assert!(!interceptor.should_ignore_table("users"));
    }

    #[test]
    fn test_supports_operation() {
        let interceptor = OptimisticLockerInterceptor::with_default();
        assert!(interceptor.supports_operation(&OperationType::Update));
        assert!(!interceptor.supports_operation(&OperationType::Select));
        assert!(!interceptor
            .supports_operation(&OperationType::Insert(akita_core::InsertType::SingleInsert)));
        assert!(!interceptor.supports_operation(&OperationType::Delete));
    }
}
