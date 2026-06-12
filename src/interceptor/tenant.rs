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

//! Tenant Line Interceptor - Automatically injects tenant ID conditions.
//!
//! This interceptor automatically adds WHERE tenant_id = ? conditions to
//! queries and fills tenant_id on INSERT operations. This provides row-level
//! multi-tenancy support.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::tenant::{TenantLineInterceptor, TenantConfig};
//!
//! let config = TenantConfig {
//!     column: "tenant_id".to_string(),
//!     tenant_id: AkitaValue::Text("tenant_001".to_string()),
//!     ignore_tables: vec!["sys_config".to_string()].into_iter().collect(),
//! };
//!
//! let interceptor = TenantLineInterceptor::new(config);
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::InterceptorType;
use akita_core::AkitaValue;
use std::collections::HashSet;

/// Configuration for tenant line behavior.
#[derive(Debug, Clone)]
pub struct TenantConfig {
    /// The column name used for tenant ID (default: "tenant_id")
    pub column: String,
    /// The current tenant ID value
    pub tenant_id: AkitaValue,
    /// Tables to exclude from tenant filtering
    pub ignore_tables: HashSet<String>,
}

impl TenantConfig {
    /// Create a new tenant config.
    pub fn new(column: &str, tenant_id: AkitaValue) -> Self {
        Self {
            column: column.to_string(),
            tenant_id,
            ignore_tables: HashSet::new(),
        }
    }

    /// Add a table to the ignore list.
    pub fn ignore_table(mut self, table: &str) -> Self {
        self.ignore_tables.insert(table.to_string());
        self
    }
}

/// Tenant Line Interceptor - Automatically injects tenant ID conditions.
///
/// This interceptor:
/// - Adds WHERE tenant_id = ? to SELECT/UPDATE/DELETE queries
/// - Fills tenant_id on INSERT operations
/// - Supports configurable column name and ignore tables
pub struct TenantLineInterceptor {
    config: TenantConfig,
}

impl TenantLineInterceptor {
    /// Create a new tenant line interceptor with the given configuration.
    ///
    /// # Arguments
    /// * `config` - The tenant configuration specifying the tenant column, ID, and ignored tables.
    ///
    /// # Returns
    /// A new `TenantLineInterceptor` using the provided configuration.
    ///
    /// # Example
    /// ```ignore
    /// use akita::interceptor::tenant::{TenantLineInterceptor, TenantConfig};
    /// use akita_core::AkitaValue;
    ///
    /// let config = TenantConfig::new("tenant_id", AkitaValue::Text("t001".to_string()));
    /// let interceptor = TenantLineInterceptor::new(config);
    /// ```
    pub fn new(config: TenantConfig) -> Self {
        Self { config }
    }

    /// Get the configuration.
    pub fn config(&self) -> &TenantConfig {
        &self.config
    }

    /// Check if a table should be ignored.
    pub(crate) fn should_ignore_table(&self, table: &str) -> bool {
        self.config.ignore_tables.contains(table)
    }
}

impl InterceptorBase for TenantLineInterceptor {
    fn name(&self) -> &'static str {
        "tenant_line"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Tenant
    }

    fn order(&self) -> i32 {
        15 // Execute after field fill, before soft delete
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
impl crate::interceptor::blocking::AkitaInterceptor for TenantLineInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let table = ctx.table_info().name.clone();

        // Skip if table is in ignore list
        if self.should_ignore_table(&table) {
            return Ok(());
        }

        // Store tenant info in metadata for the driver to use
        ctx.set_metadata("tenant_column".to_string(), self.config.column.clone());
        ctx.set_metadata("tenant_id".to_string(), self.config.tenant_id.to_string());
        ctx.set_metadata("tenant_active".to_string(), "true".to_string());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_config() {
        let config = TenantConfig::new("tenant_id", AkitaValue::Text("tenant_001".to_string()));
        assert_eq!(config.column, "tenant_id");
        assert_eq!(config.tenant_id, AkitaValue::Text("tenant_001".to_string()));
        assert!(config.ignore_tables.is_empty());
    }

    #[test]
    fn test_tenant_config_with_ignore() {
        let config = TenantConfig::new("tenant_id", AkitaValue::Text("tenant_001".to_string()))
            .ignore_table("sys_config")
            .ignore_table("sys_user");

        assert!(config.ignore_tables.contains("sys_config"));
        assert!(config.ignore_tables.contains("sys_user"));
    }

    #[test]
    fn test_tenant_line_interceptor() {
        let config = TenantConfig::new("tenant_id", AkitaValue::Text("tenant_001".to_string()));
        let interceptor = TenantLineInterceptor::new(config);

        assert_eq!(interceptor.name(), "tenant_line");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::Tenant);
        assert_eq!(interceptor.order(), 15);
    }

    #[test]
    fn test_should_ignore_table() {
        let config = TenantConfig::new("tenant_id", AkitaValue::Text("tenant_001".to_string()))
            .ignore_table("sys_config");

        let interceptor = TenantLineInterceptor::new(config);

        assert!(interceptor.should_ignore_table("sys_config"));
        assert!(!interceptor.should_ignore_table("users"));
    }
}
