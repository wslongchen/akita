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

//! Shared interceptor utilities and base trait for both sync and async interceptors.

use crate::comm::ExecuteContext;
use crate::interceptor::{InterceptorConfig, InterceptorConfigItem};
use crate::interceptor_err;
use akita_core::{InterceptorType, OperationType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Base trait for interceptors, containing non-async methods shared by both
/// sync and async interceptor implementations.
pub trait InterceptorBase: Send + Sync {
    /// Interceptor name
    fn name(&self) -> &'static str;

    /// Get the interceptor type
    fn interceptor_type(&self) -> InterceptorType;

    /// Execution order (the smaller the value, the first execution)
    fn order(&self) -> i32 {
        0
    }

    /// Whether the action type is supported
    fn supports_operation(&self, operation: &OperationType) -> bool {
        match operation {
            OperationType::Call => false,
            _ => true,
        }
    }

    /// Whether the table is ignored
    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }
}

/// Helper function to sort interceptors by order
pub fn sort_interceptors_by_order(interceptors: &mut [Arc<dyn InterceptorBase>]) {
    interceptors.sort_by(|a, b| a.order().cmp(&b.order()));
}

/// Helper function to check if an interceptor should be skipped
pub fn should_skip_interceptor(interceptor: &dyn InterceptorBase, ctx: &ExecuteContext) -> bool {
    // Check if the table is ignored
    if interceptor.will_ignore_table(&ctx.table_info().name) {
        return true;
    }

    // Check if the action is supported
    if !interceptor.supports_operation(&ctx.operation_type()) {
        return true;
    }

    // Check if the interceptor is skipped
    if ctx
        .skip_next_interceptors()
        .contains(&interceptor.interceptor_type())
    {
        return true;
    }

    false
}

/// Helper function to check depth limit
pub fn check_depth_limit(
    depth: &mut usize,
    config: &InterceptorConfig,
) -> crate::errors::Result<()> {
    *depth += 1;
    if *depth > config.max_interceptor_depth {
        return Err(interceptor_err!("Interceptor chain too deep".to_string()));
    }
    Ok(())
}

/// Helper struct for interceptor registration in builders
pub struct InterceptorEntry {
    pub interceptor: Arc<dyn InterceptorBase>,
    pub config: InterceptorConfigItem,
}

/// Helper functions for builder operations
pub mod builder_helpers {
    use super::*;

    /// Register an interceptor in the map
    pub fn register_interceptor(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        interceptor: Arc<dyn InterceptorBase>,
    ) {
        let name = interceptor.name().to_string();
        let config_item = InterceptorConfigItem {
            enabled: false,
            order: interceptor.order(),
            ignored_tables: HashSet::new(),
            supported_operations: HashSet::new(),
        };
        interceptors.insert(
            name,
            InterceptorEntry {
                interceptor,
                config: config_item,
            },
        );
    }

    /// Enable an interceptor by name
    pub fn enable_interceptor(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        name: &str,
    ) -> crate::errors::Result<()> {
        if let Some(entry) = interceptors.get_mut(name) {
            entry.config.enabled = true;
            Ok(())
        } else {
            Err(interceptor_err!(format!(
                "Interceptor '{}' not found",
                name
            )))
        }
    }

    /// Disable an interceptor by name
    pub fn disable_interceptor(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        name: &str,
    ) -> crate::errors::Result<()> {
        if let Some(entry) = interceptors.get_mut(name) {
            entry.config.enabled = false;
            Ok(())
        } else {
            Err(interceptor_err!(format!(
                "Interceptor '{}' not found",
                name
            )))
        }
    }

    /// Set order for an interceptor
    pub fn set_interceptor_order(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        name: &str,
        order: i32,
    ) -> crate::errors::Result<()> {
        if let Some(entry) = interceptors.get_mut(name) {
            entry.config.order = order;
            Ok(())
        } else {
            Err(interceptor_err!(format!(
                "Interceptor '{}' not found",
                name
            )))
        }
    }

    /// Add a table to ignore for an interceptor
    pub fn ignore_table(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        name: &str,
        table: &str,
    ) -> crate::errors::Result<()> {
        if let Some(entry) = interceptors.get_mut(name) {
            entry.config.ignored_tables.insert(table.to_string());
            Ok(())
        } else {
            Err(interceptor_err!(format!(
                "Interceptor '{}' not found",
                name
            )))
        }
    }

    /// Set supported operations for an interceptor
    pub fn set_operations(
        interceptors: &mut HashMap<String, InterceptorEntry>,
        name: &str,
        operations: &[OperationType],
    ) -> crate::errors::Result<()> {
        if let Some(entry) = interceptors.get_mut(name) {
            entry.config.supported_operations = operations.iter().cloned().collect();
            Ok(())
        } else {
            Err(interceptor_err!(format!(
                "Interceptor '{}' not found",
                name
            )))
        }
    }

    /// Get enabled interceptors sorted by order
    pub fn get_enabled_interceptors(
        interceptors: &HashMap<String, InterceptorEntry>,
    ) -> Vec<(Arc<dyn InterceptorBase>, InterceptorConfigItem)> {
        let mut enabled: Vec<_> = interceptors
            .iter()
            .filter(|(_, entry)| entry.config.enabled)
            .map(|(_, entry)| (entry.interceptor.clone(), entry.config.clone()))
            .collect();

        enabled.sort_by(|(_, a), (_, b)| a.order.cmp(&b.order));
        enabled
    }

    /// Get registered interceptor names
    pub fn registered_interceptors(interceptors: &HashMap<String, InterceptorEntry>) -> Vec<&str> {
        interceptors.keys().map(|s| s.as_str()).collect()
    }

    /// Check if an interceptor is registered
    pub fn is_registered(interceptors: &HashMap<String, InterceptorEntry>, name: &str) -> bool {
        interceptors.contains_key(name)
    }

    /// Check if an interceptor is enabled
    pub fn is_enabled(interceptors: &HashMap<String, InterceptorEntry>, name: &str) -> bool {
        interceptors
            .get(name)
            .map(|entry| entry.config.enabled)
            .unwrap_or(false)
    }
}

/// Common interceptor chain configuration presets
pub mod presets {
    use super::InterceptorConfig;

    pub fn development() -> InterceptorConfig {
        InterceptorConfig {
            enable_async: true,
            enable_metrics: true,
            enable_tracing: true,
            max_interceptor_depth: 20,
            timeout_ms: 10000,
        }
    }

    pub fn production() -> InterceptorConfig {
        InterceptorConfig {
            enable_async: true,
            enable_metrics: true,
            enable_tracing: false,
            max_interceptor_depth: 10,
            timeout_ms: 5000,
        }
    }

    pub fn high_security() -> InterceptorConfig {
        InterceptorConfig {
            enable_async: true,
            enable_metrics: true,
            enable_tracing: true,
            max_interceptor_depth: 15,
            timeout_ms: 8000,
        }
    }
}
