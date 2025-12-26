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
use crate::interceptor::{InterceptorConfig, InterceptorConfigItem};
use akita_core::{OperationType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use crate::errors::{AkitaError, Result};
use crate::interceptor::blocking::{AkitaInterceptor, InterceptorChain};

/// Interceptor builder
pub struct InterceptorBuilder {
    interceptors: HashMap<String, (Arc<dyn AkitaInterceptor>, InterceptorConfigItem)>,
    chain_config: InterceptorConfig,
}

impl Default for InterceptorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InterceptorBuilder {
    pub fn new() -> Self {
        Self {
            interceptors: HashMap::new(),
            chain_config: InterceptorConfig::default(),
        }
    }

    /// Set up the interceptor chain configuration
    pub fn with_chain_config(mut self, config: InterceptorConfig) -> Self {
        self.chain_config = config;
        self
    }

    /// Register Interceptor - Accept Arc<dyn AkitaInterceptor> directly
    pub fn register(mut self, interceptor: Arc<dyn AkitaInterceptor>) -> Self {
        let name = interceptor.name().to_string();

        let config_item = InterceptorConfigItem {
            enabled: false, // 默认不启用
            order: interceptor.order(),
            ignored_tables: HashSet::new(),
            supported_operations: HashSet::new(),
        };

        self.interceptors.insert(name, (interceptor, config_item));
        self
    }

    /// Register an interceptor instance - a convenient way
    pub fn register_instance<I>(self, interceptor: I) -> Self
    where
        I: AkitaInterceptor + 'static,
    {
        self.register(Arc::new(interceptor))
    }

    /// Enable the blocker
    pub fn enable(mut self, name: &str) -> Result<Self> {
        if let Some((_, config)) = self.interceptors.get_mut(name) {
            config.enabled = true;
            Ok(self)
        } else {
            Err(AkitaError::InterceptorError(format!(
                "Interceptor '{}' not found", name
            )))
        }
    }

    /// Disable the interceptor
    pub fn disable(mut self, name: &str) -> Result<Self> {
        if let Some((_, config)) = self.interceptors.get_mut(name) {
            config.enabled = false;
            Ok(self)
        } else {
            Err(AkitaError::InterceptorError(format!(
                "Interceptor '{}' not found", name
            )))
        }
    }

    /// Set the interceptor order
    pub fn with_order(mut self, name: &str, order: i32) -> Result<Self> {
        if let Some((_, config)) = self.interceptors.get_mut(name) {
            config.order = order;
            Ok(self)
        } else {
            Err(AkitaError::InterceptorError(format!(
                "Interceptor '{}' not found", name
            )))
        }
    }

    /// Ignore table
    pub fn ignore_table(mut self, name: &str, table: &str) -> Result<Self> {
        if let Some((_, config)) = self.interceptors.get_mut(name) {
            config.ignored_tables.insert(table.to_string());
            Ok(self)
        } else {
            Err(AkitaError::InterceptorError(format!(
                "Interceptor '{}' not found", name
            )))
        }
    }

    /// Restrict the type of action
    pub fn with_operations(mut self, name: &str, operations: &[OperationType]) -> Result<Self> {
        if let Some((_, config)) = self.interceptors.get_mut(name) {
            config.supported_operations = operations.iter().cloned().collect();
            Ok(self)
        } else {
            Err(AkitaError::InterceptorError(format!(
                "Interceptor '{}' not found", name
            )))
        }
    }

    /// Build an interceptor chain
    pub fn build(self) -> Result<InterceptorChain> {
        let mut chain = InterceptorChain::with_config(self.chain_config);

        // Collect the enabled interceptors
        let mut enabled_interceptors: Vec<(Arc<dyn AkitaInterceptor>, InterceptorConfigItem)> = self.interceptors
            .into_iter()
            .filter(|(_, (_, config))| config.enabled)
            .map(|(_, (interceptor, config))| (interceptor, config))
            .collect();

        // Sort in order
        enabled_interceptors.sort_by(|(_, a), (_, b)| a.order.cmp(&b.order));

        // ADDED TO THE CHAIN
        for (interceptor, config) in enabled_interceptors {
            chain.add_interceptor(interceptor);
        }

        Ok(chain)
    }

    /// Get all registered blocker names
    pub fn registered_interceptors(&self) -> Vec<&str> {
        self.interceptors.keys().map(|s| s.as_str()).collect()
    }

    /// Check if the blocker is registered
    pub fn is_registered(&self, name: &str) -> bool {
        self.interceptors.contains_key(name)
    }

    /// Check if the blocker is enabled
    pub fn is_enabled(&self, name: &str) -> bool {
        self.interceptors
            .get(name)
            .map(|(_, config)| config.enabled)
            .unwrap_or(false)
    }
}


impl InterceptorBuilder {
    /// Development environment configuration
    pub fn development() -> Self {
        Self::new()
            .with_chain_config(InterceptorConfig {
                enable_async: true,
                enable_metrics: true,
                enable_tracing: true,
                max_interceptor_depth: 20,
                timeout_ms: 10000,
            })
    }

    /// Production environment configuration
    pub fn production() -> Self {
        Self::new()
            .with_chain_config(InterceptorConfig {
                enable_async: true,
                enable_metrics: true,
                enable_tracing: false,
                max_interceptor_depth: 10,
                timeout_ms: 5000,
            })
    }

    /// High security configuration
    pub fn high_security() -> Self {
        Self::production()
            .with_chain_config(InterceptorConfig {
                enable_async: true,
                enable_metrics: true,
                enable_tracing: true,
                max_interceptor_depth: 15,
                timeout_ms: 8000,
            })
    }
}