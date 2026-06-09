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
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::interceptor::non_blocking::AsyncAkitaInterceptor;
use crate::interceptor::shared::{
    check_depth_limit, should_skip_interceptor, sort_interceptors_by_order, InterceptorBase,
};
use crate::interceptor::*;
use crate::prelude::AkitaError;
use crate::prelude::Result;
use akita_core::InterceptorType;
use std::collections::HashSet;
use std::sync::Arc;

/// Asynchronous interceptor chain manager
#[derive(Clone)]
pub struct AsyncInterceptorChain {
    interceptors: Vec<Arc<dyn AsyncAkitaInterceptor>>,
    enabled_types: HashSet<InterceptorType>,
    config: InterceptorConfig,
}

impl Default for AsyncInterceptorChain {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncInterceptorChain {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
            enabled_types: HashSet::new(),
            config: InterceptorConfig::default(),
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            interceptors: self.interceptors.clone(),
            enabled_types: self.enabled_types.clone(),
            config: self.config.clone(),
        }
    }

    pub fn with_config(config: InterceptorConfig) -> Self {
        Self {
            interceptors: Vec::new(),
            enabled_types: HashSet::new(),
            config,
        }
    }

    /// Add interceptor - now accepted Arc<dyn AsyncAkitaInterceptor>
    pub fn add_interceptor(&mut self, interceptor: Arc<dyn AsyncAkitaInterceptor>) -> &mut Self {
        let interceptor_type = interceptor.interceptor_type();
        self.interceptors.push(interceptor);
        self.enabled_types.insert(interceptor_type);
        // Sort by order - convert to trait object for sorting
        let mut base_refs: Vec<Arc<dyn InterceptorBase>> = self
            .interceptors
            .iter()
            .map(|i| i.clone() as Arc<dyn InterceptorBase>)
            .collect();
        sort_interceptors_by_order(&mut base_refs);
        self
    }

    /// Perform a pre-interception
    pub async fn before_query(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let mut depth = 0;

        for interceptor in &self.interceptors {
            if ctx.stop_propagation {
                break;
            }

            // Check the depth limit
            check_depth_limit(&mut depth, &self.config)?;

            // Check if the interceptor should be skipped
            if should_skip_interceptor(interceptor.as_ref() as &dyn InterceptorBase, ctx) {
                continue;
            }

            // Perform a pre-interception
            interceptor.before_execute(ctx).await?;

            // Record the executed interceptors
            ctx.executed_interceptors_mut()
                .push(interceptor.interceptor_type());
        }
        Ok(())
    }

    /// Perform post-interception
    pub async fn after_query(
        &self,
        ctx: &mut ExecuteContext,
        result: &mut std::result::Result<ExecuteResult, AkitaError>,
    ) -> Result<()> {
        for interceptor in self.interceptors.iter().rev() {
            // Check if the interceptor should be skipped
            if should_skip_interceptor(interceptor.as_ref() as &dyn InterceptorBase, ctx) {
                continue;
            }

            interceptor.after_execute(ctx, result).await?;
        }
        Ok(())
    }

    /// Perform error interception
    pub async fn on_error(&self, ctx: &ExecuteContext, error: &mut AkitaError) -> Result<()> {
        for interceptor in self.interceptors.iter().rev() {
            // Check if the interceptor should be skipped
            if should_skip_interceptor(interceptor.as_ref() as &dyn InterceptorBase, ctx) {
                continue;
            }

            interceptor.on_error(ctx, error).await?;
        }
        Ok(())
    }

    /// Check if a certain type of interceptor is enabled
    pub fn is_interceptor_enabled(&self, interceptor_type: &InterceptorType) -> bool {
        self.enabled_types.contains(interceptor_type)
    }

    /// Get the number of interceptors
    pub fn len(&self) -> usize {
        self.interceptors.len()
    }

    /// Check if it is empty
    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }
}
