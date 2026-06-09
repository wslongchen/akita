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

//! Async implementations for all built-in interceptors.
//!
//! This module provides `AsyncAkitaInterceptor` implementations for all
//! built-in interceptors. Since most interceptors are purely metadata-based
//! (no actual I/O), the async versions are trivial wrappers.

use crate::comm::{ExecuteContext, ExecuteResult};
use crate::errors::{AkitaError, Result};
use crate::interceptor::non_blocking::AsyncAkitaInterceptor;
use crate::interceptor::*;
use async_trait::async_trait;

// ========== FieldFillInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for FieldFillInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let operation = ctx.operation_type();
        let fill_values = self.get_fill_values(&operation);

        if fill_values.is_empty() {
            return Ok(());
        }

        // Store fill values in metadata for later use by the driver
        for (field, value) in fill_values {
            ctx.set_metadata(format!("fill_{}", field), value.to_string());
        }

        Ok(())
    }
}

// ========== SoftDeleteInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for SoftDeleteInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let table = ctx.table_info().name.clone();

        // Skip if table is in ignore list
        if self.should_ignore_table(&table) {
            return Ok(());
        }

        match ctx.operation_type() {
            OperationType::Delete => {
                // Rewrite DELETE to UPDATE
                ctx.set_metadata("soft_delete_rewrite".to_string(), "true".to_string());
                ctx.set_metadata(
                    "soft_delete_column".to_string(),
                    self.config().column.clone(),
                );
            }
            OperationType::Select => {
                // Add WHERE deleted = 0 condition
                ctx.set_metadata("soft_delete_filter".to_string(), "true".to_string());
                ctx.set_metadata(
                    "soft_delete_column".to_string(),
                    self.config().column.clone(),
                );
            }
            _ => {}
        }

        Ok(())
    }
}

// ========== PaginationInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for PaginationInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Check if pagination is requested
        let pagination_json = ctx.get_metadata("pagination");

        if let Some(_pagination) = pagination_json {
            // Store normalized pagination info for the driver
            ctx.set_metadata("pagination_active".to_string(), "true".to_string());
        }

        Ok(())
    }
}

// ========== CursorPaginationInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for CursorPaginationInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Check if cursor pagination is requested
        let cursor_pagination_json = ctx.get_metadata("cursor_pagination");

        if let Some(_cursor_pagination) = cursor_pagination_json {
            // Store normalized pagination info for the driver
            ctx.set_metadata("cursor_pagination_active".to_string(), "true".to_string());
        }

        Ok(())
    }
}

// ========== OptimisticLockerInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for OptimisticLockerInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
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
            self.config().column.clone(),
        );
        ctx.set_metadata("optimistic_lock_active".to_string(), "true".to_string());

        Ok(())
    }
}

// ========== TenantLineInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for TenantLineInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let table = ctx.table_info().name.clone();

        // Skip if table is in ignore list
        if self.should_ignore_table(&table) {
            return Ok(());
        }

        // Store tenant info in metadata for the driver to use
        ctx.set_metadata("tenant_column".to_string(), self.config().column.clone());
        ctx.set_metadata("tenant_id".to_string(), self.config().tenant_id.to_string());
        ctx.set_metadata("tenant_active".to_string(), "true".to_string());

        Ok(())
    }
}

// ========== CacheInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for CacheInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let sql = ctx.final_sql();
        let key = self.generate_cache_key(sql);

        // Check cache for SELECT queries
        if matches!(ctx.operation_type(), OperationType::Select) {
            if let Some(cached) = self.get_from_cache(&key) {
                ctx.set_metadata("cache_hit".to_string(), "true".to_string());
                ctx.set_metadata("cache_key".to_string(), key);
                ctx.set_metadata(
                    "cached_data".to_string(),
                    String::from_utf8_lossy(&cached).to_string(),
                );
            } else {
                ctx.set_metadata("cache_hit".to_string(), "false".to_string());
                ctx.set_metadata("cache_key".to_string(), key);
            }
        } else {
            // For INSERT/UPDATE/DELETE, invalidate related cache entries
            ctx.set_metadata("cache_invalidate".to_string(), "true".to_string());
        }

        Ok(())
    }

    async fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        _result: &mut std::result::Result<ExecuteResult, AkitaError>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // Cache the result for SELECT queries
        if matches!(ctx.operation_type(), OperationType::Select) {
            let cache_hit = ctx
                .get_metadata("cache_hit")
                .map(|v| v.to_string())
                .unwrap_or_default();
            if cache_hit == "false" {
                let key = ctx
                    .get_metadata("cache_key")
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                if !key.is_empty() {
                    // In a real implementation, we would serialize the result
                    // For now, we just store a placeholder
                    self.set_in_cache(&key, b"cached".to_vec());
                }
            }
        }

        Ok(())
    }
}

// ========== PerformanceInterceptor ==========

#[async_trait]
impl AsyncAkitaInterceptor for PerformanceInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Record start time
        ctx.set_metadata(
            "performance_start_time".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        );
        Ok(())
    }

    async fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        _result: &mut std::result::Result<ExecuteResult, AkitaError>,
    ) -> Result<()> {
        let start_time_str = ctx
            .get_metadata("performance_start_time")
            .map(|v| v.to_string())
            .unwrap_or_default();

        if let Ok(start_time) = start_time_str.parse::<i64>() {
            let now = chrono::Utc::now().timestamp_millis();
            let execution_time_ms = (now - start_time).max(0) as u64;

            let is_slow = execution_time_ms > self.config().slow_query_threshold_ms;

            // Record metrics
            if self.config().enable_metrics {
                self.metrics_collector()
                    .record_query(execution_time_ms, is_slow);
            }

            // Store in context
            ctx.set_metadata(
                "execution_time_ms".to_string(),
                execution_time_ms.to_string(),
            );
            ctx.set_metadata("is_slow_query".to_string(), is_slow.to_string());

            // Log slow queries
            if is_slow {
                tracing::warn!(
                    "Slow query detected: {}ms (threshold: {}ms)\nSQL: {}",
                    execution_time_ms,
                    self.config().slow_query_threshold_ms,
                    ctx.final_sql()
                );
            }
        }

        Ok(())
    }
}
