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

//! Performance Interceptor - SQL performance monitoring and slow query detection.
//!
//! This interceptor monitors SQL query performance and logs slow queries.
//! It collects metrics like query count, average execution time, and
//! identifies slow queries that exceed a configurable threshold.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::performance::{PerformanceInterceptor, PerformanceConfig};
//!
//! let interceptor = PerformanceInterceptor::new(
//!     PerformanceConfig {
//!         slow_query_threshold_ms: 1000,
//!         enable_metrics: true,
//!     }
//! );
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Performance monitoring configuration.
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Threshold in milliseconds for slow query detection (default: 1000ms)
    pub slow_query_threshold_ms: u64,
    /// Whether to collect metrics (default: true)
    pub enable_metrics: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            slow_query_threshold_ms: 1000,
            enable_metrics: true,
        }
    }
}

/// Performance metrics collected by the interceptor.
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Total number of queries executed
    pub total_queries: u64,
    /// Number of slow queries
    pub slow_queries: u64,
    /// Total execution time in milliseconds
    pub total_execution_time_ms: u64,
    /// Maximum execution time in milliseconds
    pub max_execution_time_ms: u64,
    /// Minimum execution time in milliseconds
    pub min_execution_time_ms: u64,
}

impl PerformanceMetrics {
    /// Calculate average execution time.
    pub fn avg_execution_time_ms(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.total_execution_time_ms as f64 / self.total_queries as f64
        }
    }
}

/// Thread-safe performance metrics collector.
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    total_queries: Arc<AtomicU64>,
    slow_queries: Arc<AtomicU64>,
    total_execution_time_ms: Arc<AtomicU64>,
    max_execution_time_ms: Arc<AtomicU64>,
    min_execution_time_ms: Arc<AtomicU64>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_queries: Arc::new(AtomicU64::new(0)),
            slow_queries: Arc::new(AtomicU64::new(0)),
            total_execution_time_ms: Arc::new(AtomicU64::new(0)),
            max_execution_time_ms: Arc::new(AtomicU64::new(0)),
            min_execution_time_ms: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    /// Record a query execution.
    pub fn record_query(&self, execution_time_ms: u64, is_slow: bool) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_ms
            .fetch_add(execution_time_ms, Ordering::Relaxed);

        if is_slow {
            self.slow_queries.fetch_add(1, Ordering::Relaxed);
        }

        // Update max
        let mut current_max = self.max_execution_time_ms.load(Ordering::Relaxed);
        loop {
            if execution_time_ms <= current_max {
                break;
            }
            match self.max_execution_time_ms.compare_exchange_weak(
                current_max,
                execution_time_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        // Update min
        let mut current_min = self.min_execution_time_ms.load(Ordering::Relaxed);
        loop {
            if execution_time_ms >= current_min {
                break;
            }
            match self.min_execution_time_ms.compare_exchange_weak(
                current_min,
                execution_time_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }
    }

    /// Get current metrics.
    pub fn get_metrics(&self) -> PerformanceMetrics {
        PerformanceMetrics {
            total_queries: self.total_queries.load(Ordering::Relaxed),
            slow_queries: self.slow_queries.load(Ordering::Relaxed),
            total_execution_time_ms: self.total_execution_time_ms.load(Ordering::Relaxed),
            max_execution_time_ms: self.max_execution_time_ms.load(Ordering::Relaxed),
            min_execution_time_ms: {
                let val = self.min_execution_time_ms.load(Ordering::Relaxed);
                if val == u64::MAX {
                    0
                } else {
                    val
                }
            },
        }
    }

    /// Reset all metrics.
    pub fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.slow_queries.store(0, Ordering::Relaxed);
        self.total_execution_time_ms.store(0, Ordering::Relaxed);
        self.max_execution_time_ms.store(0, Ordering::Relaxed);
        self.min_execution_time_ms
            .store(u64::MAX, Ordering::Relaxed);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance Interceptor - SQL performance monitoring.
///
/// This interceptor:
/// - Tracks query execution times
/// - Detects and logs slow queries
/// - Collects performance metrics
/// - Stores timing information in ExecuteContext metadata
pub struct PerformanceInterceptor {
    config: PerformanceConfig,
    metrics: MetricsCollector,
}

impl PerformanceInterceptor {
    /// Create a new performance interceptor.
    pub fn new(config: PerformanceConfig) -> Self {
        Self {
            config,
            metrics: MetricsCollector::new(),
        }
    }

    /// Create a new interceptor with default configuration.
    pub fn with_default() -> Self {
        Self::new(PerformanceConfig::default())
    }

    /// Get the current metrics.
    pub fn metrics(&self) -> PerformanceMetrics {
        self.metrics.get_metrics()
    }

    /// Get the metrics collector.
    pub fn metrics_collector(&self) -> &MetricsCollector {
        &self.metrics
    }

    /// Reset the metrics.
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Get the configuration.
    pub fn config(&self) -> &PerformanceConfig {
        &self.config
    }
}

impl InterceptorBase for PerformanceInterceptor {
    fn name(&self) -> &'static str {
        "performance"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Performance
    }

    fn order(&self) -> i32 {
        95 // Execute near the end, after most interceptors
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for PerformanceInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Record start time
        ctx.set_metadata(
            "performance_start_time".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        );
        Ok(())
    }

    fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        _result: &mut std::result::Result<crate::comm::ExecuteResult, crate::errors::AkitaError>,
    ) -> Result<()> {
        let start_time_str = ctx
            .get_metadata("performance_start_time")
            .map(|v| v.to_string())
            .unwrap_or_default();

        if let Ok(start_time) = start_time_str.parse::<i64>() {
            let now = chrono::Utc::now().timestamp_millis();
            let execution_time_ms = (now - start_time).max(0) as u64;

            let is_slow = execution_time_ms > self.config.slow_query_threshold_ms;

            // Record metrics
            if self.config.enable_metrics {
                self.metrics.record_query(execution_time_ms, is_slow);
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
                    self.config.slow_query_threshold_ms,
                    ctx.final_sql()
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_config_default() {
        let config = PerformanceConfig::default();
        assert_eq!(config.slow_query_threshold_ms, 1000);
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();

        // Record some queries
        collector.record_query(100, false);
        collector.record_query(200, false);
        collector.record_query(1500, true);

        let metrics = collector.get_metrics();
        assert_eq!(metrics.total_queries, 3);
        assert_eq!(metrics.slow_queries, 1);
        assert_eq!(metrics.total_execution_time_ms, 1800);
        assert_eq!(metrics.max_execution_time_ms, 1500);
        assert_eq!(metrics.min_execution_time_ms, 100);
        assert!((metrics.avg_execution_time_ms() - 600.0).abs() < 0.01);
    }

    #[test]
    fn test_metrics_collector_reset() {
        let collector = MetricsCollector::new();

        collector.record_query(100, false);
        collector.record_query(200, false);

        let metrics = collector.get_metrics();
        assert_eq!(metrics.total_queries, 2);

        collector.reset();

        let metrics = collector.get_metrics();
        assert_eq!(metrics.total_queries, 0);
        assert_eq!(metrics.slow_queries, 0);
    }

    #[test]
    fn test_performance_interceptor() {
        let interceptor = PerformanceInterceptor::with_default();
        assert_eq!(interceptor.name(), "performance");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::Performance);
        assert_eq!(interceptor.order(), 95);
    }

    #[test]
    fn test_performance_metrics_avg() {
        let metrics = PerformanceMetrics {
            total_queries: 0,
            slow_queries: 0,
            total_execution_time_ms: 0,
            max_execution_time_ms: 0,
            min_execution_time_ms: 0,
        };
        assert_eq!(metrics.avg_execution_time_ms(), 0.0);
    }
}
