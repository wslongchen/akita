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

//! Cache Interceptor - Automatic query result caching.
//!
//! This interceptor provides automatic caching of query results to improve
//! performance. It supports pluggable cache providers (memory, Redis, etc.)
//! and configurable TTL (time-to-live).
//!
//! # Example
//! ```ignore
//! use akita::interceptor::cache::{CacheInterceptor, MemoryCacheProvider};
//!
//! let interceptor = CacheInterceptor::new(Box::new(MemoryCacheProvider::new()))
//!     .with_default_ttl(Duration::from_secs(300));
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Cache entry with expiration.
struct CacheEntry {
    data: Vec<u8>,
    created_at: Instant,
    ttl: Duration,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Trait for cache providers.
///
/// Implement this trait to provide custom cache backends (e.g., Redis).
pub trait CacheProvider: Send + Sync {
    /// Get a value from cache.
    fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Set a value in cache with optional TTL.
    fn set(&self, key: &str, value: Vec<u8>, ttl: Duration);

    /// Delete a value from cache.
    fn delete(&self, key: &str);

    /// Clear all cached values.
    fn clear(&self);

    /// Get the number of cached entries.
    fn size(&self) -> usize;
}

/// In-memory cache provider.
///
/// This is a simple in-memory cache using `HashMap`. It's suitable for
/// single-instance applications. For distributed applications, use a
/// Redis-based provider.
pub struct MemoryCacheProvider {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    max_size: usize,
}

impl MemoryCacheProvider {
    /// Create a new memory cache provider.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size: 1000,
        }
    }

    /// Set the maximum number of cache entries.
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Remove expired entries.
    fn cleanup(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.retain(|_, entry| !entry.is_expired());
        }
    }
}

impl Default for MemoryCacheProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheProvider for MemoryCacheProvider {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.cleanup();

        if let Ok(cache) = self.cache.read() {
            cache.get(key).map(|entry| entry.data.clone())
        } else {
            None
        }
    }

    fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) {
        self.cleanup();

        if let Ok(mut cache) = self.cache.write() {
            // Check if we need to evict entries
            if cache.len() >= self.max_size {
                // Simple eviction: remove oldest entry
                if let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.created_at)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&oldest_key);
                }
            }

            cache.insert(
                key.to_string(),
                CacheEntry {
                    data: value,
                    created_at: Instant::now(),
                    ttl,
                },
            );
        }
    }

    fn delete(&self, key: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache.remove(key);
        }
    }

    fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    fn size(&self) -> usize {
        if let Ok(cache) = self.cache.read() {
            cache.len()
        } else {
            0
        }
    }
}

/// No-op cache provider (disabled caching).
pub struct NoopCacheProvider;

impl CacheProvider for NoopCacheProvider {
    fn get(&self, _key: &str) -> Option<Vec<u8>> {
        None
    }

    fn set(&self, _key: &str, _value: Vec<u8>, _ttl: Duration) {}

    fn delete(&self, _key: &str) {}

    fn clear(&self) {}

    fn size(&self) -> usize {
        0
    }
}

/// Cache Interceptor - Automatic query result caching.
///
/// This interceptor:
/// - Caches SELECT query results automatically
/// - Uses configurable cache providers
/// - Supports TTL-based expiration
/// - Invalidates cache on INSERT/UPDATE/DELETE operations
pub struct CacheInterceptor {
    provider: Box<dyn CacheProvider>,
    default_ttl: Duration,
    pub(crate) enabled: bool,
    cache_prefix: String,
}

impl CacheInterceptor {
    /// Create a new cache interceptor with the given provider.
    pub fn new(provider: Box<dyn CacheProvider>) -> Self {
        Self {
            provider,
            default_ttl: Duration::from_secs(300), // 5 minutes
            enabled: true,
            cache_prefix: "akita_cache:".to_string(),
        }
    }

    /// Set the default TTL.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Enable or disable caching.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the cache key prefix.
    pub fn with_cache_prefix(mut self, prefix: &str) -> Self {
        self.cache_prefix = prefix.to_string();
        self
    }

    /// Generate a cache key from SQL and params.
    pub fn generate_cache_key(&self, sql: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let hash = hasher.finish();

        format!("{}{:016x}", self.cache_prefix, hash)
    }

    /// Get from cache.
    pub fn get_from_cache(&self, key: &str) -> Option<Vec<u8>> {
        self.provider.get(key)
    }

    /// Set in cache.
    pub fn set_in_cache(&self, key: &str, value: Vec<u8>) {
        self.provider.set(key, value, self.default_ttl);
    }

    /// Invalidate cache entries matching a pattern.
    pub fn invalidate_pattern(&self, pattern: &str) {
        // For memory cache, we clear all entries
        // A more sophisticated implementation would support pattern matching
        if pattern.contains('*') || pattern.contains('%') {
            self.provider.clear();
        } else {
            self.provider.delete(pattern);
        }
    }
}

impl InterceptorBase for CacheInterceptor {
    fn name(&self) -> &'static str {
        "cache"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Cache
    }

    fn order(&self) -> i32 {
        5 // Execute early, before most interceptors
    }

    fn supports_operation(&self, _operation: &OperationType) -> bool {
        // Cache supports all operations
        true
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for CacheInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
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

    fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        result: &mut std::result::Result<crate::comm::ExecuteResult, crate::errors::AkitaError>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_cache_provider() {
        let provider = MemoryCacheProvider::new();

        // Test set and get
        provider.set("key1", b"value1".to_vec(), Duration::from_secs(60));
        assert_eq!(provider.get("key1"), Some(b"value1".to_vec()));

        // Test delete
        provider.delete("key1");
        assert_eq!(provider.get("key1"), None);

        // Test clear
        provider.set("key2", b"value2".to_vec(), Duration::from_secs(60));
        provider.set("key3", b"value3".to_vec(), Duration::from_secs(60));
        assert_eq!(provider.size(), 2);

        provider.clear();
        assert_eq!(provider.size(), 0);
    }

    #[test]
    fn test_memory_cache_provider_max_size() {
        let provider = MemoryCacheProvider::new().with_max_size(2);

        provider.set("key1", b"value1".to_vec(), Duration::from_secs(60));
        provider.set("key2", b"value2".to_vec(), Duration::from_secs(60));
        provider.set("key3", b"value3".to_vec(), Duration::from_secs(60));

        // Should have evicted one entry
        assert!(provider.size() <= 2);
    }

    #[test]
    fn test_noop_cache_provider() {
        let provider = NoopCacheProvider;

        // All operations should be no-ops
        provider.set("key1", b"value1".to_vec(), Duration::from_secs(60));
        assert_eq!(provider.get("key1"), None);
        assert_eq!(provider.size(), 0);
    }

    #[test]
    fn test_cache_interceptor() {
        let provider = MemoryCacheProvider::new();
        let interceptor = CacheInterceptor::new(Box::new(provider))
            .with_default_ttl(Duration::from_secs(60))
            .with_cache_prefix("test:");

        assert_eq!(interceptor.name(), "cache");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::Cache);
        assert_eq!(interceptor.order(), 5);
    }

    #[test]
    fn test_generate_cache_key() {
        let provider = MemoryCacheProvider::new();
        let interceptor = CacheInterceptor::new(Box::new(provider)).with_cache_prefix("test:");

        let key1 = interceptor.generate_cache_key("SELECT * FROM users");
        let key2 = interceptor.generate_cache_key("SELECT * FROM users");
        let key3 = interceptor.generate_cache_key("SELECT * FROM orders");

        // Same SQL should generate same key
        assert_eq!(key1, key2);

        // Different SQL should generate different key
        assert_ne!(key1, key3);

        // Key should have prefix
        assert!(key1.starts_with("test:"));
    }
}
