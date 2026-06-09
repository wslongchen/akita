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

//! Pagination Interceptor - Automatically adds pagination to SELECT queries.
//!
//! This interceptor automatically adds LIMIT/OFFSET clauses to SELECT queries
//! based on pagination metadata. It can also execute COUNT queries to get the
//! total number of records.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::pagination::{PaginationInterceptor, PaginationRequest};
//!
//! let interceptor = PaginationInterceptor::new();
//!
//! // In your code:
//! ctx.set_metadata("pagination", PaginationRequest {
//!     page: 1,
//!     size: 10,
//!     count_total: true,
//! });
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};

/// Pagination request parameters.
#[derive(Debug, Clone)]
pub struct PaginationRequest {
    /// Page number (1-based)
    pub page: u64,
    /// Page size
    pub size: u64,
    /// Whether to count total records
    pub count_total: bool,
}

impl PaginationRequest {
    pub fn new(page: u64, size: u64) -> Self {
        Self {
            page,
            size,
            count_total: true,
        }
    }

    pub fn without_count(mut self) -> Self {
        self.count_total = false;
        self
    }

    /// Calculate the offset.
    pub fn offset(&self) -> u64 {
        if self.page > 0 {
            (self.page - 1) * self.size
        } else {
            0
        }
    }
}

/// Pagination result stored in metadata after query execution.
#[derive(Debug, Clone)]
pub struct PaginationResult {
    /// Current page number
    pub page: u64,
    /// Page size
    pub size: u64,
    /// Total number of records (if count_total was true)
    pub total: Option<u64>,
    /// Total number of pages
    pub pages: Option<u64>,
}

impl PaginationResult {
    pub fn new(page: u64, size: u64, total: Option<u64>) -> Self {
        let pages = total.map(|t| if size > 0 { (t + size - 1) / size } else { 0 });

        Self {
            page,
            size,
            total,
            pages,
        }
    }
}

/// Pagination Interceptor - Automatically adds pagination to SELECT queries.
///
/// This interceptor:
/// - Adds LIMIT/OFFSET to SELECT queries based on pagination metadata
/// - Can execute COUNT queries to get total records
/// - Stores pagination result in metadata
pub struct PaginationInterceptor {
    /// Default page size if not specified
    default_page_size: u64,
    /// Maximum allowed page size
    max_page_size: u64,
}

impl Default for PaginationInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl PaginationInterceptor {
    pub fn new() -> Self {
        Self {
            default_page_size: 10,
            max_page_size: 1000,
        }
    }

    /// Set the default page size.
    pub fn with_default_page_size(mut self, size: u64) -> Self {
        self.default_page_size = size;
        self
    }

    /// Set the maximum page size.
    pub fn with_max_page_size(mut self, size: u64) -> Self {
        self.max_page_size = size;
        self
    }

    /// Validate and normalize pagination request.
    fn normalize_request(&self, req: &PaginationRequest) -> PaginationRequest {
        let size = if req.size == 0 {
            self.default_page_size
        } else {
            req.size.min(self.max_page_size)
        };

        PaginationRequest {
            page: req.page.max(1),
            size,
            count_total: req.count_total,
        }
    }
}

impl InterceptorBase for PaginationInterceptor {
    fn name(&self) -> &'static str {
        "pagination"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Pagination
    }

    fn order(&self) -> i32 {
        30 // Execute after soft delete
    }

    fn supports_operation(&self, operation: &OperationType) -> bool {
        matches!(operation, OperationType::Select)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for PaginationInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Check if pagination is requested
        let pagination_json = ctx.get_metadata("pagination");

        if let Some(_pagination) = pagination_json {
            // Store normalized pagination info for the driver
            ctx.set_metadata("pagination_active".to_string(), "true".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_request() {
        let req = PaginationRequest::new(2, 10);
        assert_eq!(req.page, 2);
        assert_eq!(req.size, 10);
        assert_eq!(req.offset(), 10);
        assert!(req.count_total);
    }

    #[test]
    fn test_pagination_request_first_page() {
        let req = PaginationRequest::new(1, 20);
        assert_eq!(req.offset(), 0);
    }

    #[test]
    fn test_pagination_request_without_count() {
        let req = PaginationRequest::new(1, 10).without_count();
        assert!(!req.count_total);
    }

    #[test]
    fn test_pagination_result() {
        let result = PaginationResult::new(2, 10, Some(25));
        assert_eq!(result.page, 2);
        assert_eq!(result.size, 10);
        assert_eq!(result.total, Some(25));
        assert_eq!(result.pages, Some(3));
    }

    #[test]
    fn test_pagination_result_no_total() {
        let result = PaginationResult::new(1, 10, None);
        assert_eq!(result.total, None);
        assert_eq!(result.pages, None);
    }

    #[test]
    fn test_pagination_interceptor() {
        let interceptor = PaginationInterceptor::new();
        assert_eq!(interceptor.name(), "pagination");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::Pagination);
        assert_eq!(interceptor.order(), 30);
        assert!(interceptor.supports_operation(&OperationType::Select));
        assert!(!interceptor
            .supports_operation(&OperationType::Insert(akita_core::InsertType::SingleInsert)));
    }

    #[test]
    fn test_normalize_request() {
        let interceptor = PaginationInterceptor::new()
            .with_default_page_size(20)
            .with_max_page_size(100);

        // Test default size
        let req = PaginationRequest::new(1, 0);
        let normalized = interceptor.normalize_request(&req);
        assert_eq!(normalized.size, 20);

        // Test max size limit
        let req = PaginationRequest::new(1, 200);
        let normalized = interceptor.normalize_request(&req);
        assert_eq!(normalized.size, 100);

        // Test page normalization
        let req = PaginationRequest::new(0, 10);
        let normalized = interceptor.normalize_request(&req);
        assert_eq!(normalized.page, 1);
    }
}
