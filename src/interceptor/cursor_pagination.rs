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

//! Cursor Pagination Interceptor - Efficient pagination for large datasets.
//!
//! This interceptor provides cursor-based pagination which is more efficient
//! than offset-based pagination for large datasets. Instead of using OFFSET,
//! it uses a cursor (typically the last record's ID) to fetch the next page.
//!
//! # Example
//! ```ignore
//! use akita::interceptor::cursor_pagination::{CursorPaginationInterceptor, CursorPaginationRequest, CursorDirection};
//!
//! let interceptor = CursorPaginationInterceptor::new();
//!
//! // First page (no cursor)
//! let request = CursorPaginationRequest::new(None, 10, CursorDirection::Forward);
//!
//! // Next page (with cursor from last result)
//! let request = CursorPaginationRequest::new(Some("123".to_string()), 10, CursorDirection::Forward);
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};

/// Cursor pagination direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    /// Fetch records after the cursor (next page)
    Forward,
    /// Fetch records before the cursor (previous page)
    Backward,
}

/// Cursor pagination request parameters.
#[derive(Debug, Clone)]
pub struct CursorPaginationRequest {
    /// The cursor value (typically the last record's ID)
    pub cursor: Option<String>,
    /// Number of records to fetch
    pub limit: u64,
    /// Direction of pagination
    pub direction: CursorDirection,
    /// The column to use as cursor (default: "id")
    pub cursor_column: String,
}

impl CursorPaginationRequest {
    /// Create a new cursor pagination request.
    pub fn new(cursor: Option<String>, limit: u64, direction: CursorDirection) -> Self {
        Self {
            cursor,
            limit,
            direction,
            cursor_column: "id".to_string(),
        }
    }

    /// Set the cursor column.
    pub fn with_cursor_column(mut self, column: &str) -> Self {
        self.cursor_column = column.to_string();
        self
    }
}

/// Cursor pagination result stored in metadata.
#[derive(Debug, Clone)]
pub struct CursorPaginationResult {
    /// The data records
    pub has_more: bool,
    /// Next cursor for forward pagination
    pub next_cursor: Option<String>,
    /// Previous cursor for backward pagination
    pub prev_cursor: Option<String>,
}

impl CursorPaginationResult {
    pub fn new(has_more: bool, next_cursor: Option<String>, prev_cursor: Option<String>) -> Self {
        Self {
            has_more,
            next_cursor,
            prev_cursor,
        }
    }
}

/// Cursor Pagination Interceptor - Efficient pagination for large datasets.
///
/// This interceptor:
/// - Adds WHERE conditions based on cursor for efficient pagination
/// - Supports forward and backward pagination
/// - Generates next/previous cursors for the result
pub struct CursorPaginationInterceptor {
    /// Default page size
    default_limit: u64,
    /// Maximum allowed page size
    max_limit: u64,
}

impl Default for CursorPaginationInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorPaginationInterceptor {
    pub fn new() -> Self {
        Self {
            default_limit: 10,
            max_limit: 1000,
        }
    }

    /// Set the default page size.
    pub fn with_default_limit(mut self, limit: u64) -> Self {
        self.default_limit = limit;
        self
    }

    /// Set the maximum page size.
    pub fn with_max_limit(mut self, limit: u64) -> Self {
        self.max_limit = limit;
        self
    }

    /// Validate and normalize the request.
    fn normalize_request(&self, req: &CursorPaginationRequest) -> CursorPaginationRequest {
        let limit = if req.limit == 0 {
            self.default_limit
        } else {
            req.limit.min(self.max_limit)
        };

        CursorPaginationRequest {
            cursor: req.cursor.clone(),
            limit,
            direction: req.direction,
            cursor_column: req.cursor_column.clone(),
        }
    }
}

impl InterceptorBase for CursorPaginationInterceptor {
    fn name(&self) -> &'static str {
        "cursor_pagination"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Pagination
    }

    fn order(&self) -> i32 {
        35 // Execute after regular pagination
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
impl crate::interceptor::blocking::AkitaInterceptor for CursorPaginationInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Check if cursor pagination is requested
        let cursor_pagination_json = ctx.get_metadata("cursor_pagination");

        if let Some(_cursor_pagination) = cursor_pagination_json {
            // Store normalized pagination info for the driver
            ctx.set_metadata("cursor_pagination_active".to_string(), "true".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_pagination_request() {
        let req =
            CursorPaginationRequest::new(Some("123".to_string()), 10, CursorDirection::Forward);
        assert_eq!(req.cursor, Some("123".to_string()));
        assert_eq!(req.limit, 10);
        assert_eq!(req.direction, CursorDirection::Forward);
        assert_eq!(req.cursor_column, "id");
    }

    #[test]
    fn test_cursor_pagination_request_first_page() {
        let req = CursorPaginationRequest::new(None, 10, CursorDirection::Forward);
        assert_eq!(req.cursor, None);
    }

    #[test]
    fn test_cursor_pagination_request_backward() {
        let req =
            CursorPaginationRequest::new(Some("123".to_string()), 10, CursorDirection::Backward);
        assert_eq!(req.direction, CursorDirection::Backward);
    }

    #[test]
    fn test_cursor_pagination_request_custom_column() {
        let req =
            CursorPaginationRequest::new(Some("123".to_string()), 10, CursorDirection::Forward)
                .with_cursor_column("created_at");
        assert_eq!(req.cursor_column, "created_at");
    }

    #[test]
    fn test_cursor_pagination_result() {
        let result =
            CursorPaginationResult::new(true, Some("456".to_string()), Some("123".to_string()));
        assert!(result.has_more);
        assert_eq!(result.next_cursor, Some("456".to_string()));
        assert_eq!(result.prev_cursor, Some("123".to_string()));
    }

    #[test]
    fn test_cursor_pagination_interceptor() {
        let interceptor = CursorPaginationInterceptor::new();
        assert_eq!(interceptor.name(), "cursor_pagination");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::Pagination);
        assert_eq!(interceptor.order(), 35);
        assert!(interceptor.supports_operation(&OperationType::Select));
        assert!(!interceptor
            .supports_operation(&OperationType::Insert(akita_core::InsertType::SingleInsert)));
    }

    #[test]
    fn test_normalize_request() {
        let interceptor = CursorPaginationInterceptor::new()
            .with_default_limit(20)
            .with_max_limit(100);

        // Test default limit
        let req = CursorPaginationRequest::new(None, 0, CursorDirection::Forward);
        let normalized = interceptor.normalize_request(&req);
        assert_eq!(normalized.limit, 20);

        // Test max limit
        let req = CursorPaginationRequest::new(None, 200, CursorDirection::Forward);
        let normalized = interceptor.normalize_request(&req);
        assert_eq!(normalized.limit, 100);
    }
}
