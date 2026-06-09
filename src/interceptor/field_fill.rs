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

//! Field Fill Interceptor - Automatically populates fields based on operation type.
//!
//! This interceptor automatically fills fields like `create_time`, `update_time`,
//! `create_by`, `update_by` based on the operation type (INSERT/UPDATE).
//!
//! # Example
//! ```ignore
//! use akita::interceptor::field_fill::{FieldFillInterceptor, TimestampFillHandler};
//!
//! let interceptor = FieldFillInterceptor::new()
//!     .with_handler("create_time", Box::new(TimestampFillHandler::new()))
//!     .with_handler("update_time", Box::new(TimestampFillHandler::new()));
//! ```

use crate::comm::ExecuteContext;
use crate::errors::Result;
use crate::interceptor::shared::InterceptorBase;
use crate::interceptor::{InterceptorType, OperationType};
use akita_core::AkitaValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for custom field fill handlers.
///
/// Implement this trait to provide custom fill logic for specific fields.
pub trait FieldFillHandler: Send + Sync {
    /// Get the fill value for the given operation.
    fn fill(&self, operation: &OperationType) -> Option<AkitaValue>;

    /// Get the handler name for debugging.
    fn name(&self) -> &str;
}

/// Timestamp fill handler - fills with current UTC timestamp.
pub struct TimestampFillHandler {
    format: TimestampFormat,
}

/// Timestamp format options.
pub enum TimestampFormat {
    /// DateTime (NaiveDateTime)
    DateTime,
    /// Unix timestamp (seconds)
    UnixSeconds,
    /// Unix timestamp (milliseconds)
    UnixMillis,
}

impl TimestampFillHandler {
    /// Create a new timestamp fill handler with the default `DateTime` format.
    ///
    /// # Returns
    /// A new `TimestampFillHandler` that produces `NaiveDateTime` timestamps.
    ///
    /// # Example
    /// ```ignore
    /// let handler = TimestampFillHandler::new();
    /// ```
    pub fn new() -> Self {
        Self {
            format: TimestampFormat::DateTime,
        }
    }

    /// Set the timestamp format for this handler.
    ///
    /// # Arguments
    /// * `format` - The desired timestamp format (`DateTime`, `UnixSeconds`, or `UnixMillis`).
    ///
    /// # Returns
    /// The handler with the updated format (builder pattern).
    ///
    /// # Example
    /// ```ignore
    /// let handler = TimestampFillHandler::new()
    ///     .with_format(TimestampFormat::UnixSeconds);
    /// ```
    pub fn with_format(mut self, format: TimestampFormat) -> Self {
        self.format = format;
        self
    }
}

impl Default for TimestampFillHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldFillHandler for TimestampFillHandler {
    fn fill(&self, _operation: &OperationType) -> Option<AkitaValue> {
        let now = chrono::Utc::now();
        match self.format {
            TimestampFormat::DateTime => Some(AkitaValue::Timestamp(now)),
            TimestampFormat::UnixSeconds => Some(AkitaValue::Bigint(now.timestamp())),
            TimestampFormat::UnixMillis => Some(AkitaValue::Bigint(now.timestamp_millis())),
        }
    }

    fn name(&self) -> &str {
        "TimestampFillHandler"
    }
}

/// UUID fill handler - fills with UUID v4.
pub struct UuidFillHandler;

impl UuidFillHandler {
    /// Create a new UUID fill handler.
    ///
    /// # Returns
    /// A new `UuidFillHandler` that generates UUID v4 strings.
    ///
    /// # Example
    /// ```ignore
    /// let handler = UuidFillHandler::new();
    /// ```
    pub fn new() -> Self {
        Self
    }
}

impl FieldFillHandler for UuidFillHandler {
    fn fill(&self, _operation: &OperationType) -> Option<AkitaValue> {
        Some(AkitaValue::Text(uuid::Uuid::new_v4().to_string()))
    }

    fn name(&self) -> &str {
        "UuidFillHandler"
    }
}

/// Fixed value fill handler - always returns the same value.
pub struct FixedValueFillHandler {
    value: AkitaValue,
}

impl FixedValueFillHandler {
    /// Create a new fixed value fill handler.
    ///
    /// # Arguments
    /// * `value` - The fixed value to return for every fill operation.
    ///
    /// # Returns
    /// A new `FixedValueFillHandler` that always returns the given value.
    ///
    /// # Example
    /// ```ignore
    /// use akita_core::AkitaValue;
    /// let handler = FixedValueFillHandler::new(AkitaValue::Text("system".to_string()));
    /// ```
    pub fn new(value: AkitaValue) -> Self {
        Self { value }
    }
}

impl FieldFillHandler for FixedValueFillHandler {
    fn fill(&self, _operation: &OperationType) -> Option<AkitaValue> {
        Some(self.value.clone())
    }

    fn name(&self) -> &str {
        "FixedValueFillHandler"
    }
}

/// Field Fill Interceptor - Automatically populates fields based on operation type.
///
/// This interceptor inspects the SQL being executed and adds field values
/// for INSERT and UPDATE operations based on configured handlers.
pub struct FieldFillInterceptor {
    handlers: HashMap<String, Box<dyn FieldFillHandler>>,
    pub(crate) enabled: bool,
}

impl Default for FieldFillInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldFillInterceptor {
    /// Create a new field fill interceptor with no handlers configured.
    ///
    /// # Returns
    /// A new `FieldFillInterceptor` in enabled state with an empty handler map.
    ///
    /// # Example
    /// ```ignore
    /// let interceptor = FieldFillInterceptor::new();
    /// ```
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            enabled: true,
        }
    }

    /// Add a fill handler for a specific field name.
    pub fn with_handler(mut self, field_name: &str, handler: Box<dyn FieldFillHandler>) -> Self {
        self.handlers.insert(field_name.to_string(), handler);
        self
    }

    /// Enable or disable the interceptor.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Get fill values for the given operation.
    pub fn get_fill_values(&self, operation: &OperationType) -> HashMap<String, AkitaValue> {
        let mut fills = HashMap::new();

        for (field_name, handler) in &self.handlers {
            if let Some(value) = handler.fill(operation) {
                fills.insert(field_name.clone(), value);
            }
        }

        fills
    }
}

impl InterceptorBase for FieldFillInterceptor {
    fn name(&self) -> &'static str {
        "field_fill"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::FieldFill
    }

    fn order(&self) -> i32 {
        10 // Execute early, before other interceptors
    }

    fn supports_operation(&self, operation: &OperationType) -> bool {
        matches!(operation, OperationType::Insert(_) | OperationType::Update)
    }
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
impl crate::interceptor::blocking::AkitaInterceptor for FieldFillInterceptor {
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use akita_core::InsertType;

    #[test]
    fn test_timestamp_fill_handler() {
        let handler = TimestampFillHandler::new();
        let value = handler.fill(&OperationType::Insert(InsertType::SingleInsert));
        assert!(value.is_some(), "Timestamp handler should return a value");
    }

    #[test]
    fn test_uuid_fill_handler() {
        let handler = UuidFillHandler::new();
        let value = handler.fill(&OperationType::Insert(InsertType::SingleInsert));
        assert!(value.is_some(), "UUID handler should return a value");

        if let Some(AkitaValue::Text(uuid_str)) = value {
            assert!(uuid_str.len() == 36, "UUID should be 36 chars");
        }
    }

    #[test]
    fn test_fixed_value_handler() {
        let handler = FixedValueFillHandler::new(AkitaValue::Text("test".to_string()));
        let value = handler.fill(&OperationType::Insert(InsertType::SingleInsert));
        assert_eq!(value, Some(AkitaValue::Text("test".to_string())));
    }

    #[test]
    fn test_field_fill_interceptor() {
        let interceptor = FieldFillInterceptor::new()
            .with_handler("create_time", Box::new(TimestampFillHandler::new()))
            .with_handler("id", Box::new(UuidFillHandler::new()));

        let fills = interceptor.get_fill_values(&OperationType::Insert(InsertType::SingleInsert));
        assert_eq!(fills.len(), 2, "Should have 2 fill values");
        assert!(fills.contains_key("create_time"));
        assert!(fills.contains_key("id"));
    }

    #[test]
    fn test_interceptor_base() {
        let interceptor = FieldFillInterceptor::new();
        assert_eq!(interceptor.name(), "field_fill");
        assert_eq!(interceptor.interceptor_type(), InterceptorType::FieldFill);
        assert_eq!(interceptor.order(), 10);
        assert!(interceptor.supports_operation(&OperationType::Insert(InsertType::SingleInsert)));
        assert!(interceptor.supports_operation(&OperationType::Update));
        assert!(!interceptor.supports_operation(&OperationType::Select));
        assert!(!interceptor.supports_operation(&OperationType::Delete));
    }
}
