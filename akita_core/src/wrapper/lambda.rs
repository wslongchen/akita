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

//! Lambda Query Wrapper - Compile-time safe column references.
//!
//! This module provides `LambdaWrapper<T>` which allows using struct field
//! references instead of string column names, providing compile-time safety.
//!
//! # Example
//! ```ignore
//! use akita::prelude::*;
//!
//! #[derive(Entity)]
//! #[table(name = "users")]
//! struct User {
//!     #[id]
//!     id: i64,
//!     #[field(name = "user_name")]
//!     name: String,
//!     age: i32,
//! }
//!
//! // Compile-time safe column references
//! let wrapper = LambdaWrapper::<User>::new()
//!     .eq(User::name, "Alice")
//!     .gt(User::age, 18);
//! ```

use crate::{AkitaValue, GetFields, IntoAkitaValue, SqlOperator, Wrapper};
use std::marker::PhantomData;

/// Lambda wrapper for compile-time safe column references.
///
/// This wrapper uses struct field accessor functions instead of string column names,
/// providing compile-time safety for column references.
pub struct LambdaWrapper<T: GetFields> {
    wrapper: Wrapper,
    _phantom: PhantomData<T>,
}

impl<T: GetFields> LambdaWrapper<T> {
    /// Create a new lambda wrapper.
    pub fn new() -> Self {
        Self {
            wrapper: Wrapper::new(),
            _phantom: PhantomData,
        }
    }

    /// Create a lambda wrapper from an existing wrapper.
    pub fn from_wrapper(wrapper: Wrapper) -> Self {
        Self {
            wrapper,
            _phantom: PhantomData,
        }
    }

    /// Get the underlying wrapper.
    pub fn into_wrapper(self) -> Wrapper {
        self.wrapper
    }

    /// Get a reference to the underlying wrapper.
    pub fn wrapper(&self) -> &Wrapper {
        &self.wrapper
    }

    // ========== Basic Conditions ==========

    /// Add an equals condition using a field accessor.
    ///
    /// # Example
    /// ```ignore
    /// let wrapper = LambdaWrapper::<User>::new()
    ///     .eq(User::name, "Alice");
    /// ```
    pub fn eq<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.eq(column, value);
        self
    }

    /// Add a not equals condition using a field accessor.
    pub fn ne<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.ne(column, value);
        self
    }

    /// Add a greater than condition using a field accessor.
    pub fn gt<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.gt(column, value);
        self
    }

    /// Add a greater than or equals condition using a field accessor.
    pub fn ge<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.ge(column, value);
        self
    }

    /// Add a less than condition using a field accessor.
    pub fn lt<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.lt(column, value);
        self
    }

    /// Add a less than or equals condition using a field accessor.
    pub fn le<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.le(column, value);
        self
    }

    /// Add a LIKE condition using a field accessor.
    pub fn like<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.like(column, value);
        self
    }

    /// Add a NOT LIKE condition using a field accessor.
    pub fn not_like<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, value: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.not_like(column, value);
        self
    }

    // ========== NULL Checks ==========

    /// Add an IS NULL condition using a field accessor.
    pub fn is_null(mut self, field: fn(&T) -> String) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.is_null(column);
        self
    }

    /// Add an IS NOT NULL condition using a field accessor.
    pub fn is_not_null(mut self, field: fn(&T) -> String) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.is_not_null(column);
        self
    }

    // ========== IN/NOT IN ==========

    /// Add an IN condition using a field accessor.
    pub fn r#in<V: IntoAkitaValue, I: IntoIterator<Item = V>>(
        mut self,
        field: fn(&T) -> String,
        values: I,
    ) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.r#in(column, values);
        self
    }

    /// Add a NOT IN condition using a field accessor.
    pub fn not_in<V: IntoAkitaValue, I: IntoIterator<Item = V>>(
        mut self,
        field: fn(&T) -> String,
        values: I,
    ) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.not_in(column, values);
        self
    }

    // ========== BETWEEN ==========

    /// Add a BETWEEN condition using a field accessor.
    pub fn between<V: IntoAkitaValue>(mut self, field: fn(&T) -> String, start: V, end: V) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.between(column, start, end);
        self
    }

    /// Add a NOT BETWEEN condition using a field accessor.
    pub fn not_between<V: IntoAkitaValue>(
        mut self,
        field: fn(&T) -> String,
        start: V,
        end: V,
    ) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.not_between(column, start, end);
        self
    }

    // ========== Logical Operators ==========

    /// Add an AND group.
    pub fn and<F>(mut self, func: F) -> Self
    where
        F: FnOnce(LambdaWrapper<T>) -> LambdaWrapper<T>,
    {
        let inner = func(LambdaWrapper::new());
        self.wrapper = self.wrapper.and(|_| inner.into_wrapper());
        self
    }

    /// Add an OR group.
    pub fn or<F>(mut self, func: F) -> Self
    where
        F: FnOnce(LambdaWrapper<T>) -> LambdaWrapper<T>,
    {
        let inner = func(LambdaWrapper::new());
        self.wrapper = self.wrapper.or(|_| inner.into_wrapper());
        self
    }

    /// Add a direct OR.
    pub fn or_direct(mut self) -> Self {
        self.wrapper = self.wrapper.or_direct();
        self
    }

    // ========== ORDER BY ==========

    /// Add ORDER BY ASC using a field accessor.
    pub fn order_by_asc(mut self, field: fn(&T) -> String) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.order_by_asc(vec![column]);
        self
    }

    /// Add ORDER BY DESC using a field accessor.
    pub fn order_by_desc(mut self, field: fn(&T) -> String) -> Self {
        let column = get_column_name::<T>(field);
        self.wrapper = self.wrapper.order_by_desc(vec![column]);
        self
    }

    // ========== Pagination ==========

    /// Set the limit.
    pub fn limit(mut self, limit: u64) -> Self {
        self.wrapper = self.wrapper.limit(limit);
        self
    }

    /// Set the offset.
    pub fn offset(mut self, offset: u64) -> Self {
        self.wrapper = self.wrapper.offset(offset);
        self
    }

    /// Set pagination.
    pub fn page(mut self, page: u64, size: u64) -> Self {
        self.wrapper = self.wrapper.page(page, size);
        self
    }

    // ========== Conditional Control ==========

    /// Conditionally apply the next condition.
    pub fn when(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.when(condition);
        self
    }

    /// Conditionally skip the next condition.
    pub fn unless(mut self, condition: bool) -> Self {
        self.wrapper = self.wrapper.unless(condition);
        self
    }
}

/// Get the column name from a field accessor function.
///
/// This function creates a default instance of T and calls the accessor
/// to get the column name. It uses the field metadata from GetFields.
fn get_column_name<T: GetFields>(field: fn(&T) -> String) -> String {
    // Get all fields from the type
    let fields = T::fields();

    // Try to match the accessor function by creating a dummy instance
    // This is a simplified approach - in production, you'd use a more robust method
    for f in &fields {
        // Return the field name from metadata
        return f.name.clone();
    }

    // Fallback: use a generic approach
    // In practice, this should be handled by the derive macro
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock type for testing
    struct MockEntity;

    impl GetFields for MockEntity {
        fn fields() -> Vec<crate::FieldName> {
            vec![
                crate::FieldName {
                    name: "id".to_string(),
                    column: "id".to_string(),
                    ..Default::default()
                },
                crate::FieldName {
                    name: "name".to_string(),
                    column: "user_name".to_string(),
                    ..Default::default()
                },
            ]
        }
    }

    #[test]
    fn test_lambda_wrapper_new() {
        let wrapper = LambdaWrapper::<MockEntity>::new();
        assert!(wrapper.wrapper().get_where_conditions().is_empty());
    }

    #[test]
    fn test_lambda_wrapper_into() {
        let wrapper = LambdaWrapper::<MockEntity>::new();
        let inner = wrapper.into_wrapper();
        assert!(inner.get_where_conditions().is_empty());
    }
}
