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

//! SubQuery Builder - Build subqueries for IN, EXISTS, etc.
//!
//! This module provides `SubQuery` for building subqueries that can be used
//! with `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` conditions.
//!
//! # Example
//! ```ignore
//! use akita::prelude::*;
//!
//! // IN subquery
//! let sub = SubQuery::in_query("user_id")
//!     .select(vec!["id"])
//!     .from("users")
//!     .where_eq("status", "active");
//!
//! let wrapper = Wrapper::new()
//!     .in_subquery("id", sub);
//!
//! // EXISTS subquery
//! let sub = SubQuery::exists()
//!     .select(vec!["1"])
//!     .from("orders")
//!     .where_eq("orders.user_id", "users.id");
//!
//! let wrapper = Wrapper::new()
//!     .exists_subquery(sub);
//! ```

use crate::{AkitaValue, IntoAkitaValue, SqlOperator, Wrapper};

/// Subquery type
#[derive(Debug, Clone, PartialEq)]
pub enum SubQueryType {
    /// IN (SELECT ...)
    In,
    /// NOT IN (SELECT ...)
    NotIn,
    /// EXISTS (SELECT ...)
    Exists,
    /// NOT EXISTS (SELECT ...)
    NotExists,
}

/// SubQuery builder for constructing subqueries.
///
/// This builder allows constructing subqueries that can be used with
/// `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` conditions.
pub struct SubQuery {
    wrapper: Wrapper,
    query_type: SubQueryType,
    column: Option<String>,
}

impl SubQuery {
    /// Create a new IN subquery for a specific column.
    ///
    /// # Example
    /// ```ignore
    /// let sub = SubQuery::in_query("user_id")
    ///     .select(vec!["id"])
    ///     .from("users")
    ///     .where_eq("status", "active");
    /// ```
    pub fn in_query(column: &str) -> Self {
        Self {
            wrapper: Wrapper::new(),
            query_type: SubQueryType::In,
            column: Some(column.to_string()),
        }
    }

    /// Create a new NOT IN subquery for a specific column.
    pub fn not_in_query(column: &str) -> Self {
        Self {
            wrapper: Wrapper::new(),
            query_type: SubQueryType::NotIn,
            column: Some(column.to_string()),
        }
    }

    /// Create a new EXISTS subquery.
    ///
    /// # Example
    /// ```ignore
    /// let sub = SubQuery::exists()
    ///     .select(vec!["1"])
    ///     .from("orders")
    ///     .where_eq("orders.user_id", "users.id");
    /// ```
    pub fn exists() -> Self {
        Self {
            wrapper: Wrapper::new(),
            query_type: SubQueryType::Exists,
            column: None,
        }
    }

    /// Create a new NOT EXISTS subquery.
    pub fn not_exists() -> Self {
        Self {
            wrapper: Wrapper::new(),
            query_type: SubQueryType::NotExists,
            column: None,
        }
    }

    /// Set the SELECT columns.
    pub fn select<S: Into<String>>(mut self, columns: Vec<S>) -> Self {
        self.wrapper = self.wrapper.select(columns);
        self
    }

    /// Set the FROM table.
    pub fn from<S: Into<String>>(mut self, table: S) -> Self {
        self.wrapper = self.wrapper.table(table);
        self
    }

    /// Set the table alias.
    pub fn alias<S: Into<String>>(mut self, alias: S) -> Self {
        self.wrapper = self.wrapper.alias(alias);
        self
    }

    // ========== WHERE conditions ==========

    /// Add an equals condition.
    pub fn where_eq<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.eq(column, value);
        self
    }

    /// Add a not equals condition.
    pub fn where_ne<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.ne(column, value);
        self
    }

    /// Add a greater than condition.
    pub fn where_gt<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.gt(column, value);
        self
    }

    /// Add a greater than or equals condition.
    pub fn where_ge<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.ge(column, value);
        self
    }

    /// Add a less than condition.
    pub fn where_lt<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.lt(column, value);
        self
    }

    /// Add a less than or equals condition.
    pub fn where_le<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.le(column, value);
        self
    }

    /// Add a LIKE condition.
    pub fn where_like<S: Into<String>, V: IntoAkitaValue>(mut self, column: S, value: V) -> Self {
        self.wrapper = self.wrapper.like(column, value);
        self
    }

    /// Add an IS NULL condition.
    pub fn where_is_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_null(column);
        self
    }

    /// Add an IS NOT NULL condition.
    pub fn where_is_not_null<S: Into<String>>(mut self, column: S) -> Self {
        self.wrapper = self.wrapper.is_not_null(column);
        self
    }

    /// Add an IN condition.
    pub fn where_in<S, V, I>(mut self, column: S, values: I) -> Self
    where
        S: Into<String>,
        V: IntoAkitaValue,
        I: IntoIterator<Item = V>,
    {
        self.wrapper = self.wrapper.r#in(column, values);
        self
    }

    /// Add a BETWEEN condition.
    pub fn where_between<S: Into<String>, V: IntoAkitaValue>(
        mut self,
        column: S,
        start: V,
        end: V,
    ) -> Self {
        self.wrapper = self.wrapper.between(column, start, end);
        self
    }

    // ========== Build ==========

    /// Build the subquery SQL.
    pub fn build(self) -> (SubQueryType, Option<String>, String) {
        let sql = self.wrapper.build_select_sql();
        (self.query_type, self.column, sql)
    }

    /// Get the query type.
    pub fn query_type(&self) -> &SubQueryType {
        &self.query_type
    }

    /// Get the column (for IN/NOT IN queries).
    pub fn column(&self) -> Option<&String> {
        self.column.as_ref()
    }

    /// Get the underlying wrapper.
    pub fn wrapper(&self) -> &Wrapper {
        &self.wrapper
    }
}

/// Extension methods for Wrapper to support subqueries.
impl Wrapper {
    /// Add an IN subquery condition.
    ///
    /// # Example
    /// ```ignore
    /// let sub = SubQuery::in_query("user_id")
    ///     .select(vec!["id"])
    ///     .from("users")
    ///     .where_eq("status", "active");
    ///
    /// let wrapper = Wrapper::new()
    ///     .in_subquery("id", sub);
    /// ```
    pub fn in_subquery<S: Into<String>>(self, column: S, subquery: SubQuery) -> Self {
        let (query_type, _, sql) = subquery.build();
        let column_str = column.into();

        match query_type {
            SubQueryType::In => self.apply_raw(format!("{} IN ({})", column_str, sql)),
            SubQueryType::NotIn => self.apply_raw(format!("{} NOT IN ({})", column_str, sql)),
            _ => self,
        }
    }

    /// Add an EXISTS subquery condition.
    ///
    /// # Example
    /// ```ignore
    /// let sub = SubQuery::exists()
    ///     .select(vec!["1"])
    ///     .from("orders")
    ///     .where_eq("orders.user_id", "users.id");
    ///
    /// let wrapper = Wrapper::new()
    ///     .exists_subquery(sub);
    /// ```
    pub fn exists_subquery(self, subquery: SubQuery) -> Self {
        let (query_type, _, sql) = subquery.build();

        match query_type {
            SubQueryType::Exists => self.apply_raw(format!("EXISTS ({})", sql)),
            SubQueryType::NotExists => self.apply_raw(format!("NOT EXISTS ({})", sql)),
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subquery_in() {
        let sub = SubQuery::in_query("user_id")
            .select(vec!["id"])
            .from("users")
            .where_eq("status", "active");

        let (query_type, column, sql) = sub.build();
        assert_eq!(query_type, SubQueryType::In);
        assert_eq!(column, Some("user_id".to_string()));
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("users"));
    }

    #[test]
    fn test_subquery_not_in() {
        let sub = SubQuery::not_in_query("id")
            .select(vec!["id"])
            .from("users")
            .where_eq("status", "inactive");

        let (query_type, column, _) = sub.build();
        assert_eq!(query_type, SubQueryType::NotIn);
        assert_eq!(column, Some("id".to_string()));
    }

    #[test]
    fn test_subquery_exists() {
        let sub = SubQuery::exists()
            .select(vec!["1"])
            .from("orders")
            .where_eq("orders.user_id", "users.id");

        let (query_type, column, sql) = sub.build();
        assert_eq!(query_type, SubQueryType::Exists);
        assert_eq!(column, None);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_subquery_not_exists() {
        let sub = SubQuery::not_exists()
            .select(vec!["1"])
            .from("orders")
            .where_eq("orders.user_id", "users.id");

        let (query_type, _, _) = sub.build();
        assert_eq!(query_type, SubQueryType::NotExists);
    }

    #[test]
    fn test_wrapper_in_subquery() {
        let sub = SubQuery::in_query("user_id")
            .select(vec!["id"])
            .from("users")
            .where_eq("status", "active");

        let wrapper = Wrapper::new().in_subquery("id", sub);
        let sql = wrapper.build_select_sql();
        assert!(sql.contains("IN"), "Expected IN in SQL: {}", sql);
    }

    #[test]
    fn test_wrapper_exists_subquery() {
        let sub = SubQuery::exists()
            .select(vec!["1"])
            .from("orders")
            .where_eq("orders.user_id", "users.id");

        let wrapper = Wrapper::new().exists_subquery(sub);
        let sql = wrapper.build_select_sql();
        assert!(sql.contains("EXISTS"), "Expected EXISTS in SQL: {}", sql);
    }

    #[test]
    fn test_subquery_multiple_conditions() {
        let sub = SubQuery::in_query("id")
            .select(vec!["id"])
            .from("users")
            .where_eq("status", "active")
            .where_gt("age", 18);

        let (_, _, sql) = sub.build();
        assert!(sql.contains("AND"), "Expected AND in SQL: {}", sql);
    }

    #[test]
    fn test_subquery_with_alias() {
        let sub = SubQuery::in_query("user_id")
            .select(vec!["id"])
            .from("users")
            .alias("u")
            .where_eq("u.status", "active");

        let (_, _, sql) = sub.build();
        assert!(sql.contains("u"), "Expected alias in SQL: {}", sql);
    }
}
