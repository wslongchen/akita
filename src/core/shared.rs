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

//! Shared core utilities for both sync and async implementations.
//!
//! This module contains helper functions and types shared between
//! `QueryBuilder`/`UpdateBuilder` (blocking) and their async counterparts.

use akita_core::{AkitaValue, IntoAkitaValue, SqlOperator, Wrapper};

/// Wrapper builder delegate methods shared between sync and async `QueryBuilder`/`UpdateBuilder`.
///
/// Each function in this module takes ownership of a `Wrapper`, delegates to the
/// corresponding `Wrapper` method, and returns the modified wrapper. This avoids
/// code duplication between the blocking and non-blocking builder implementations.
pub mod wrapper_delegates {
    use super::*;

    // ========== Basic Conditions ==========

    /// Add an equals (`=`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    ///
    /// # Returns
    /// The modified `Wrapper` with the condition added.
    pub fn eq<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.eq(column, value)
    }

    /// Add a not equals (`!=`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    pub fn ne<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.ne(column, value)
    }

    /// Add a greater than (`>`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    pub fn gt<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.gt(column, value)
    }

    /// Add a greater than or equals (`>=`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    pub fn ge<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.ge(column, value)
    }

    /// Add a less than (`<`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    pub fn lt<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.lt(column, value)
    }

    /// Add a less than or equals (`<=`) condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The value to compare against.
    pub fn le<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.le(column, value)
    }

    /// Add a `LIKE` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The pattern to match against (supports `%` and `_` wildcards).
    pub fn like<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.like(column, value)
    }

    /// Add a `NOT LIKE` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to compare.
    /// - `value`: The pattern to exclude.
    pub fn not_like<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.not_like(column, value)
    }

    // ========== NULL Checks ==========

    /// Add an `IS NULL` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check for null.
    pub fn is_null<S: Into<String>>(wrapper: Wrapper, column: S) -> Wrapper {
        wrapper.is_null(column)
    }

    /// Add an `IS NOT NULL` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check for non-null.
    pub fn is_not_null<S: Into<String>>(wrapper: Wrapper, column: S) -> Wrapper {
        wrapper.is_not_null(column)
    }

    // ========== IN/NOT IN ==========

    /// Add an `IN` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check.
    /// - `values`: An iterable of values to match against.
    pub fn r#in<S, V, I>(wrapper: Wrapper, column: S, values: I) -> Wrapper
    where
        S: Into<String>,
        V: IntoAkitaValue,
        I: IntoIterator<Item = V>,
    {
        wrapper.r#in(column, values)
    }

    /// Add a `NOT IN` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check.
    /// - `values`: An iterable of values to exclude.
    pub fn not_in<S, V, I>(wrapper: Wrapper, column: S, values: I) -> Wrapper
    where
        S: Into<String>,
        V: IntoAkitaValue,
        I: IntoIterator<Item = V>,
    {
        wrapper.not_in(column, values)
    }

    // ========== BETWEEN ==========

    /// Add a `BETWEEN` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check.
    /// - `start`: The lower bound value (inclusive).
    /// - `end`: The upper bound value (inclusive).
    pub fn between<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        start: V,
        end: V,
    ) -> Wrapper {
        wrapper.between(column, start, end)
    }

    /// Add a `NOT BETWEEN` condition to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to check.
    /// - `start`: The lower bound value (inclusive).
    /// - `end`: The upper bound value (inclusive).
    pub fn not_between<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        start: V,
        end: V,
    ) -> Wrapper {
        wrapper.not_between(column, start, end)
    }

    // ========== Logical Operators ==========

    /// Add an `AND` group of conditions to the wrapper.
    ///
    /// The closure receives a fresh `Wrapper` and should return it with the
    /// desired conditions applied. The result is wrapped in parentheses and
    /// joined with `AND`.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `func`: A closure that configures the inner wrapper.
    pub fn and<F>(wrapper: Wrapper, func: F) -> Wrapper
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        wrapper.and(func)
    }

    /// Add an `OR` group of conditions to the wrapper.
    ///
    /// The closure receives a fresh `Wrapper` and should return it with the
    /// desired conditions applied. The result is wrapped in parentheses and
    /// joined with `OR`.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `func`: A closure that configures the inner wrapper.
    pub fn or<F>(wrapper: Wrapper, func: F) -> Wrapper
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        wrapper.or(func)
    }

    /// Add a direct `OR` separator between the previous and next condition.
    ///
    /// Unlike [`and()`](Self::and) / [`or()`](Self::or), this does not create
    /// a parenthesized group; it simply inserts `OR` at the current position.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    pub fn or_direct(wrapper: Wrapper) -> Wrapper {
        wrapper.or_direct()
    }

    // ========== JOIN ==========

    /// Add an `INNER JOIN` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `table`: The table name to join.
    /// - `condition`: The join condition (e.g., `"users.id = orders.user_id"`).
    pub fn inner_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.inner_join(table, condition)
    }

    /// Add a `LEFT JOIN` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `table`: The table name to join.
    /// - `condition`: The join condition.
    pub fn left_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.left_join(table, condition)
    }

    /// Add a `RIGHT JOIN` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `table`: The table name to join.
    /// - `condition`: The join condition.
    pub fn right_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.right_join(table, condition)
    }

    /// Add a `FULL JOIN` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `table`: The table name to join.
    /// - `condition`: The join condition.
    pub fn full_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.full_join(table, condition)
    }

    // ========== GROUP BY / HAVING ==========

    /// Add a `GROUP BY` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `columns`: A vector of column names to group by.
    pub fn group_by<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.group_by(columns)
    }

    /// Add a `HAVING` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column or aggregate expression to filter on.
    /// - `operator`: The comparison operator (e.g., `SqlOperator::Gt`).
    /// - `value`: The value to compare against.
    pub fn having<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        operator: SqlOperator,
        value: V,
    ) -> Wrapper {
        wrapper.having(column, operator, value)
    }

    // ========== ORDER BY ==========

    /// Add an `ORDER BY ASC` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `columns`: A vector of column names to sort by in ascending order.
    pub fn order_by_asc<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.order_by_asc(columns)
    }

    /// Add an `ORDER BY DESC` clause to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `columns`: A vector of column names to sort by in descending order.
    pub fn order_by_desc<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.order_by_desc(columns)
    }

    // ========== Pagination ==========

    /// Set the maximum number of rows to return.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `limit`: The maximum number of rows.
    pub fn limit(wrapper: Wrapper, limit: u64) -> Wrapper {
        wrapper.limit(limit)
    }

    /// Set the number of rows to skip before returning results.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `offset`: The number of rows to skip.
    pub fn offset(wrapper: Wrapper, offset: u64) -> Wrapper {
        wrapper.offset(offset)
    }

    /// Set pagination by page number and page size.
    ///
    /// Computes the appropriate `LIMIT` and `OFFSET` from the given page
    /// number and size.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `page`: The 1-based page number.
    /// - `size`: The number of rows per page.
    pub fn page(wrapper: Wrapper, page: u64, size: u64) -> Wrapper {
        wrapper.page(page, size)
    }

    // ========== Conditional Control ==========

    /// Conditionally apply the next condition.
    ///
    /// If `condition` is `true`, the next chained condition is applied normally.
    /// If `false`, the next condition is silently skipped.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `condition`: Whether the next condition should be applied.
    pub fn when(wrapper: Wrapper, condition: bool) -> Wrapper {
        wrapper.when(condition)
    }

    /// Conditionally skip the next condition.
    ///
    /// This is the inverse of [`when()`](Self::when). If `condition` is `true`,
    /// the next condition is skipped. If `false`, it is applied normally.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `condition`: Whether the next condition should be skipped.
    pub fn unless(wrapper: Wrapper, condition: bool) -> Wrapper {
        wrapper.unless(condition)
    }

    /// Skip the next condition in the chain.
    ///
    /// The immediately following condition call will be ignored.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    pub fn skip_next(wrapper: Wrapper) -> Wrapper {
        wrapper.skip_next()
    }

    // ========== Raw SQL ==========

    /// Apply a raw SQL fragment with optional parameters to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `sql`: The raw SQL fragment to append.
    /// - `params`: Optional bind parameters for the SQL fragment.
    pub fn apply<S: Into<String>>(
        wrapper: Wrapper,
        sql: S,
        params: Option<Vec<AkitaValue>>,
    ) -> Wrapper {
        wrapper.apply(sql, params)
    }

    /// Apply a raw SQL fragment (without parameters) to the wrapper.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `sql`: The raw SQL fragment to append.
    pub fn apply_raw<S: Into<String>>(wrapper: Wrapper, sql: S) -> Wrapper {
        wrapper.apply_raw(sql)
    }

    // ========== SET Operations (for UPDATE) ==========

    /// Add a `SET column = value` assignment for an UPDATE statement.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `column`: The column name to set.
    /// - `value`: The value to assign.
    pub fn set<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.set(column, value)
    }

    /// Add multiple `SET` assignments for an UPDATE statement.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `operations`: An iterable of `(column, value)` pairs to set.
    pub fn set_multiple<S: Into<String>, V: IntoAkitaValue, I: IntoIterator<Item = (S, V)>>(
        wrapper: Wrapper,
        operations: I,
    ) -> Wrapper {
        wrapper.set_multiple(operations)
    }

    // ========== SELECT ==========

    /// Set the columns to select.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `columns`: A vector of column names or expressions to select.
    pub fn select<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.select(columns)
    }

    /// Set the columns to select with `DISTINCT`.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `columns`: A vector of column names or expressions to select.
    pub fn select_distinct<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.select_distinct(columns)
    }

    // ========== Table ==========

    /// Set the target table for the query.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `table`: The table name.
    pub fn table<S: Into<String>>(wrapper: Wrapper, table: S) -> Wrapper {
        wrapper.table(table)
    }

    /// Set a table alias.
    ///
    /// # Parameters
    /// - `wrapper`: The query wrapper to modify.
    /// - `alias`: The alias name for the table.
    pub fn alias<S: Into<String>>(wrapper: Wrapper, alias: S) -> Wrapper {
        wrapper.alias(alias)
    }
}
