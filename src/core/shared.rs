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

/// Wrapper builder methods shared between sync and async QueryBuilder/UpdateBuilder.
///
/// These methods delegate to the underlying `Wrapper` and return the modified wrapper.
pub mod wrapper_delegates {
    use super::*;

    // ========== Basic Conditions ==========

    pub fn eq<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.eq(column, value)
    }

    pub fn ne<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.ne(column, value)
    }

    pub fn gt<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.gt(column, value)
    }

    pub fn ge<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.ge(column, value)
    }

    pub fn lt<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.lt(column, value)
    }

    pub fn le<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.le(column, value)
    }

    pub fn like<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.like(column, value)
    }

    pub fn not_like<S: Into<String>, V: Into<AkitaValue>>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.not_like(column, value)
    }

    // ========== NULL Checks ==========

    pub fn is_null<S: Into<String>>(wrapper: Wrapper, column: S) -> Wrapper {
        wrapper.is_null(column)
    }

    pub fn is_not_null<S: Into<String>>(wrapper: Wrapper, column: S) -> Wrapper {
        wrapper.is_not_null(column)
    }

    // ========== IN/NOT IN ==========

    pub fn r#in<S, V, I>(wrapper: Wrapper, column: S, values: I) -> Wrapper
    where
        S: Into<String>,
        V: IntoAkitaValue,
        I: IntoIterator<Item = V>,
    {
        wrapper.r#in(column, values)
    }

    pub fn not_in<S, V, I>(wrapper: Wrapper, column: S, values: I) -> Wrapper
    where
        S: Into<String>,
        V: IntoAkitaValue,
        I: IntoIterator<Item = V>,
    {
        wrapper.not_in(column, values)
    }

    // ========== BETWEEN ==========

    pub fn between<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        start: V,
        end: V,
    ) -> Wrapper {
        wrapper.between(column, start, end)
    }

    pub fn not_between<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        start: V,
        end: V,
    ) -> Wrapper {
        wrapper.not_between(column, start, end)
    }

    // ========== Logical Operators ==========

    pub fn and<F>(wrapper: Wrapper, func: F) -> Wrapper
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        wrapper.and(func)
    }

    pub fn or<F>(wrapper: Wrapper, func: F) -> Wrapper
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        wrapper.or(func)
    }

    pub fn or_direct(wrapper: Wrapper) -> Wrapper {
        wrapper.or_direct()
    }

    // ========== JOIN ==========

    pub fn inner_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.inner_join(table, condition)
    }

    pub fn left_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.left_join(table, condition)
    }

    pub fn right_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.right_join(table, condition)
    }

    pub fn full_join<S: Into<String>, C: Into<String>>(
        wrapper: Wrapper,
        table: S,
        condition: C,
    ) -> Wrapper {
        wrapper.full_join(table, condition)
    }

    // ========== GROUP BY / HAVING ==========

    pub fn group_by<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.group_by(columns)
    }

    pub fn having<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        operator: SqlOperator,
        value: V,
    ) -> Wrapper {
        wrapper.having(column, operator, value)
    }

    // ========== ORDER BY ==========

    pub fn order_by_asc<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.order_by_asc(columns)
    }

    pub fn order_by_desc<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.order_by_desc(columns)
    }

    // ========== Pagination ==========

    pub fn limit(wrapper: Wrapper, limit: u64) -> Wrapper {
        wrapper.limit(limit)
    }

    pub fn offset(wrapper: Wrapper, offset: u64) -> Wrapper {
        wrapper.offset(offset)
    }

    pub fn page(wrapper: Wrapper, page: u64, size: u64) -> Wrapper {
        wrapper.page(page, size)
    }

    // ========== Conditional Control ==========

    pub fn when(wrapper: Wrapper, condition: bool) -> Wrapper {
        wrapper.when(condition)
    }

    pub fn unless(wrapper: Wrapper, condition: bool) -> Wrapper {
        wrapper.unless(condition)
    }

    pub fn skip_next(wrapper: Wrapper) -> Wrapper {
        wrapper.skip_next()
    }

    // ========== Raw SQL ==========

    pub fn apply<S: Into<String>>(
        wrapper: Wrapper,
        sql: S,
        params: Option<Vec<AkitaValue>>,
    ) -> Wrapper {
        wrapper.apply(sql, params)
    }

    pub fn apply_raw<S: Into<String>>(wrapper: Wrapper, sql: S) -> Wrapper {
        wrapper.apply_raw(sql)
    }

    // ========== SET Operations (for UPDATE) ==========

    pub fn set<S: Into<String>, V: IntoAkitaValue>(
        wrapper: Wrapper,
        column: S,
        value: V,
    ) -> Wrapper {
        wrapper.set(column, value)
    }

    pub fn set_multiple<S: Into<String>, V: IntoAkitaValue, I: IntoIterator<Item = (S, V)>>(
        wrapper: Wrapper,
        operations: I,
    ) -> Wrapper {
        wrapper.set_multiple(operations)
    }

    // ========== SELECT ==========

    pub fn select<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.select(columns)
    }

    pub fn select_distinct<S: Into<String>>(wrapper: Wrapper, columns: Vec<S>) -> Wrapper {
        wrapper.select_distinct(columns)
    }

    // ========== Table ==========

    pub fn table<S: Into<String>>(wrapper: Wrapper, table: S) -> Wrapper {
        wrapper.table(table)
    }

    pub fn alias<S: Into<String>>(wrapper: Wrapper, alias: S) -> Wrapper {
        wrapper.alias(alias)
    }
}
