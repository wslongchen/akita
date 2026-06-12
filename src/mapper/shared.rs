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

//! Shared mapper utilities for both sync and async implementations.
//!
//! This module contains helper functions and types that are common to both
//! `AkitaMapper` (blocking) and `AsyncAkitaMapper` (non-blocking) traits.

use crate::data_err;
use crate::errors::Result;
use akita_core::{FromAkitaValue, Rows};

/// Process raw rows into a vector of deserialized objects.
///
/// Each row is converted from its `AkitaValue` representation into the target
/// type `R` using the `FromAkitaValue` trait. This is the core logic shared by
/// both sync and async `exec_raw` implementations.
///
/// # Parameters
/// - `rows`: The raw rows returned from a query.
///
/// # Returns
/// A `Vec<R>` of deserialized objects, one per input row.
pub fn process_rows_to_vec<R: FromAkitaValue>(rows: Rows) -> Vec<R> {
    rows.object_iter()
        .map(|data| R::from_value(&data))
        .collect()
}

/// Extract a single record from rows, returning an error if empty or multiple.
///
/// Expects exactly one row in the result set. Returns an error if zero rows or
/// more than one row is returned. This is the core logic shared by both sync
/// and async `exec_first` implementations.
///
/// # Parameters
/// - `rows`: The raw rows returned from a query.
///
/// # Returns
/// `Ok(R)` with the single deserialized record, or an `Err` if the result set
/// does not contain exactly one row.
pub fn extract_single<R: FromAkitaValue>(rows: Rows) -> Result<R> {
    let mut results = process_rows_to_vec::<R>(rows);
    match results.len() {
        0 => Err(data_err!("Empty record returned".to_string())),
        1 => Ok(results.remove(0)),
        _ => Err(data_err!("More than one record returned".to_string())),
    }
}

/// Extract an optional single record from rows.
///
/// Returns `Ok(None)` if the result set is empty, `Ok(Some(value))` if exactly
/// one row is present, or an error if multiple records are found. This is the
/// core logic shared by both sync and async `exec_first_opt` implementations.
///
/// # Parameters
/// - `rows`: The raw rows returned from a query.
///
/// # Returns
/// `Ok(None)` when no rows are found, `Ok(Some(R))` when exactly one row is
/// found, or an `Err` when more than one row is returned.
pub fn extract_optional<R: FromAkitaValue>(rows: Rows) -> Result<Option<R>> {
    let mut results = process_rows_to_vec::<R>(rows);
    match results.len() {
        0 => Ok(None),
        1 => Ok(Some(results.remove(0))),
        _ => Err(data_err!("More than one record returned".to_string())),
    }
}

/// Fold (reduce) rows using a function and an initial accumulator value.
///
/// Each row is deserialized to type `T` and then folded into the accumulator
/// of type `U` using the provided closure. This is the core logic shared by
/// both sync and async `query_fold` implementations.
///
/// # Parameters
/// - `rows`: The raw rows returned from a query.
/// - `init`: The initial accumulator value.
/// - `f`: A closure that combines the accumulator with each deserialized row.
///
/// # Returns
/// The final accumulated value after processing all rows.
pub fn fold_rows<T, F, U>(rows: Rows, init: U, mut f: F) -> U
where
    T: FromAkitaValue,
    F: FnMut(U, T) -> U,
{
    rows.object_iter()
        .map(|data| akita_core::from_akita_value(data))
        .fold(init, |acc, row| f(acc, row))
}

/// Map rows through a transformation function.
///
/// Each row is deserialized to type `T` and then transformed into type `U`
/// using the provided closure. This is the core logic shared by both sync and
/// async `query_map` implementations.
///
/// # Parameters
/// - `rows`: The raw rows returned from a query.
/// - `f`: A closure that transforms each deserialized row.
///
/// # Returns
/// A `Vec<U>` of transformed values, one per input row.
pub fn map_rows<T, F, U>(rows: Rows, mut f: F) -> Vec<U>
where
    T: FromAkitaValue,
    F: FnMut(T) -> U,
{
    fold_rows(rows, Vec::new(), |mut acc, row| {
        acc.push(f(row));
        acc
    })
}
