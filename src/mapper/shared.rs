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
use crate::errors::{AkitaError, Result};
use akita_core::{AkitaValue, FromAkitaValue, IntoAkitaValue, Rows};

/// Process raw rows into a vector of deserialized objects.
///
/// This is the core logic shared by both sync and async `exec_raw` implementations.
pub fn process_rows_to_vec<R: FromAkitaValue>(rows: Rows) -> Vec<R> {
    rows.object_iter()
        .map(|data| R::from_value(&data))
        .collect()
}

/// Extract a single record from rows, returning an error if empty or multiple.
///
/// This is the core logic shared by both sync and async `exec_first` implementations.
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
/// Returns `Ok(None)` if empty, `Ok(Some(value))` if exactly one,
/// or an error if multiple records found.
///
/// This is the core logic shared by both sync and async `exec_first_opt` implementations.
pub fn extract_optional<R: FromAkitaValue>(rows: Rows) -> Result<Option<R>> {
    let mut results = process_rows_to_vec::<R>(rows);
    match results.len() {
        0 => Ok(None),
        1 => Ok(Some(results.remove(0))),
        _ => Err(data_err!("More than one record returned".to_string())),
    }
}

/// Fold rows using a function and initial value.
///
/// This is the core logic shared by both sync and async `query_fold` implementations.
pub fn fold_rows<T, F, U>(rows: Rows, init: U, mut f: F) -> U
where
    T: FromAkitaValue,
    F: FnMut(U, T) -> U,
{
    rows.object_iter()
        .map(|data| akita_core::from_akita_value(data))
        .fold(init, |acc, row| f(acc, row))
}

/// Map rows using a transformation function.
///
/// This is the core logic shared by both sync and async `query_map` implementations.
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
