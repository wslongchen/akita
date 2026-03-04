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

use std::sync::atomic::{AtomicU8, Ordering};
use std::fmt;

/// Error traceback level
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorLevel {
    /// No backtracking (production environment, extreme performance)
    None = 0,
    /// Lightweight traceback (file only: line number, development environment)
    Light = 1,
    /// Full stack traceback (debugging complex issues)
    Full = 2,
}

impl From<u8> for ErrorLevel {
    fn from(level: u8) -> Self {
        match level {
            0 => ErrorLevel::None,
            1 => ErrorLevel::Light,
            _ => ErrorLevel::Full,
        }
    }
}

/// Global error level controller
pub(crate) static ERROR_LEVEL: AtomicU8 = AtomicU8::new(0);

/// Sets the error traceback level
#[inline]
pub(crate) fn set_error_level(level: ErrorLevel) {
    ERROR_LEVEL.store(level as u8, Ordering::Relaxed);
}

/// Gets the current error traceback level
#[inline]
pub(crate) fn error_level() -> ErrorLevel {
    ERROR_LEVEL.load(Ordering::Relaxed).into()
}

/// Lightweight location information (stack allocation, zero heap overhead)
#[derive(Debug, Clone, Copy)]
pub struct Location {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}

impl Location {
    #[inline(always)]
    pub const fn new(file: &'static str, line: u32, column: u32) -> Self {
        Self { file, line, column }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

/// Intelligent backtracking enumeration -automatically selects the optimal representation based on the level
#[derive(Debug)]
pub enum SmartBacktrace {
    /// No backtracking (empty struct, zero overhead)
    None,

    /// Location only (allocated on the stack)
    Location(Location),

    /// Full stack backtracking
    Full(std::backtrace::Backtrace),
}

impl SmartBacktrace {
    /// Capture the backtracking according to the current level
    #[inline]
    pub fn capture() -> Self {
        
        match error_level() {
            ErrorLevel::None => SmartBacktrace::None,
            ErrorLevel::Light => SmartBacktrace::Location(Location::new(
                file!(),
                line!(),
                column!(),
            )),
            ErrorLevel::Full => {
                #[cfg(feature = "backtrace")]
                let bt = std::backtrace::Backtrace::force_capture();

                #[cfg(not(feature = "backtrace"))]
                let bt = std::backtrace::Backtrace::capture();

                SmartBacktrace::Full(bt)
            }
        }
    }

    /// Get a string representation of the traceback information (for Display).
    pub fn to_display_string(&self) -> Option<String> {
        match self {
            SmartBacktrace::None => None,
            SmartBacktrace::Location(loc) => Some(format!(" at {}", loc)),
            SmartBacktrace::Full(bt) => Some(format!("\nBacktrace:\n{}", bt)),
        }
    }
}