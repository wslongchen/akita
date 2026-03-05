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
use tracing::level_filters::LevelFilter;

#[inline]
fn current_backtrace_mode() -> BacktraceMode {
    match LevelFilter::current() {
        LevelFilter::TRACE | LevelFilter::DEBUG => BacktraceMode::Full,
        LevelFilter::INFO => BacktraceMode::Light,
        LevelFilter::WARN | LevelFilter::ERROR | LevelFilter::OFF => BacktraceMode::None,
    }
}

/// Error traceback level
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktraceMode {
    /// No backtracking (production environment, extreme performance)
    None = 0,
    /// Lightweight traceback (file only: line number, development environment)
    Light = 1,
    /// Full stack traceback (debugging complex issues)
    Full = 2,
}

impl From<u8> for BacktraceMode {
    fn from(level: u8) -> Self {
        match level {
            0 => BacktraceMode::None,
            1 => BacktraceMode::Light,
            _ => BacktraceMode::Full,
        }
    }
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

        match current_backtrace_mode() {
            BacktraceMode::None => SmartBacktrace::None,
            BacktraceMode::Light => SmartBacktrace::Location(Location::new(
                file!(),
                line!(),
                column!(),
            )),
            BacktraceMode::Full => {
                #[cfg(feature = "backtrace-symbols")]
                let bt = std::backtrace::Backtrace::force_capture();
                #[cfg(not(feature = "backtrace-symbols"))]
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