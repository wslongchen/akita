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
use std::collections::HashSet;
use akita_core::{cfg_if, InterceptorType, OperationType};

mod logging;

pub use logging::LoggingInterceptor;

cfg_if! {if #[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync", 
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))] {
    pub mod blocking;
}}

cfg_if! {if #[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async", 
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))] {
    pub mod non_blocking;
}}

/// Interceptor configuration
#[derive(Debug, Clone)]
pub struct InterceptorConfig {
    pub enable_async: bool,
    pub enable_metrics: bool,
    pub enable_tracing: bool,
    pub max_interceptor_depth: usize,
    pub timeout_ms: u64,
}

impl Default for InterceptorConfig {
    fn default() -> Self {
        Self {
            enable_async: true,
            enable_metrics: true,
            enable_tracing: true,
            max_interceptor_depth: 10,
            timeout_ms: 5000,
        }
    }
}




/// Interceptor configuration items
#[derive(Debug, Clone)]
pub struct InterceptorConfigItem {
    pub enabled: bool,
    pub order: i32,
    pub ignored_tables: HashSet<String>,
    pub supported_operations: HashSet<OperationType>,
}


/// Log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    /// Tracking level (lowest priority)
    Trace = 1,
    /// Debug level
    Debug = 2,
    /// Information level
    Info = 3,
    /// Warning level
    Warn = 4,
    /// Error Level (Highest Priority)
    Error = 5,
}

impl LogLevel {
    /// Parsing logs from the string level
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ERROR" | "ERR" => Some(LogLevel::Error),
            "WARN" | "WARNING" => Some(LogLevel::Warn),
            "INFO" => Some(LogLevel::Info),
            "DEBUG" => Some(LogLevel::Debug),
            "TRACE" => Some(LogLevel::Trace),
            _ => None,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "ERROR",
            LogLevel::Warn => "WARN",
            LogLevel::Info => "INFO",
            LogLevel::Debug => "DEBUG",
            LogLevel::Trace => "TRACE",
        }
    }

    /// Check if a level is recorded
    pub fn should_log(&self, other: LogLevel) -> bool {
        *self <= other
    }
}

impl PartialOrd for LogLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((*self as u8).cmp(&(*other as u8)))
    }
}

impl Ord for LogLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}