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
use crate::comm::ExecuteContext;
use crate::prelude::{ExecuteResult};
use crate::errors::{AkitaError, Result};
use crate::interceptor::{InterceptorType, LogLevel, OperationType};

/// Simplified log blocker - Focus on SQL execution logs
pub struct LoggingInterceptor {
    pub log_level: LogLevel,
    pub slow_query_threshold_ms: u64,
}

impl LoggingInterceptor {
    pub fn new() -> Self {
        Self {
            log_level: LogLevel::Debug,
            slow_query_threshold_ms: 1000,
        }
    }

    pub fn with_log_level(mut self, level: LogLevel) -> Self {
        self.log_level = level;
        self
    }

    pub fn with_slow_query_threshold(mut self, threshold_ms: u64) -> Self {
        self.slow_query_threshold_ms = threshold_ms;
        self
    }
}


impl Default for LoggingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

