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
use async_trait::async_trait;
use tracing::{debug, error, info, trace, warn};
use akita_core::{InterceptorType, OperationType};
use crate::comm::ExecuteContext;
use crate::prelude::{AsyncAkitaInterceptor, ExecuteResult};
use crate::errors::{AkitaError, Result};
use crate::interceptor::{LogLevel, LoggingInterceptor};

/// Simplified log blocker - Focus on SQL execution logs
pub struct AsyncLoggingInterceptor {
    log_level: LogLevel,
    pub slow_query_threshold_ms: u64,
}

#[async_trait]
impl AsyncAkitaInterceptor for LoggingInterceptor {
    fn name(&self) -> &'static str {
        "logging"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Logging
    }

    fn order(&self) -> i32 {
        90
    }

    fn supports_operation(&self, _operation: &OperationType) -> bool {
        true
    }

    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }

    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        if self.log_level.should_log(LogLevel::Debug) {
            debug!("{} ==> [Akita]  Preparing: {}", timestamp, ctx.final_sql());
            println!("{} ==> [Akita]  Preparing: {}", timestamp, ctx.final_sql());

            let params_str = if !ctx.final_params().is_empty() {
                format!("{} {}", timestamp, ctx.final_params())
            } else {
                format!("{} ==> [Akita] Parameters: None", timestamp)
            };

            debug!("{}", params_str);
            println!("{}", params_str);

            // 如果是TRACE级别，记录更多信息
            if self.log_level.should_log(LogLevel::Trace) {
                println!("{} ==> [TRACE] Start execution at: {:?}", timestamp, ctx.start_time());
                trace!("{} ==> [Akita] Start execution at: {:?}", timestamp, ctx.start_time());

                println!("{} ==> [TRACE] Connection ID: {}", timestamp, ctx.connection_id().map(|v| v.to_string()).unwrap_or("N/A".to_string()));
                trace!("{} ==> [Akita] Connection ID: {}", timestamp, ctx.connection_id().map(|v| v.to_string()).unwrap_or("N/A".to_string()));
            }
        }
        Ok(())
    }

    async fn after_execute(&self, ctx: &mut ExecuteContext, result: &mut Result<ExecuteResult>) -> Result<()> {
        let duration_ms = ctx.start_time().elapsed().as_millis();
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");

        if let Err(err) = result {
            // 错误日志
            if self.log_level.should_log(LogLevel::Error) {
                println!("{} <== [Akita]    ERROR: {}", timestamp, err);
                error!("{} <== [Akita]    ERROR: {}", timestamp, err);
                println!("{} <== [Akita]    Failed SQL: {}", timestamp, ctx.final_sql());
                error!("{} <== [Akita]    Failed SQL: {}", timestamp, ctx.final_sql());

                if self.log_level.should_log(LogLevel::Debug) && !ctx.final_params().is_empty() {
                    println!("{} <== [Akita]    Failed with params: {:?}", timestamp, ctx.final_params());
                    debug!("{} <== [Akita]    Failed with params: {:?}", timestamp, ctx.final_params());
                }
            }
            return Ok(());
        }

        // 成功执行的日志
        let rows = if *ctx.operation_type() == OperationType::Select {
            result.as_ref().map(|row| row.len()).unwrap_or_default()
        } else {
            ctx.metrics().rows_affected
        };
        if duration_ms > self.slow_query_threshold_ms as u128 {
            // 慢查询警告
            if self.log_level.should_log(LogLevel::Warn) {
                println!("{} <== [Akita] Slow Query! Cost: {} ms, Rows: {}", timestamp, duration_ms, rows);
                warn!("{} <== [Akita] Slow Query! Cost: {} ms, Rows: {}", timestamp, duration_ms, rows);
            }
        }

        // 常规执行结果
        if self.log_level.should_log(LogLevel::Info) {
            println!("{} <== [Akita]      Total: {}, Cost: {} ms", timestamp, rows, duration_ms);
            info!("{} <==  [Akita]     Total: {}, Cost: {} ms", timestamp, rows, duration_ms);
        }

        // TRACE级别的详细信息
        if self.log_level.should_log(LogLevel::Trace) {
            println!("{} <== [Akita] End execution at: {:?}, Total duration: {} ms", timestamp, ctx.start_time().elapsed(), duration_ms);
            trace!("{} <== [Akita] End execution at: {:?}, Total duration: {} ms", timestamp, ctx.start_time().elapsed(), duration_ms);
        }

        Ok(())
    }
}