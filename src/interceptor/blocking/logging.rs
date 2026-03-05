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
use tracing::{debug, enabled, error, info, trace, warn, Level};
use crate::comm::ExecuteContext;
use crate::prelude::{ExecuteResult};
use crate::errors::{AkitaError, Result};
use crate::interceptor::{InterceptorType, LogLevel, OperationType};
use crate::interceptor::blocking::AkitaInterceptor;
use crate::interceptor::logging::LoggingInterceptor;

impl AkitaInterceptor for LoggingInterceptor {
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

    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Preparation logs are only recorded at DEBUG level and above
        if enabled!(target: "akita::sql", Level::DEBUG) {
            debug!(
                target: "akita::sql",
                sql = %ctx.final_sql(),
                params = ?ctx.final_params(),
                "Preparing SQL"
            );
        }

        // More context is recorded at the TRACE level
        if enabled!(target: "akita::sql", Level::TRACE) {
            trace!(
                target: "akita::sql",
                connection_id = ?ctx.connection_id(),
                "Start execution"
            );
        }

        Ok(())
    }

    fn after_execute(&self, ctx: &mut ExecuteContext, result: &mut Result<ExecuteResult>) -> Result<()> {
        let duration_ms = ctx.start_time().elapsed().as_millis();
        // Processing execution results
        match result {
            Err(err) => {
                // 错误日志（ERROR 级别）
                if enabled!(target: "akita::sql", Level::ERROR) {
                    error!(
                        target: "akita::sql",
                        error = %err,
                        sql = %ctx.final_sql(),
                        params = ?ctx.final_params(),
                        cost_ms = duration_ms as u64,
                        "SQL execution failed"
                    );
                }
                Ok(())
            }
            Ok(exec_result) => {
                // Gets the number of affected rows or query result rows
                let rows = if *ctx.operation_type() == OperationType::Select {
                    exec_result.len()
                } else {
                    ctx.metrics().rows_affected
                };

                // Slow Query warning (WARN level)
                if duration_ms > self.slow_query_threshold_ms as u128
                    && enabled!(target: "akita::sql", Level::WARN)
                {
                    warn!(
                        target: "akita::sql",
                        cost_ms = duration_ms as u64,
                        rows = rows,
                        sql = %ctx.final_sql(),
                        "Slow query detected"
                    );
                }

                // General success log (INFO level)
                if enabled!(target: "akita::sql", Level::INFO) {
                    info!(
                        target: "akita::sql",
                        cost_ms = duration_ms as u64,
                        rows = rows,
                        operation = ?ctx.operation_type(),
                        "SQL executed"
                    );
                }

                // Detailed tracking (TRACE level)
                if enabled!(target: "akita::sql", Level::TRACE) {
                    trace!(
                        target: "akita::sql",
                        cost_ms = duration_ms as u64,
                        rows = rows,
                        sql = %ctx.final_sql(),
                        params = ?ctx.final_params(),
                        "Execution finished"
                    );
                }
                Ok(())
            }
        }
    }
}