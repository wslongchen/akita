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
use crate::errors::{AkitaError, Result};
use crate::interceptor::logging::LoggingInterceptor;
use crate::interceptor::non_blocking::AsyncAkitaInterceptor;
use crate::prelude::ExecuteResult;
use async_trait::async_trait;
use tracing::{debug, enabled, error, trace, warn, Level};

#[async_trait]
impl AsyncAkitaInterceptor for LoggingInterceptor {
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
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

    async fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        result: &mut Result<ExecuteResult>,
    ) -> Result<()> {
        let duration_ms = ctx.start_time().elapsed().as_millis();

        match result {
            Err(err) => {
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
                let rows = if *ctx.operation_type() == crate::interceptor::OperationType::Select {
                    exec_result.len()
                } else {
                    ctx.metrics().rows_affected
                };

                // Slow Query Warnings
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

                // SQL execution details (DEBUG level): full SQL + cost + rows + operation.
                // INFO no longer logs per-SQL events - SQL text belongs at DEBUG, timing
                // stats belong to the performance interceptor. Keeps `RUST_LOG=info` quiet
                // (only slow-query WARN + failure ERROR).
                if enabled!(target: "akita::sql", Level::DEBUG) {
                    debug!(
                        target: "akita::sql",
                        sql = %ctx.final_sql(),
                        cost_ms = duration_ms as u64,
                        rows = rows,
                        operation = ?ctx.operation_type(),
                        "SQL executed"
                    );
                }

                // Detailed tracking
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
