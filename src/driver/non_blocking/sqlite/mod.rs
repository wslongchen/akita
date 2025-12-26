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
mod connection;
mod adapter;


pub use connection::*;
pub use adapter::*;

use std::sync::Arc;
use async_trait::async_trait;
use akita_core::{OperationType, Params, Rows, SqlInjectionDetector, SqlSecurityConfig, TableName};
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::driver::non_blocking::AsyncDbExecutor;
use crate::interceptor::non_blocking::AsyncInterceptorChain;

/// SQLite asynchronous driver (uses blocking thread pool to wrap synchronous library)
#[derive(Clone)]
pub struct SqliteAsync {
    adapter: Arc<SqliteAsyncAdapter>,
    interceptor_chain: Option<Arc<AsyncInterceptorChain>>,
    sql_injection_detector: Option<SqlInjectionDetector>,
}

impl SqliteAsync {
    pub fn new(conn: SqliteAsyncConnection) -> Self {
        Self {
            adapter: Arc::new(SqliteAsyncAdapter::new(conn)),
            interceptor_chain: None,
            sql_injection_detector: None,
        }
    }

    pub fn with_interceptor_chain(mut self, interceptor_chain: Arc<AsyncInterceptorChain>) -> Self {
        self.interceptor_chain = Some(interceptor_chain);
        self
    }

    /// Set up SQL security configuration
    pub fn with_sql_security(mut self, sql_security_config: Option<SqlSecurityConfig>) -> Self {
        if let Some(sql_security_config) = sql_security_config {
            self.sql_injection_detector = Some(SqlInjectionDetector::with_config(sql_security_config));
        }
        self
    }

    pub fn interceptor_chain(&self) -> Option<Arc<AsyncInterceptorChain>> {
        self.interceptor_chain.clone()
    }

    async fn execute_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<ExecuteResult> {
        

        let mut ctx = ExecuteContext::new(
            sql.to_string(),
            params,
            TableName::parse_table_name(sql),
            OperationType::detect_operation_type(sql),
        );

        ctx.record_parse_complete();

        if let Some(chain) = &self.interceptor_chain {
            chain.before_query(&mut ctx).await?;

            if ctx.stop_propagation {
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(ExecuteResult::None);
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }

        let mut result = self.adapter
            .execute(ctx.final_sql(), ctx.final_params().clone())
            .await;

        if let Ok(_) = &result {
            ctx.set_connection_id(0); // SQLite 没有连接ID
            let rows_affected = self.adapter.affected_rows().await;
            ctx.record_execute_complete(rows_affected);
        }

        if let Some(chain) = &self.interceptor_chain {
            chain.after_query(&mut ctx, &mut result).await?;
        }

        ctx.record_query_metrics();
        result
    }

    async fn query_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<Rows> {
        

        let mut ctx = ExecuteContext::new(
            sql.to_string(),
            params,
            TableName::parse_table_name(sql),
            OperationType::detect_operation_type(sql),
        );

        ctx.record_parse_complete();

        if let Some(chain) = &self.interceptor_chain {
            chain.before_query(&mut ctx).await?;

            if ctx.stop_propagation {
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(Rows::new());
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }

        let mut result = self.adapter
            .query(ctx.final_sql(), ctx.final_params().clone())
            .await
            .map(ExecuteResult::Rows);

        if let Ok(_) = &result {
            ctx.set_connection_id(0);
            let rows_affected = self.adapter.affected_rows().await;
            ctx.record_execute_complete(rows_affected);
        }

        if let Some(chain) = &self.interceptor_chain {
            chain.after_query(&mut ctx, &mut result).await?;
        }

        ctx.record_query_metrics();
        result.map(|v| v.rows())
    }
}

#[async_trait]
impl AsyncDbExecutor for SqliteAsync {
    async fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        self.query_with_interceptors(sql, params).await
    }

    async fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        self.execute_with_interceptors(sql, params).await
    }

    async fn start(&self) -> crate::prelude::Result<()> {
        self.adapter.start_transaction().await
    }

    async fn commit(&self) -> crate::prelude::Result<()> {
        self.adapter.commit_transaction().await
    }

    async fn rollback(&self) -> crate::prelude::Result<()> {
        self.adapter.rollback_transaction().await
    }

    async fn affected_rows(&self) -> u64 {
        self.adapter.affected_rows().await
    }

    async fn last_insert_id(&self) -> u64 {
        self.adapter.last_insert_id().await
    }
}