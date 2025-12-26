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

//!
//! MySQL modules.
//!

mod adapter;
mod connection;

pub use adapter::*;
pub use connection::*;



use akita_core::{OperationType, Params, Rows, SqlInjectionDetector, SqlSecurityConfig, TableName};
use std::sync::Arc;
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::driver::blocking::DbExecutor;
use crate::interceptor::blocking::InterceptorChain;
use crate::prelude::AkitaConfig;

pub struct MySQL {
    adapter: MysqlAdapter,
    database: Option<String>,
    interceptor_chain: Option<Arc<InterceptorChain>>,
    sql_injection_detector: Option<SqlInjectionDetector>,
}

impl MySQL {
    pub fn new(conn: MysqlConnection) -> Self {
        Self {
            adapter: MysqlAdapter::new(conn),
            interceptor_chain: None,
            sql_injection_detector: None,
            database: None,
        }
    }

    /// Set up the interceptor chain
    pub fn with_interceptor_chain(mut self, interceptor_chain: Arc<InterceptorChain>) -> Self {
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
    
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }
    
    pub fn database(&self) -> Option<&String> {
        self.database.as_ref()
    }

    /// Get a clone of the interceptor chain
    pub fn interceptor_chain(&self) -> Option<Arc<InterceptorChain>> {
        self.interceptor_chain.clone()
    }

    /// Execute queries with interceptors
    fn execute_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<ExecuteResult> {
        // Create a query context
        let mut ctx = ExecuteContext::new(sql.to_string(), params, TableName::parse_table_name(sql), OperationType::detect_operation_type(sql));
        // Record parsing begins
        ctx.record_parse_complete();

        // If there is an interceptor chain, perform a pre-intercept
        if let Some(chain) = &self.interceptor_chain {
            // Perform pre-interception synchronously
            if let Err(e) = chain.before_query(&mut ctx) {
                return Err(e);
            }

            if ctx.stop_propagation {
                // If the interceptor stops propagating, returns an empty result
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(ExecuteResult::None);
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }
        
        // Execute the query
        let mut result = self.adapter.execute(ctx.final_sql(), ctx.final_params().clone());
        
        // Record the number of affected rows
        if let Ok(_rows) = &result {
            ctx.set_connection_id(self.adapter.connection_id());
            // Record execution complete
            let rows_affected = self.adapter.affected_rows();
            ctx.record_execute_complete(rows_affected);
        }
        // If there is an interceptor chain, perform a post-interception
        if let Some(chain) = &self.interceptor_chain {
            // Perform post-intercepts synchronously
            if let Err(e) = chain.after_query(&mut ctx, &mut result) {
                return Err(e);
            }
        }

        // Record query metrics
        ctx.record_query_metrics();
        
        result
    }


    fn query_with_interceptors(
        &self,
        sql: &str,
        params: Params,
    ) -> crate::prelude::Result<Rows> {
        // Create a query context
        let mut ctx = ExecuteContext::new(sql.to_string(), params, TableName::parse_table_name(sql), OperationType::detect_operation_type(sql));
        // Record parsing begins
        ctx.record_parse_complete();

        // If there is an interceptor chain, perform a pre-intercept
        if let Some(chain) = &self.interceptor_chain {
            // Perform pre-interception synchronously
            if let Err(e) = chain.before_query(&mut ctx) {
                return Err(e);
            }

            if ctx.stop_propagation {
                // If the interceptor stops propagating, returns an empty result
                tracing::info!("Query propagation stopped by interceptor");
                return Ok(Rows::new());
            }

            if let Some(sql_injection_detector) = self.sql_injection_detector.as_ref() {
                // Blocker modified SQL security checks
                let detection_result = sql_injection_detector.contains_dangerous_operations(ctx.final_sql(), ctx.final_params())?;
                ctx.set_detection_result(detection_result);
            }
        }

        // Execute the query
        let mut result = self.adapter.query(ctx.final_sql(), ctx.final_params().clone()).map(ExecuteResult::Rows);

        // Record the number of affected rows
        if let Ok(_rows) = &result {
            ctx.set_connection_id(self.adapter.connection_id());
            // Record execution complete
            let rows_affected = self.adapter.affected_rows();
            ctx.record_execute_complete(rows_affected);
        }
        // If there is an interceptor chain, perform a post-interception
        if let Some(chain) = &self.interceptor_chain {
            // Perform post-intercepts synchronously
            if let Err(e) = chain.after_query(&mut ctx, &mut result) {
                return Err(e);
            }
        }

        // Record query metrics
        ctx.record_query_metrics();

        result.map(|v| v.rows())
    }
}


impl DbExecutor for MySQL {
    fn query(&self, sql: &str, params: Params) -> crate::prelude::Result<Rows> {
        self.query_with_interceptors(sql, params)
    }

    fn execute(&self, sql: &str, params: Params) -> crate::prelude::Result<ExecuteResult> {
        self.execute_with_interceptors(sql, params)
    }

    fn start(&self) -> crate::prelude::Result<()> {
        self.adapter.start_transaction()
    }

    fn commit(&self) -> crate::prelude::Result<()> {
        self.adapter.commit_transaction()
    }

    fn rollback(&self) -> crate::prelude::Result<()> {
        self.adapter.rollback_transaction()
    }
    
    fn affected_rows(&self) -> u64 {
        self.adapter.affected_rows()
    }

    fn last_insert_id(&self) -> u64 {
        self.adapter.last_insert_id()
    }

}