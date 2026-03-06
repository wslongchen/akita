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
#[allow(unused)]
mod chain;
#[allow(unused)]
pub mod builder;
#[allow(unused)]
mod logging;

use akita_core::{InterceptorType, OperationType};
pub use chain::*;
pub use builder::*;
pub use logging::*;
use crate::comm::{ExecuteContext, ExecuteResult};
use crate::errors::AkitaError;

#[allow(unused)]
pub trait AkitaInterceptor: Send + Sync {
    /// Call before executing the query
    #[track_caller]
    fn before_execute(&self, ctx: &mut ExecuteContext) -> crate::prelude::Result<()>;

    /// Call after executing the query
    #[track_caller]
    fn after_execute(&self, ctx: &mut ExecuteContext, result: &mut Result<ExecuteResult, AkitaError>) -> crate::prelude::Result<()> {
        Ok(())
    }

    /// Call when the query executes an error
    #[track_caller]
    fn on_error(&self, _ctx: &ExecuteContext, error: &mut AkitaError) -> crate::prelude::Result<()> {
        // Default implementation, subclasses can be overridden
        tracing::error!("Interceptor '{}' encountered error: {}", self.name(), error);
        Ok(())
    }

    /// Interceptor name
    fn name(&self) -> &'static str;

    /// Get the interceptor type
    fn interceptor_type(&self) -> InterceptorType;

    /// Execution order (the smaller the value, the first execution)
    fn order(&self) -> i32 { 0 }

    /// Whether the action type is supported
    fn supports_operation(&self, operation: &OperationType) -> bool {
        match operation {
            OperationType::Call => false,
            _ => true,
        }
    }

    /// Whether the table is ignored
    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }

    /// Interceptor initialization
    #[track_caller]
    fn init(&self) -> crate::prelude::Result<()> { Ok(()) }

    /// Interceptor destruction
    #[track_caller]
    fn destroy(&self) -> crate::prelude::Result<()> { Ok(()) }
}