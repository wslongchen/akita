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
mod builder;
mod chain;
mod interceptors;
mod logging;

pub use builder::*;
pub use chain::*;
pub use interceptors::*;
pub use logging::*;

use crate::comm::{ExecuteContext, ExecuteResult};
use crate::errors::AkitaError;
use crate::interceptor::shared::InterceptorBase;
use akita_core::{InterceptorType, OperationType};

/// Asynchronous interceptor trait.
///
/// Extends `InterceptorBase` with asynchronous lifecycle methods.
/// All interceptors that implement `AsyncAkitaInterceptor` automatically
/// implement `InterceptorBase` through blanket implementation.
#[async_trait::async_trait]
pub trait AsyncAkitaInterceptor: InterceptorBase {
    /// Call before executing the query
    #[track_caller]
    async fn before_execute(&self, ctx: &mut ExecuteContext) -> crate::prelude::Result<()>;

    /// Call after executing the query
    #[track_caller]
    async fn after_execute(
        &self,
        ctx: &mut ExecuteContext,
        result: &mut Result<ExecuteResult, AkitaError>,
    ) -> crate::prelude::Result<()> {
        Ok(())
    }

    /// Call when the query executes an error
    #[track_caller]
    async fn on_error(
        &self,
        _ctx: &ExecuteContext,
        error: &mut AkitaError,
    ) -> crate::prelude::Result<()> {
        tracing::error!("Interceptor '{}' encountered error: {}", self.name(), error);
        Ok(())
    }

    /// Interceptor initialization
    #[track_caller]
    async fn init(&self) -> crate::prelude::Result<()> {
        Ok(())
    }

    /// Interceptor destruction
    #[track_caller]
    async fn destroy(&self) -> crate::prelude::Result<()> {
        Ok(())
    }
}
