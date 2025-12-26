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

#![deny(clippy::all)]
extern crate core;

mod information;
mod error;
mod data;
mod value;
mod macros;
mod types;
pub mod comm;
mod snowflake;
mod interceptor;
mod param;
mod wrapper;
mod security;

#[doc(inline)]
pub use security::*;
#[doc(inline)]
pub use interceptor::*;
#[doc(inline)]
pub use wrapper::*;
#[doc(inline)]
pub use data::*;
#[doc(inline)]
pub use snowflake::*;
#[doc(inline)]
pub use information::*;
#[doc(inline)]
pub use value::*;
#[doc(inline)]
pub use types::*;
#[doc(inline)]
pub use param::*;
#[doc(inline)]
pub use error::*;
pub use serde;