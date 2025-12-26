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
use serde::{Deserialize, Serialize};


/// Interceptor type
#[derive(Debug, Clone, PartialEq, Eq, Hash,Serialize, Deserialize)]
pub enum InterceptorType {
    // Core features:
    Tenant,
    Pagination,
    DataPermission,
    FieldFill,
    // Performance related
    Performance,
    Cache,
    // Safety-related
    Encryption,
    Audit,
    // Business function
    SoftDelete,
    OptimisticLock,
    VersionControl,
    // Monitor diagnostics
    Metrics,
    Tracing,
    Logging,
    // customization
    Custom(String),
}
