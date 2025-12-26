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
use crate::{AkitaDataError, AkitaValue, FromAkitaValue, IntoAkitaValue};

#[derive(Debug, Clone)]
pub struct SqlExpr(pub String);

impl IntoAkitaValue for SqlExpr {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::RawSql(self.0.to_string())
    }
}

impl FromAkitaValue for SqlExpr {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Text(v) => Ok(SqlExpr(v.to_string())),
            AkitaValue::RawSql(v) => Ok(SqlExpr(v.to_string())),
            _ => Ok(SqlExpr(value.to_string()))
        }
    }
}