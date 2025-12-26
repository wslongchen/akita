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
use crate::{AkitaDataError, AkitaValue, ConversionError, FromAkitaValue, IntoAkitaValue};

// <u8> Create a new type for the Vec
#[derive(Debug, Clone, PartialEq)]
pub struct Blob(Vec<u8>);

impl From<Vec<u8>> for Blob {
    fn from(data: Vec<u8>) -> Self {
        Blob(data)
    }
}

impl From<Blob> for Vec<u8> {
    fn from(blob: Blob) -> Self {
        blob.0
    }
}

impl FromAkitaValue for Blob {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Blob(data) => Ok(Blob(data.clone())),
            AkitaValue::Text(s) => Ok(Blob(s.as_bytes().to_vec())),
            _ => Err(AkitaDataError::ConversionError(ConversionError::TypeMismatch {
                expected: "Vec<u8>".to_string(),
                found: format!("{:?}", value),
            })),
        }
    }
}

impl IntoAkitaValue for Blob {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Blob(self.0.clone())
    }
}
