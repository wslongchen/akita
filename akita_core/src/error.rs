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

use std::fmt;



#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Couldn't convert the row `{:?}` to a desired type",
            self.to_owned()
        )
    }
}

impl From<serde_json::Error> for ConvertError {
    fn from(err: serde_json::Error) -> Self {
        ConvertError::NotSupported(err.to_string(), "SerdeJson".to_string())
    }
}

impl From<serde_json::Error> for AkitaDataError {
    fn from(err: serde_json::Error) -> Self {
        AkitaDataError::ConvertError(ConvertError::NotSupported(err.to_string(), "SerdeJson".to_string()))
    }
}



#[derive(Debug)]
pub enum AkitaDataError {
    ConvertError(ConvertError),
    NoSuchValueError(String),
    ObjectValidError(String),
}

impl ToString for AkitaDataError {
    fn to_string(&self) -> String {
        match self {
            AkitaDataError::ConvertError(e) => e.to_string(),
            AkitaDataError::NoSuchValueError(e) => e.to_string(),
            AkitaDataError::ObjectValidError(e) => e.to_string(),
        }
    }
}