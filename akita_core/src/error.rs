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
use crate::{DetectionResult};

#[derive(Debug, Clone)]
pub struct SqlInjectionError {
    pub input: String,
    pub reason: DetectionResult,
}


impl fmt::Display for SqlInjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SQL Injection detected: {:?} in input: {}", self.reason, self.input)
    }
}

impl std::error::Error for SqlInjectionError {}



impl From<serde_json::Error> for ConversionError {
    fn from(err: serde_json::Error) -> Self {
        ConversionError::NotSupported(err.to_string(), "SerdeJson".to_string())
    }
}


#[derive(Debug)]
pub enum ConversionError {
    TypeMismatch {
        expected: String,
        found: String,
    },
    NotSupported(String, String),
    MissingField {
        field: String,
        expected_type: String,
    },
    NullValue {
        target_type: String,
    },
    ConversionError {
        message: String,
    },
    NumericOverflow {
        target_type: String,
    },
    ParseError {
        message: String,
    },
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConversionError::TypeMismatch { expected, found } => {
                write!(
                    f,
                    "Type mismatch: expected {}, found {}",
                    expected, found
                )
            }
            ConversionError::NullValue { target_type } => {
                write!(
                    f,
                    "Cannot convert null value to {}",
                    target_type
                )
            }
            ConversionError::ConversionError { message } => {
                write!(
                    f,
                    "Conversion failed: {}",
                    message
                )
            }
            ConversionError::NumericOverflow { target_type } => {
                write!(
                    f,
                    "Numeric overflow when converting to {}",
                    target_type
                )
            }
            ConversionError::ParseError { message } => {
                write!(
                    f,
                    "Parse error: {}",
                    message
                )
            }
            ConversionError::MissingField { field, expected_type } => write!(
                f,
                "Missing Field: field {}, expected_type {}",
                field, expected_type
            ),
            ConversionError::NotSupported(k, v) => write!(
                f,
                "NotSupported  `{}` :`{}`",
                k, v
            )
        }
    }
}

impl std::error::Error for ConversionError {}


impl ConversionError {
    pub fn conversion_error<T: Into<String>>(err: T) -> Self {
        let err = err.into();
        Self::ConversionError { message: err }
    }
    pub fn parse_error<T: Into<String>>(err: T) -> Self {
        let err = err.into();
        Self::ParseError { message: err }
    }
    
    pub fn type_mismatch_error<T: Into<String>, E: Into<String>>(expected: T, found: E) -> Self {
        let expected = expected.into();
        let found = found.into();
        Self::TypeMismatch { expected, found }
    }
    
    pub fn not_supported_error<T: Into<String>, E: Into<String>>(field: T, expected: E) -> Self {
        let expected = expected.into();
        let field = field.into();
        Self::NotSupported(field, expected)
    }
    
    pub fn missing_field_error<T: Into<String>, E: Into<String>>(field: T, expected_type: E) -> Self {
        let expected_type = expected_type.into();
        let field = field.into();
        Self::MissingField { field, expected_type }
    }
    
    pub fn null_value_error<T: Into<String>>(target_type: T) -> Self {
        let target_type = target_type.into();
        Self::NullValue { target_type }
    }
    
    pub fn numeric_overflow_error<T: Into<String>>(target_type: T) -> Self {
        let target_type = target_type.into();
        Self::NumericOverflow { target_type }
    }
}

#[derive(Debug)]
pub enum AkitaDataError {
    NoSuchValueError(String),
    NoSuchFieldError(String),
    ParseError(String),
    ConversionError(ConversionError),
    SqlInjectionError(SqlInjectionError),
    TableNameEmpError,
    SecurityError,
    ObjectValidError(String),
    IndexOutOfBounds(usize, usize),
}

impl AkitaDataError {
    pub fn conversion_error<T: Into<String>>(err: T) -> Self {
        Self::ConversionError(ConversionError::conversion_error(err))
    }
    
    pub fn parse_error<T: Into<String>>(err: T) -> Self {
        let err = err.into();
        Self::ConversionError(ConversionError::ParseError { message: err })
    }

    pub fn type_mismatch_error<T: Into<String>, E: Into<String>>(expected: T, found: E) -> Self {
        let expected = expected.into();
        let found = found.into();
        Self::ConversionError(ConversionError::TypeMismatch { expected, found })
    }

    pub fn not_supported_error<T: Into<String>, E: Into<String>>(field: T, expected: E) -> Self {
        let expected = expected.into();
        let field = field.into();
        Self::ConversionError(ConversionError::NotSupported(field, expected))
    }

    pub fn missing_field_error<T: Into<String>, E: Into<String>>(field: T, expected_type: E) -> Self {
        let expected_type = expected_type.into();
        let field = field.into();
        Self::ConversionError(ConversionError::MissingField { field, expected_type })
    }

    pub fn null_value_error<T: Into<String>>(target_type: T) -> Self {
        let target_type = target_type.into();
        Self::ConversionError(ConversionError::NullValue { target_type })
    }

    pub fn numeric_overflow_error<T: Into<String>>(target_type: T) -> Self {
        let target_type = target_type.into();
        Self::ConversionError(ConversionError::NumericOverflow { target_type })
    }

    pub fn sql_injection_error<T: Into<String>>(input: T, reason: DetectionResult) -> Self {
        let input = input.into();
        Self::SqlInjectionError(SqlInjectionError {
            input,
            reason,
        })
    }
}

impl fmt::Display for AkitaDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AkitaDataError::ConversionError(e) =>  write!(f, "Conversion Data Error: {e}"),
            AkitaDataError::NoSuchValueError(e) =>  write!(f, "No Such Value Error: {e}"),
            AkitaDataError::NoSuchFieldError(e) =>  write!(f, "No Such Field Error: {e}"),
            AkitaDataError::ParseError(e) =>  write!(f, "Parse Error: {e}"),
            AkitaDataError::TableNameEmpError =>  write!(f, "table name is empty"),
            AkitaDataError::SecurityError =>  write!(f, "Unsafe SQL detected"),
            AkitaDataError::ObjectValidError(e) =>  write!(f, "Object Valid Error: {e}"),
            AkitaDataError::IndexOutOfBounds(i, u) =>  write!(f, "IndexOutOfBoundsException: Index: {i}, Size: {u}"),
            AkitaDataError::SqlInjectionError(e) =>  write!(f, "SqlInjection Error: {e}"),
        }
    }
}