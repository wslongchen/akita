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

//! Module containing the error type returned by TinyTemplate if an error occurs.

use crate::instruction::{path_to_str, PathSlice};
use serde_json::Error as SerdeJsonError;
use serde_json::Value;
use std::error::Error as StdError;
use std::fmt;

/// Enum representing the potential errors that TinyTemplate can encounter.
#[derive(Debug)]
pub enum Error {
    ParseError {
        msg: String,
        line: usize,
        column: usize,
    },
    RenderError {
        msg: String,
        line: usize,
        column: usize,
    },
    SerdeError {
        err: SerdeJsonError,
    },
    GenericError {
        msg: String,
    },
    StdFormatError {
        err: fmt::Error,
    },
    CalledTemplateError {
        name: String,
        err: Box<Error>,
        line: usize,
        column: usize,
    },
    CalledFormatterError {
        name: String,
        err: Box<Error>,
        line: usize,
        column: usize,
    },

    #[doc(hidden)]
    __NonExhaustive,
}
impl From<SerdeJsonError> for Error {
    fn from(err: SerdeJsonError) -> Error {
        Error::SerdeError { err }
    }
}
impl From<fmt::Error> for Error {
    fn from(err: fmt::Error) -> Error {
        Error::StdFormatError { err }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ParseError { msg, line, column } => write!(
                f,
                "Failed to parse the template (line {}, column {}). Reason: {}",
                line, column, msg
            ),
            Error::RenderError { msg, line, column } => {
                write!(
                    f,
                    "Encountered rendering error on line {}, column {}. Reason: {}",
                    line, column, msg
                )
            }
            Error::SerdeError { err } => {
                write!(f, "Unexpected serde error while converting the context to a serde_json::Value. Error: {}", err)
            }
            Error::GenericError { msg } => {
                write!(f, "{}", msg)
            }
            Error::StdFormatError { err } => {
                write!(f, "Unexpected formatting error: {}", err)
            }
            Error::CalledTemplateError {
                name,
                err,
                line,
                column,
            } => {
                write!(
                    f,
                    "Call to sub-template \"{}\" on line {}, column {} failed. Reason: {}",
                    name, line, column, err
                )
            }
            Error::CalledFormatterError {
                name,
                err,
                line,
                column,
            } => {
                write!(
                    f,
                    "Call to value formatter \"{}\" on line {}, column {} failed. Reason: {}",
                    name, line, column, err
                )
            }
            Error::__NonExhaustive => unreachable!(),
        }
    }
}
impl StdError for Error {
    fn description(&self) -> &str {
        match self {
            Error::ParseError { .. } => "ParseError",
            Error::RenderError { .. } => "RenderError",
            Error::SerdeError { .. } => "SerdeError",
            Error::GenericError { msg } => &msg,
            Error::StdFormatError { .. } => "StdFormatError",
            Error::CalledTemplateError { .. } => "CalledTemplateError",
            Error::CalledFormatterError { .. } => "CalledFormatterError",
            Error::__NonExhaustive => unreachable!(),
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;