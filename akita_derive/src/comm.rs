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

use regex::Regex;
use lazy_static::lazy_static;
use proc_macro2::{Span};
use syn::{self, Expr, Type};


lazy_static! {
    pub static ref COW_TYPE: Regex = Regex::new(r"Cow<'[a-z]+,str>").unwrap();
}


pub static CUSTOM_ARG_LIFETIME: &str = "v_a";

pub static ALLOW_TABLE_ID_TYPES :[&str; 20] = [
    "u32",
    "u64",
    "u128",
    "String",
    "&str",
    "i32",
    "i64",
    "i128",
    "usize",
    "iszie",

    "Option<u32>",
    "Option<u64>",
    "Option<u128>",
    "Option<String>",
    "Option<&str>",
    "Option<i32>",
    "Option<i64>",
    "Option<i128>",
    "Option<usize>",
    "Option<iszie>",
];

pub static CUSTOM_ARG_ALLOWED_COPY_TYPES: [&str; 14] = [
    "usize",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "isize",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "f32",
    "f64",
];

pub static NUMBER_TYPES: [&str; 38] = [
    "usize",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "isize",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "f32",
    "f64",
    "Option<usize>",
    "Option<u8>",
    "Option<u16>",
    "Option<u32>",
    "Option<u64>",
    "Option<isize>",
    "Option<i8>",
    "Option<i16>",
    "Option<i32>",
    "Option<i64>",
    "Option<f32>",
    "Option<f64>",
    "Option<Option<usize>>",
    "Option<Option<u8>>",
    "Option<Option<u16>>",
    "Option<Option<u32>>",
    "Option<Option<u64>>",
    "Option<Option<isize>>",
    "Option<Option<i8>>",
    "Option<Option<i16>>",
    "Option<Option<i32>>",
    "Option<Option<i64>>",
    "Option<Option<f32>>",
    "Option<Option<f64>>",
];


#[derive(Debug)]
#[allow(unused)]
pub struct FieldInformation {
    pub field: syn::Field,
    pub field_type: String,
    pub name: String,
    pub extra: Vec<FieldExtra>,
}

impl FieldInformation {
    pub fn new(
        field: syn::Field,
        field_type: String,
        name: String,
        extra: Vec<FieldExtra>,
    ) -> Self {
        FieldInformation { field, field_type, name, extra }
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub enum FieldExtra {
    Field,
    TableId,
    Name(String),
    IdType(String),
    Table(String),
    Select(bool),
    Exist(bool),
    Converter(String),
    Fill {
        /// This is the name of the function that should be cacalledlled
        function: String,
        mode: Option<String>,
        /// This is the argument type that can be passed in with a macro
        argument: Option<CustomArgument>,
    },
    NumericScale(ValueOrPath<u64>),
}

/// This struct stores information about defined custom arguments that will be passed in
/// by the user in the annotion step.
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct CustomArgument {
    /// The span of type definition, this can be used in combination with `quote_spanned!` for
    /// better error reporting
    pub def_span: Span,
    /// The type of the argument. This can use `'v_a` as a lifetime but has to be Sized. This
    /// means that the type size has to be known at compile time
    pub arg_type: Type,
    /// This is the way we can access the value from the provided arguments. This will usually
    /// look something like `args.0`.
    pub arg_access: Option<Expr>,
}

impl CustomArgument {
    pub fn new(def_span: Span, arg_type: Type) -> Self {
        CustomArgument { def_span, arg_type, arg_access: None }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueOrPath<T: std::fmt::Debug + Clone + PartialEq> {
    Value(T),
    Path(String),
}
