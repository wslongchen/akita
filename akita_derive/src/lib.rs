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

//!
//! Generate Database Methods.
//! 
use proc_macro::TokenStream;
use proc_macro_error::{ proc_macro_error};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, AttributeArgs, ItemFn};
use crate::sql_derive::impl_sql_with_config;

#[macro_use]
mod table_derive;
#[macro_use]
mod convert_derive;
mod sql_derive;
#[allow(unused)]
mod util;
mod comm;
mod enum_derive;

/// Generate table info data
#[proc_macro_derive(FromValue)]
#[proc_macro_error]
pub fn from_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_from_akita(input)
}

/// Format table info data
#[proc_macro_derive(ToValue)]
#[proc_macro_error]
pub fn to_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_to_akita(input)
}

/// Generate table info
/// ```rust
/// use akita_derive::Entity;
///
/// /// Annotion Support: Table、id、field (name, exist)
/// #[derive(Debug, Entity, Clone)]
/// #[table(name="t_system_user")]
/// struct SystemUser {
///     #[field = "name"]
///     id: Option<i32>,
///     #[id]
///     username: String,
///     #[field(name="ages", exist = "false")]
///     age: i32,
/// }
/// ```
/// 
#[proc_macro_derive(Entity, attributes(field, table, schema, id, fill))]
#[proc_macro_error]
pub fn to_table(input: TokenStream) -> TokenStream {
    table_derive::impl_get_table(input)
}


#[proc_macro_derive(AkitaEnum, attributes(akita_enum))]
#[proc_macro_error]
pub fn derive_akita_enum(input: TokenStream) -> TokenStream {
    enum_derive::derive_akita_enum(input)
}

/// auto create sql macro,this macro use RB.fetch_prepare and RB.exec_prepare
/// for example:
/// ```rust
/// use lazy_static::lazy::Lazy;
/// use akita_derive::sql;
///
/// pub static AK:Lazy<Akita> = Lazy::new(|| {
///         let mut cfg = AkitaConfig::new("xxxx".to_string()).set_max_size(5).set_connection_timeout(Duration::from_secs(5)).set_log_level(LogLevel::Info);
///         kita::new(cfg).unwrap()
///     });
///     #[sql(AK,"select * from mch_info where mch_no = ?")]
///     fn select(name: &str) -> Vec<MchInfo> { todo!() }
///     // or
///    #[sql(AK,"select * from mch_info where mch_no = ?")]
///     fn select(ak: &AKita, name: &str) -> Vec<MchInfo> { todo!() }
/// ```
#[proc_macro_attribute]
pub fn sql(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Attempt to parse to XML schema
    if let Ok(config) = sql_derive::parse_sql_xml_args(&args) {
        return sql_derive::impl_sql_with_config(&target_fn, &config).into();
    }

    // Otherwise, standard SQL macro parsing is used
    sql_derive::impl_sql(&target_fn, &args).into()
}


/// SQL XML macros - Support XML configuration
/// ```rust
/// use akita_derive::sql_xml;
///
/// #[sql_xml("path/to/mapper.xml", "getRecordById", param_style = "named")]
///  fn get_user_by_id(id: i64) -> Result<Option<User>> {}
/// ```
#[proc_macro_attribute]
pub fn sql_xml(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Parsing parameters
    let config = sql_derive::parse_sql_xml_args(&args)
        .unwrap_or_else(|e| panic!("[Akita] sql_xml macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}

/// Another form of SQL macros, with a more flexible syntax
/// ```rust
/// use akita_derive::query;
///
/// #[query("select * from t_system_user WHERE id = ?")]
///     fn update_user_status(id: i64) -> Result<u64, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn query(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Parses parameters, allowing for a more flexible syntax
    let config = sql_derive::parse_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] query macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}


/// Shortcut macros: Used for insertion operations
/// ```rust
/// use akita_derive::insert;
///
/// #[insert("INSERT INTO t_system_user (pk, name, status, token) VALUES (?, ?, ?, ?)")]
///     fn insert_user_direct(pk: &str, name: &str, status: u8, token: &str) -> Result<u64, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn insert(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Automatically check if u64 is returned (insert ID)
    let return_ty = &target_fn.sig.output;
    let return_u64 = match return_ty {
        syn::ReturnType::Type(_, ty) => {
            let type_str = ty.to_token_stream().to_string();
            type_str.contains("u64") || type_str.contains("Result<u64")
        }
        _ => false,
    };

    let config = sql_derive::parse_positional_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] insert macro error: {}", e));

    let code = impl_sql_with_config(&target_fn, &config);

    // If an insert does not return u64, add a warning
    if !return_u64 {
        let _func_name = &target_fn.sig.ident;
        let warning = quote! {
            #[deprecated(note = "insert operations typically return the inserted ID (u64)")]
        };

        proc_macro2::TokenStream::from(quote! {
            #warning
            #code
        }).into()
    } else {
        code.into()
    }
}


/// Shortcut macro: Used for update operations
/// ```rust
/// use akita_derive::update;
///
/// #[update("UPDATE t_system_user SET status = ? WHERE id = ?")]
///     fn update_user_status(status: u8, id: i64) -> Result<u64, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn update(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    let config = sql_derive::parse_positional_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] update macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}

/// Shortcut macros: Used for delete operations
/// ```rust
/// use akita_derive::delete;
///
/// #[delete("DELETE FROM t_system_user WHERE id = ?")]
///     fn delete_user(id: i64) -> Result<u64, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn delete(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    let config = sql_derive::parse_positional_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] delete macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}


/// Shortcut macros: Used to query a single record
///```rust
/// use akita_derive::select_one;
/// 
/// #[select_one("SELECT * FROM t_system_user WHERE id = ?")]
///     fn get_user_by_id(id: i64) -> Result<Option<User>, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn select_one(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Make sure the return type is Option<T> or Result<Option<T>>
    let return_ty = &target_fn.sig.output;
    let is_valid = match return_ty {
        syn::ReturnType::Type(_, ty) => {
            let type_str = ty.to_token_stream().to_string();
            let type_string_no_space = type_str.replace(' ', "");
            type_string_no_space.contains("Option<") || type_string_no_space.contains("Result<Option<")
        }
        _ => false,
    };

    if !is_valid {
        panic!("select_one macro requires return type to be Option<T> or Result<Option<T>>");
    }

    let config = sql_derive::parse_positional_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] select_one macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}

///Shortcut macros: Used to query multiple records
///```rust
/// use akita_derive::list;
/// 
/// #[list("SELECT * FROM t_system_user ORDER BY id")]
///     fn get_all_users() -> Result<Vec<User>, AkitaError> {
///     }
/// ```
#[proc_macro_attribute]
pub fn list(args: TokenStream, func: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let target_fn = parse_macro_input!(func as ItemFn);

    // Make sure the return type is Vec<T> or Result<Vec<T>>
    let return_ty = &target_fn.sig.output;
    let is_valid = match return_ty {
        syn::ReturnType::Type(_, ty) => {
            let type_str = ty.to_token_stream().to_string();
            let type_string_no_space = type_str.replace(' ', "");
            type_string_no_space.contains("Vec<") || type_str.contains("Result<Vec<")
        }
        _ => false,
    };

    if !is_valid {
        panic!("select_many macro requires return type to be Vec<T> or Result<Vec<T>>");
    }

    let config = sql_derive::parse_positional_query_args(&args, &target_fn)
        .unwrap_or_else(|e| panic!("[Akita] select_many macro error: {}", e));

    sql_derive::impl_sql_with_config(&target_fn, &config).into()
}