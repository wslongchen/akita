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
use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use regex::Regex;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{FnArg, ItemFn, Pat, ReturnType};
use crate::comm::{crate_ident, AttrArg, AttrArgs};

/// Helper: extract a string literal from an AttrArg
fn attr_arg_to_str(arg: &AttrArg) -> Option<String> {
    match arg {
        AttrArg::Lit(syn::Lit::Str(s)) => Some(s.value()),
        AttrArg::Meta(syn::Meta::NameValue(nv)) => {
            if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &nv.value {
                Some(s.value())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper: extract a string literal value from a MetaNameValue (syn 2 uses .value instead of .lit)
fn name_value_str(nv: &syn::MetaNameValue) -> Option<String> {
    if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &nv.value {
        Some(s.value())
    } else {
        None
    }
}

pub fn impl_sql(target_fn: &ItemFn, args: &AttrArgs) -> TokenStream {
    let func_name_ident = &target_fn.sig.ident;

    // Parsing macro parameters
    let config = parse_sql_config(args, target_fn)
        .unwrap_or_else(|e| panic!("[Akita] {} in function '{}'", e, func_name_ident));

    let token = impl_sql_with_config(target_fn, &config);
    token
}

/// Parse the SQL XML macro parameters
pub fn parse_sql_xml_args(args: &AttrArgs) -> Result<SqlConfig, String> {
    if args.is_empty() || args.len() > 3 {
        return Err(format!("sql_xml macro requires 1-3 arguments, got {}", args.len()));
    }

    let mut xml_file = None;
    let mut sql_id = None;
    let mut param_style = None;

    for (i, arg) in args.iter().enumerate() {
        match arg {
            AttrArg::Lit(syn::Lit::Str(lit_str)) => {
                if i == 0 {
                    xml_file = Some(lit_str.value());
                } else if i == 1 {
                    sql_id = Some(lit_str.value());
                } else {
                    return Err("Too many string literal arguments".to_string());
                }
            }
            AttrArg::Meta(syn::Meta::NameValue(name_value)) => {
                if name_value.path.is_ident("param_style") {
                    if let Some(s) = name_value_str(name_value) {
                        param_style = match s.as_str() {
                            "positional" => Some(ParamStyle::Positional),
                            "named" => Some(ParamStyle::Named),
                            "numbered" => Some(ParamStyle::Numbered),
                            _ => return Err(format!(
                                "Invalid param_style value: {}. Must be 'positional', 'named', or 'numbered'",
                                s
                            )),
                        };
                    } else {
                        return Err("param_style must be a string literal".to_string());
                    }
                } else {
                    return Err(format!(
                        "Unknown named argument: {}. Only 'param_style' is supported",
                        name_value.path.to_token_stream()
                    ));
                }
            }
            // Handle other types of literals
            AttrArg::Lit(lit) => {
                if i < 2 {
                    return Err(format!(
                        "Argument {} must be a string literal, got {:?}",
                        i + 1,
                        lit
                    ));
                }
            }
            // Handle other Meta types
            AttrArg::Meta(syn::Meta::Path(path)) => {
                return Err(format!(
                    "Unexpected path argument: {}. Use 'param_style = \"...\"' for named arguments",
                    path.to_token_stream()
                ));
            }
            AttrArg::Meta(syn::Meta::List(list)) => {
                return Err(format!(
                    "List arguments are not supported in sql_xml macro: {}",
                    list.path.to_token_stream()
                ));
            }
        }
    }

    let file_path = xml_file.ok_or_else(|| "Missing XML file path".to_string())?;
    let sql_id = sql_id.ok_or_else(|| "Missing SQL ID".to_string())?;

    Ok(SqlConfig {
        mode: SqlMode::Xml { file_path, sql_id },
        param_style,
    })
}

pub fn parse_query_args(args: &AttrArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    if args.is_empty() {
        return Err("query macro requires at least one argument".to_string());
    }

    // Checks if it is in named argument form
    let mut has_named_args = false;
    for arg in args {
        if let AttrArg::Meta(syn::Meta::NameValue(_)) = arg {
            has_named_args = true;
            break;
        }
    }

    if has_named_args {
        parse_named_query_args(args)
    } else {
        parse_positional_query_args(args, target_fn)
    }
}

fn parse_named_query_args(args: &AttrArgs) -> Result<SqlConfig, String> {
    let mut file = None;
    let mut id = None;
    let mut sql = None;
    let mut param_style = None;
    let mut akita_name = None;

    for arg in args {
        match arg {
            AttrArg::Meta(syn::Meta::NameValue(ref name_value)) => {
                let arg_name = name_value.path.get_ident()
                    .ok_or_else(|| "Argument must have a valid identifier".to_string())?
                    .to_string();

                match arg_name.as_str() {
                    "file" => {
                        if let Some(s) = name_value_str(name_value) {
                            file = Some(s);
                        } else {
                            return Err("file must be a string literal".to_string());
                        }
                    }
                    "id" => {
                        if let Some(s) = name_value_str(name_value) {
                            id = Some(s);
                        } else {
                            return Err("id must be a string literal".to_string());
                        }
                    }
                    "sql" => {
                        if let Some(s) = name_value_str(name_value) {
                            sql = Some(s);
                        } else {
                            return Err("sql must be a string literal".to_string());
                        }
                    }
                    "param_style" => {
                        if let Some(s) = name_value_str(name_value) {
                            param_style = match s.as_str() {
                                "positional" => Some(ParamStyle::Positional),
                                "named" => Some(ParamStyle::Named),
                                "numbered" => Some(ParamStyle::Numbered),
                                _ => return Err(format!(
                                    "Invalid param_style value: {}. Must be 'positional', 'named', or 'numbered'",
                                    s
                                )),
                            };
                        } else {
                            return Err("param_style must be a string literal".to_string());
                        }
                    }
                    "akita" => {
                        if let Some(s) = name_value_str(name_value) {
                            akita_name = Some(s);
                        } else {
                            return Err("akita must be a string literal".to_string());
                        }
                    }
                    _ => {
                        return Err(format!("Unknown named argument: {}", arg_name));
                    }
                }
            }
            _ => {
                return Err("query macro with named arguments only supports name=value syntax".to_string());
            }
        }
    }

    // Decision mode
    if let (Some(file), Some(id)) = (file, id) {
        Ok(SqlConfig {
            mode: SqlMode::Xml { file_path: file, sql_id: id },
            param_style,
        })
    } else if let Some(sql_str) = sql {
        if let Some(akita) = akita_name {
            Ok(SqlConfig {
                mode: SqlMode::Explicit{ conn: ConnectionInfo::new(&akita), sql: sql_str },
                param_style,
            })
        } else {
            Err("query macro requires a connection parameter (Akita, AkitaTransaction, or DbDriver). \
                                 If using repository pattern, add &self parameter."
                .to_string())
        }
    } else {
        Err("query macro requires either 'file' and 'id' or 'sql' parameter".to_string())
    }
}

pub fn parse_positional_query_args(args: &AttrArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    parse_sql_config(args, target_fn)
}



pub fn impl_sql_with_config(target_fn: &ItemFn, config: &SqlConfig) -> TokenStream {
    let return_ty = &target_fn.sig.output;
    let func_name_ident = &target_fn.sig.ident;
    let func_args = &target_fn.sig.inputs;

    // Checks if it is an asynchronous function
    let is_async = is_async_function(target_fn);

    // Generating code
    let code = match &config.mode {
        SqlMode::Explicit{ conn, sql } => {
            generate_explicit_sql_code(func_name_ident, func_args, return_ty, conn, sql, is_async)
        }
        SqlMode::Xml { file_path, sql_id } => {
            generate_xml_sql_code(func_name_ident, func_args, return_ty, file_path, sql_id, is_async)
        }
    };
    code
}

// ========== Configuration parsing ==========
#[derive(Debug)]
pub enum SqlMode {
    Explicit {
        conn: ConnectionInfo,
        sql: String,
    },
    Xml{
        file_path: String,
        sql_id: String,
    },
}

#[allow(unused)]
#[derive(Debug)]
pub struct SqlConfig {
    pub mode: SqlMode,
    pub param_style: Option<ParamStyle>,
}

#[derive(Clone, Copy, Debug)]
pub enum ParamStyle {
    Positional,  // ? Placeholders
    Named,       // :name Named parameters
    Numbered,    // $1, $2 Number parameters
}

fn parse_sql_config(args: &AttrArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    match args.len() {
        1 => {
            let arg = &args[0];
            if let Some(value) = attr_arg_to_str(arg) {
                let param_style = detect_param_style(&value);

                // Check for the &self argument
                let has_self = target_fn.sig.inputs.iter().any(|input| {
                    if let FnArg::Receiver(_) = input { true } else { false }
                });

                if has_self {
                    Ok(SqlConfig {
                        mode: SqlMode::Explicit {
                            conn: ConnectionInfo::new("akita"),
                            sql: value,
                        },
                        param_style,
                    })
                } else {
                    let connection_param = get_connection_param_name(&target_fn.sig.inputs);
                    match connection_param {
                        Some(conn) => {
                            Ok(SqlConfig {
                                mode: SqlMode::Explicit {
                                    conn,
                                    sql: value
                                },
                                param_style,
                            })
                        }
                        None => {
                            let func_name = &target_fn.sig.ident;
                            Err(format!(
                                "Function '{}' requires a connection parameter (Akita, AkitaTransaction, or DbDriver). \
                                 If using repository pattern, add &self parameter.",
                                func_name
                            ))
                        }
                    }
                }
            } else {
                Err("Single argument must be a string literal".to_string())
            }
        }
        2 => {
            let arg1 = &args[0];
            let arg2 = &args[1];

            let akita_ident = arg1.to_token_stream().to_string();
            if let Some(value) = attr_arg_to_str(arg2) {
                let param_style = detect_param_style(&value);

                Ok(SqlConfig {
                    mode: SqlMode::Explicit { conn: ConnectionInfo::new(&akita_ident), sql: value },
                    param_style,
                })
            } else {
                Err("Second argument must be a SQL string literal".to_string())
            }
        }
        3 => {
            let arg1 = &args[0];
            let arg2 = &args[1];
            let arg3 = &args[2];

            let fp = attr_arg_to_str(arg1);
            let si = attr_arg_to_str(arg2);
            if let (Some(file_path), Some(sql_id)) = (fp, si) {
                let param_style = if let AttrArg::Meta(syn::Meta::NameValue(name_value)) = arg3 {
                    if name_value.path.is_ident("param_style") {
                        if let Some(s) = name_value_str(name_value) {
                            match s.as_str() {
                                "positional" => Some(ParamStyle::Positional),
                                "named" => Some(ParamStyle::Named),
                                "numbered" => Some(ParamStyle::Numbered),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(SqlConfig {
                    mode: SqlMode::Xml { file_path, sql_id },
                    param_style,
                })
            } else {
                Err("XML mode requires file path and SQL ID".to_string())
            }
        }
        _ => Err(format!("Expected 1, 2 or 3 arguments, got {}", args.len())),
    }
}

// Detecting parameter styles
fn detect_param_style(sql: &str) -> Option<ParamStyle> {
    // Named parameters: :param_name
    let named_re = Regex::new(r":\w+").unwrap();
    if named_re.is_match(sql) {
        return Some(ParamStyle::Named);
    }

    // Numbered parameter: $1, $2
    let numbered_re = Regex::new(r"\$\d+").unwrap();
    if numbered_re.is_match(sql) {
        return Some(ParamStyle::Numbered);
    }

    // Position parameter: ?
    if sql.contains('?') {
        return Some(ParamStyle::Positional);
    }
    None
}

// ========== Code generation ==========

fn generate_explicit_sql_code(
    func_name: &Ident,
    func_args: &Punctuated<FnArg, Comma>,
    return_ty: &ReturnType,
    connection: &ConnectionInfo,
    sql_expr: &str,
    is_async: bool,
) -> TokenStream {
    let connection_name = connection.name.to_string();
    let connection = connection.param_ident();
    let crate_ident = crate_ident();

    // Check for the &self argument
    let has_self = func_args.iter().any(|arg| {
        if let FnArg::Receiver(_) = arg { true } else { false }
    });
    // Get connection type information
    let connection_info = get_connection_param_name(func_args);
    if has_self {
        // Repository pattern: Use self.xxx
        // Parameter preparation (excluding self)
        let params_prepare = generate_params_prepare_code(
            func_args,
            Some(Ident::new("self", Span::call_site()))
        );
        // Executing code
        let connection_ident = Ident::new("conn", Span::call_site());
        let call_code = if is_async {
            generate_async_execution_code(
                return_ty,
                sql_expr,
                &connection_ident,
            )
        } else {
            generate_execution_code(
                return_ty,
                sql_expr,
                &connection_ident,
            )
        };

        // Different connection acquisition codes are generated depending on the field type
        let conn_acquire_code = if let Some(connection_info) = connection_info.as_ref() {
            if is_async {
                if is_akita_type(&connection_info.type_name) {
                    // The Akita type requires the acquire() call.
                    quote! {
                    let mut conn = self.#connection.acquire().await
                    .expect(&format!("Failed to acquire connection from self.{}", #connection_name));
                }
                } else if is_transaction_type(&connection_info.type_name) || is_db_driver_type(&connection_info.type_name) {
                    // AkitaTransaction and DbDriver are used directly
                    quote! {
                    let mut conn = &mut self.#connection;
                }
                } else {
                    quote! {
                        compile_error!("Unsupported connection type in self.{}", #connection_name);
                    }
                }
            } else {
                if is_akita_type(&connection_info.type_name) {
                    // The Akita type requires the acquire() call.
                    quote! {
                    let mut conn = self.#connection.acquire().await
                    .expect(&format!("Failed to acquire connection from self.{}", #connection_name));
                }
                } else if is_transaction_type(&connection_info.type_name) || is_db_driver_type(&connection_info.type_name) {
                    // AkitaTransaction and DbDriver are used directly
                    quote! {
                    let mut conn = &mut self.#connection;
                }
                } else {
                    quote! {
                        compile_error!("Unsupported connection type in self.{}", #connection_name);
                    }
                }
            }
        } else {
            if is_async {
                quote! {
                    let mut conn = self.#connection.acquire().await
                        .expect(&format!("Failed to acquire connection from self.{}", #connection_name));
                }
            } else {
                quote! {
                    let mut conn = self.#connection.acquire()
                        .expect(&format!("Failed to acquire connection from self.{}", #connection_name));
                }
            }
        };
        
        if is_async {
            quote! {
                pub async fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::Params;

                    #conn_acquire_code

                    #params_prepare

                    #call_code
                }
            }
        } else {
            quote! {
                pub fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::Params;

                    #conn_acquire_code

                    #params_prepare

                    #call_code
                }
            }
        }
    } else {
        // Getting the connection type
        let connection_info = connection_info.expect("Should have connection parameter");

        // Function argument pattern
        let (call_code, params_prepare) = if is_transaction_type(&connection_info.type_name) ||
            is_db_driver_type(&connection_info.type_name) {
            // AkitaTransaction or DbDriver: Used directly
            generate_call_code_with_params(
                return_ty,
                sql_expr,
                func_args,
                Some(connection.clone()),
                &connection,
                is_async,
            )
        } else {
            // Akita type: Need to get connection
            let (call_code, params_prepare) = generate_call_code_with_params(
                return_ty,
                sql_expr,
                func_args,
                Some(connection.clone()),
                &Ident::new("akita_conn", Span::call_site()),
                is_async
            );
            let params_prepare = if is_async {
                quote! {
                    let mut akita_conn = #connection.acquire().await
                        .expect("Akita connection not initialized");
                    #params_prepare
                }
            } else {
                quote! {
                    let mut akita_conn = #connection.acquire()
                        .expect("Akita connection not initialized");
                    #params_prepare
                }
            };
            (call_code, params_prepare)
        };

        if is_async {
            quote! {
                pub async fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::Params;
                    #params_prepare

                    #call_code
                }
            }
        } else {
            quote! {
                pub fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::Params;
                    #params_prepare

                    #call_code
                }
            }
        }
    }
}

fn generate_xml_sql_code(
    func_name: &Ident,
    func_args: &Punctuated<FnArg, Comma>,
    return_ty: &ReturnType,
    xml_file: &str,
    sql_id: &str,
    is_async: bool
) -> TokenStream {
    // XML schemas should also require connection parameters
    // Check if there are connection parameters
    let connection_info = get_connection_param_name(func_args);
    // Getting the connection type
    let connection_info = connection_info.expect("Should have connection parameter");
    let (call_code, params_prepare) = generate_call_code_with_params(
        return_ty,
        "&sql",
        func_args,
        Some(connection_info.param_ident()),
        &Ident::new("conn", Span::call_site()),
        is_async
    );

    let crate_ident = crate_ident();
    let connection = connection_info.param_ident();

    // Different codes are generated depending on the connection type
    if is_akita_type(&connection_info.type_name) {
        if is_async {
            quote! {
                pub async fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::{Params, XmlSqlLoader};

                    let mut conn = #connection.acquire().await
                        .expect("Akita connection not initialized");
                    let xml_sql_loader = conn.xml_sql_loader();
                    let sql = xml_sql_loader.load_sql(#xml_file, #sql_id)
                        .expect(&format!("Failed to load SQL from {} with id {}", #xml_file, #sql_id));

                    #params_prepare

                    #call_code
                }
            }
        } else {
            quote! {
                pub fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::{Params, XmlSqlLoader};

                    let mut conn = #connection.acquire()
                        .expect("Akita connection not initialized");
                    let xml_sql_loader = conn.xml_sql_loader();
                    let sql = xml_sql_loader.load_sql(#xml_file, #sql_id)
                        .expect(&format!("Failed to load SQL from {} with id {}", #xml_file, #sql_id));

                    #params_prepare

                    #call_code
                }
            }
        }
    } else {
        // AkitaTransaction Or DbDriver
        quote! {
            pub fn #func_name(#func_args) #return_ty {
                use #crate_ident::prelude::{Params, XmlSqlLoader};
                let mut conn = &mut #connection;
                let xml_sql_loader = conn.xml_sql_loader();
                let sql = xml_sql_loader.load_sql(#xml_file, #sql_id)
                    .expect(&format!("Failed to load SQL from {} with id {}", #xml_file, #sql_id));

                #params_prepare

                #call_code
            }
        }
    }
}
// ========== Core: Generate calling code (with argument handling) ==========

fn generate_call_code_with_params(
    return_ty: &ReturnType,
    sql_expr: &str,
    func_args: &Punctuated<FnArg, Comma>,
    exclude_ident: Option<Ident>,
    connection: &Ident,
    is_async: bool,
) -> (TokenStream, TokenStream) {
    // Generate the parameter preparation code
    let params_prepare = generate_params_prepare_code(func_args, exclude_ident);

    // Generating execution code
    let call_code = if is_async {
        generate_async_execution_code(return_ty, sql_expr, connection)
    } else {
        generate_execution_code(return_ty, sql_expr, connection)
    };

    (call_code, params_prepare)
}

// Generate the parameter preparation code
fn generate_params_prepare_code(
    func_args: &Punctuated<FnArg, Comma>,
    exclude_ident: Option<Ident>,
) -> TokenStream {
    let mut params_prepare = quote! {
        let mut params_vec = Vec::new();
    };

    for arg in func_args {
        if let FnArg::Typed(pat_type) = arg {
            let arg_ident = &pat_type.pat;

            // Check if it should be excluded
            let should_include = if let Some(ref exclude) = exclude_ident {
                // Comparing identifiers
                if let Pat::Ident(pat_ident) = &**arg_ident {
                    &pat_ident.ident != exclude
                } else {
                    true
                }
            } else {
                true
            };

            if should_include {
                params_prepare = quote! {
                    #params_prepare
                    params_vec.push(#arg_ident.into_value());
                };
            }
        }
    }

    quote! {
        #params_prepare
        let params = Params::Positional(params_vec);
    }
}


// Generate execution code (using execute_result/execute_drop)
fn generate_execution_code(
    return_ty: &ReturnType,
    sql_expr: &str,
    connection: &Ident,
) -> TokenStream {
    // Check whether akita_ident is a transaction type
    let base_call = match return_ty {
        ReturnType::Type(_, ty) => {
            let type_string = ty.to_token_stream().to_string();
            let type_string_no_space = type_string.replace(' ', "");

            let is_update = (type_string_no_space.contains("u64")) &&
                (sql_expr.to_uppercase().contains("UPDATE") ||
                    sql_expr.to_uppercase().contains("DELETE"));
            let is_insert = (type_string_no_space.contains("u64")) &&
                (sql_expr.to_uppercase().contains("INSERT INTO") ||
                    (sql_expr.to_uppercase().contains("INSERT ") &&
                        sql_expr.to_uppercase().contains("INTO")));

            // Checks if it is a collection type
            let is_collection = type_string_no_space.contains("Vec<") &&
                !type_string_no_space.contains("Option<");
            let is_option = type_string_no_space.contains("Option<") &&
                !type_string_no_space.contains("Vec<");
            let has_result = type_string_no_space.contains("Result<");

            if has_result {
                //A type wrapped with Result
                if type_string_no_space.contains("Result<Option<") {
                    // Result<Option<T>> - Querying a single record
                    if is_insert {
                        quote! {
                            #connection.exec_drop(#sql_expr, params)?;
                            Ok(#connection.last_insert_id())
                        }
                    } else if is_update {
                        quote! {
                            #connection.exec_drop(#sql_expr, params)?;
                            Ok(#connection.affected_rows())
                        }
                    } else {
                        if is_option {
                            quote! {
                                #connection.exec_first_opt(#sql_expr, params)
                            }
                        } else {
                            quote! {
                                #connection.exec_first(#sql_expr, params)
                            }
                        }
                    }
                } else if type_string_no_space.contains("Result<Vec<") {
                    // Result<Vec<T>> - Querying multiple records
                    quote! {
                        #connection.exec_raw(#sql_expr, params)
                    }
                } else if type_string_no_space.contains("Result<u64") {
                    // Result<u64> - Update/delete operations
                    if is_insert {
                        quote! {
                            #connection.exec_drop(#sql_expr, params)?;
                            Ok(#connection.last_insert_id())
                        }
                    } else if is_update {
                        quote! {
                            #connection.exec_drop(#sql_expr, params)?;
                            Ok(#connection.affected_rows())
                        }
                    } else {
                        quote! {
                            #connection.exec_first(#sql_expr, params)
                        }
                    }
                } else if type_string_no_space.contains("Result<()") {
                    // Result<()> - An operation that returns no value
                    quote! {
                        #connection.exec_drop(#sql_expr, params)
                    }
                } else {
                    // The default Result type, assuming a single record query
                    quote! {
                        #connection.exec_first(#sql_expr, params)
                    }
                }
            } else if is_collection {
                // Return collection type directly - Query multiple records
                quote! {
                    #connection.exec_raw(#sql_expr, params)
                }
            } else if is_option {
                // Return Option type directly - query a single record
                if is_insert {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).unwrap_or_default();
                        let last_insert_id = #connection.last_insert_id();
                        Ok(last_insert_id)
                    }
                } else if is_update {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).unwrap_or_default();
                        let affected_rows = #connection.affected_rows();
                        Ok(affected_rows)
                    }
                } else {
                    quote! {
                        #connection.exec_first_opt(#sql_expr, params)
                    }
                }
            } else {
                // Other types, assuming a single record query
                if is_insert {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).unwrap_or_default();
                        let last_insert_id = #connection.last_insert_id();
                        Ok(last_insert_id)
                    }
                } else if is_update {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).unwrap_or_default();
                        let affected_rows = #connection.affected_rows();
                        Ok(affected_rows)
                    }
                } else {
                    if is_option {
                        quote! {
                                #connection.exec_first_opt(#sql_expr, params)
                            }
                    } else {
                        quote! {
                                #connection.exec_first(#sql_expr, params)
                            }
                    }
                }
            }
        }
        ReturnType::Default => {
            // Case with no return type - perform update/delete operation
            quote! {
                #connection.exec_drop(#sql_expr, params).unwrap_or_default();
            }
        }
    };
    // Wrapper calls are used directly if they are transactions, otherwise connections are used
    quote! {
        {
            #base_call
        }
    }
}


fn generate_async_execution_code(
    return_ty: &ReturnType,
    sql_expr: &str,
    connection: &Ident,
) -> TokenStream {
    // Check whether akita_ident is a transaction type
    let base_call = match return_ty {
        ReturnType::Type(_, ty) => {
            let type_string = ty.to_token_stream().to_string();
            let type_string_no_space = type_string.replace(' ', "");

            let is_update = (type_string_no_space.contains("u64")) &&
                (sql_expr.to_uppercase().contains("UPDATE") ||
                    sql_expr.to_uppercase().contains("DELETE"));
            let is_insert = (type_string_no_space.contains("u64")) &&
                (sql_expr.to_uppercase().contains("INSERT INTO") ||
                    (sql_expr.to_uppercase().contains("INSERT ") &&
                        sql_expr.to_uppercase().contains("INTO")));

            // Checks if it is a collection type
            let is_collection = type_string_no_space.contains("Vec<") &&
                !type_string_no_space.contains("Option<");
            let is_option = type_string_no_space.contains("Option<") &&
                !type_string_no_space.contains("Vec<");
            let has_result = type_string_no_space.contains("Result<");

            if has_result {
                //A type wrapped with Result
                if type_string_no_space.contains("Result<Option<") {
                    // Result<Option<T>> - Querying a single record
                    if is_insert {
                        quote! {
                            #connection.exec_drop(#sql_expr, params).await?;
                            Ok(#connection.last_insert_id().await)
                        }
                    } else if is_update {
                        quote! {
                            #connection.exec_drop(#sql_expr, params).await?;
                            Ok(#connection.affected_rows().await)
                        }
                    } else {
                        if is_option {
                            quote! {
                                #connection.exec_first_opt(#sql_expr, params).await
                            }
                        } else {
                            quote! {
                                #connection.exec_first(#sql_expr, params).await
                            }
                        }
                    }
                } else if type_string_no_space.contains("Result<Vec<") {
                    // Result<Vec<T>> - Querying multiple records
                    quote! {
                        #connection.exec_raw(#sql_expr, params).await
                    }
                } else if type_string_no_space.contains("Result<u64") {
                    // Result<u64> - Update/delete operations
                    if is_insert {
                        quote! {
                            #connection.exec_drop(#sql_expr, params).await?;
                            Ok(#connection.last_insert_id().await)
                        }
                    } else if is_update {
                        quote! {
                            #connection.exec_drop(#sql_expr, params).await?;
                            Ok(#connection.affected_rows().await)
                        }
                    } else {
                        quote! {
                            #connection.exec_first(#sql_expr, params).await
                        }
                    }
                } else if type_string_no_space.contains("Result<()") {
                    // Result<()> - An operation that returns no value
                    quote! {
                        #connection.exec_drop(#sql_expr, params).await
                    }
                } else {
                    // The default Result type, assuming a single record query
                    quote! {
                        #connection.exec_first(#sql_expr, params).await
                    }
                }
            } else if is_collection {
                // Return collection type directly - Query multiple records
                quote! {
                    #connection.exec_raw(#sql_expr, params).await
                }
            } else if is_option {
                // Return Option type directly - query a single record
                if is_insert {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).await.unwrap_or_default();
                        let last_insert_id = #connection.last_insert_id().await;
                        Ok(last_insert_id)
                    }
                } else if is_update {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).await.unwrap_or_default();
                        let affected_rows = #connection.affected_rows().await;
                        Ok(affected_rows)
                    }
                } else {
                    quote! {
                        #connection.exec_first_opt(#sql_expr, params).await
                    }
                }
            } else {
                // Other types, assuming a single record query
                if is_insert {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).await.unwrap_or_default();
                        let last_insert_id = #connection.last_insert_id().await;
                        Ok(last_insert_id)
                    }
                } else if is_update {
                    quote! {
                        let _ = #connection.exec_drop(#sql_expr, params).await.unwrap_or_default();
                        let affected_rows = #connection.affected_rows().await;
                        Ok(affected_rows)
                    }
                } else {
                    if is_option {
                        quote! {
                                #connection.exec_first_opt(#sql_expr, params).await
                            }
                    } else {
                        quote! {
                                #connection.exec_first(#sql_expr, params).await
                            }
                    }
                }
            }
        }
        ReturnType::Default => {
            // Case with no return type - perform update/delete operation
            quote! {
                #connection.exec_drop(#sql_expr, params).await.unwrap_or_default();
            }
        }
    };
    // Wrapper calls are used directly if they are transactions, otherwise connections are used
    quote! {
        {
            #base_call
        }
    }
}



// ========== Other helper functions ==========
#[allow(unused,dead_code)]
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub name: String,
    pub type_name: String,
    pub is_async: bool,
    pub is_transaction: bool,
    pub is_db_driver: bool,
    pub is_akita: bool,
    /// The original, unprocessed type string used for error messages
    pub raw_type_string: String,
}

impl ConnectionInfo {

    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            type_name: "".to_string(),
            is_async: false,
            is_transaction: false,
            is_db_driver: false,
            is_akita: false,
            raw_type_string: "".to_string(),
        }
    }

    fn param_ident(&self) -> Ident {
        Ident::new(&self.name, Span::call_site())
    }
}

fn is_akita_type(type_str: &str) -> bool {
    analyze_connection_type(type_str).is_akita
}

fn is_transaction_type(type_str: &str) -> bool {
    analyze_connection_type(type_str).is_transaction
}

fn is_db_driver_type(type_str: &str) -> bool {
    analyze_connection_type(type_str).is_db_driver
}

/// Get information about the connection parameters (Akita, AkitaTransaction, or DbDriver)
fn get_connection_param_name(inputs: &Punctuated<FnArg, Comma>) -> Option<ConnectionInfo> {
    for input in inputs {
        if let FnArg::Typed(pat_type) = input {
            let raw_type_str = pat_type.ty.to_token_stream().to_string();
            let type_str_no_space = raw_type_str.replace(' ', "");

            // Checks if it is a supported type
            let type_analysis = analyze_connection_type(&type_str_no_space);

            if type_analysis.is_connection_type {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    return Some(ConnectionInfo {
                        name: pat_ident.ident.to_string(),
                        type_name: type_str_no_space.clone(),
                        is_async: type_analysis.is_async,
                        is_transaction: type_analysis.is_transaction,
                        is_db_driver: type_analysis.is_db_driver,
                        is_akita: type_analysis.is_akita,
                        raw_type_string: raw_type_str.clone(),
                    });
                }
            }
        }
    }
    None
}

#[derive(Debug, Clone)]
struct TypeAnalysis {
    is_connection_type: bool,
    is_async: bool,
    is_transaction: bool,
    is_db_driver: bool,
    is_akita: bool,
}

fn analyze_connection_type(type_str: &str) -> TypeAnalysis {
    // Convert to lowercase so that it is not case sensitive
    let lower_type = type_str.to_lowercase();

    // Check for common keywords
    let contains_async = lower_type.contains("async") || type_str.contains("Async");
    let contains_sync = lower_type.contains("sync") || type_str.contains("Sync");
    let contains_transaction = lower_type.contains("transaction") || type_str.contains("Transaction") || lower_type.contains("tx");
    let contains_dbdriver = lower_type.contains("dbdriver") || type_str.contains("DbDriver");
    let contains_akita = lower_type.contains("akita") || type_str.contains("Akita");
    // Is it a connection type?
    let is_connection_type = contains_akita || contains_dbdriver || contains_transaction;

    // Intelligent inference asynchrony - synchronous by default
    let is_async = if contains_async {
        true  // Explicitly marked as asynchronous
    } else if contains_sync {
        false  // Explicitly marked as synchronous
    } else if contains_akita && !contains_transaction {
        // For the simple "Akita" type, further judgment is required
        // Check for common asynchronous type name patterns
        let is_explicit_async = type_str.contains("AkitaAsync") ||
            type_str.contains("AsyncAkita") ||
            type_str.contains("AAsync") ||
            type_str.ends_with("Async") ||
            type_str.starts_with("Async");

        let is_explicit_sync = type_str.contains("AkitaSync") ||
            type_str.contains("SyncAkita") ||
            type_str.ends_with("Sync") ||
            type_str.starts_with("Sync");

        if is_explicit_async {
            true
        } else if is_explicit_sync {
            false
        } else {
            // It is set to synchronous by default, which is conservative
            false
        }
    } else if contains_transaction {
        // Asynchronous inference of transaction types
        let is_explicit_async = type_str.contains("AsyncAkitaTransaction") ||
            type_str.contains("AkitaAsyncTransaction") ||
            type_str.contains("AsyncTransaction") ||
            (type_str.contains("AkitaTransaction") && type_str.contains("Async"));

        let is_explicit_sync = type_str.contains("SyncAkitaTransaction") ||
            type_str.contains("AkitaSyncTransaction") ||
            type_str.contains("SyncTransaction") ||
            (type_str.contains("AkitaTransaction") && type_str.contains("Sync"));

        if is_explicit_async {
            true
        } else if is_explicit_sync {
            false
        } else {
            // It is set to synchronous by default
            false
        }
    } else if contains_dbdriver {
        // DbDriver Asynchronous inference of types
        let is_explicit_async = type_str.contains("AsyncDbDriver") ||
            type_str.contains("DbDriverAsync");

        let is_explicit_sync = type_str.contains("SyncDbDriver") ||
            type_str.contains("DbDriverSync");

        if is_explicit_async {
            true
        } else if is_explicit_sync {
            false
        } else {
            // It is set to synchronous by default
            false
        }
    } else {
        false
    };

    TypeAnalysis {
        is_connection_type,
        is_async,
        is_transaction: contains_transaction,
        is_db_driver: contains_dbdriver,
        is_akita: contains_akita && !contains_transaction && !contains_dbdriver,
    }
}
// Check if the function is async fn when parsing
fn is_async_function(target_fn: &ItemFn) -> bool {
    target_fn.sig.asyncness.is_some()
}