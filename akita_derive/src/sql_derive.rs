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
use syn::{AttributeArgs, FnArg, ItemFn, Pat, ReturnType};
use crate::comm::crate_ident;

pub fn impl_sql(target_fn: &ItemFn, args: &AttributeArgs) -> TokenStream {
    let func_name_ident = &target_fn.sig.ident;

    // Parsing macro parameters
    let config = parse_sql_config(args, target_fn)
        .unwrap_or_else(|e| panic!("[Akita] {} in function '{}'", e, func_name_ident));

    let token = impl_sql_with_config(target_fn, &config);
    token
}

/// Parse the SQL XML macro parameters
pub fn parse_sql_xml_args(args: &AttributeArgs) -> Result<SqlConfig, String> {
    if args.is_empty() || args.len() > 3 {
        return Err(format!("sql_xml macro requires 1-3 arguments, got {}", args.len()));
    }

    let mut xml_file = None;
    let mut sql_id = None;
    let mut param_style = None;

    for (i, arg) in args.iter().enumerate() {
        match arg {
            syn::NestedMeta::Lit(syn::Lit::Str(lit_str)) => {
                if i == 0 {
                    xml_file = Some(lit_str.value());
                } else if i == 1 {
                    sql_id = Some(lit_str.value());
                } else {
                    return Err("Too many string literal arguments".to_string());
                }
            }
            syn::NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                if name_value.path.is_ident("param_style") {
                    if let syn::Lit::Str(lit_str) = &name_value.lit {
                        param_style = match lit_str.value().as_str() {
                            "positional" => Some(ParamStyle::Positional),
                            "named" => Some(ParamStyle::Named),
                            "numbered" => Some(ParamStyle::Numbered),
                            _ => return Err(format!(
                                "Invalid param_style value: {}. Must be 'positional', 'named', or 'numbered'",
                                lit_str.value()
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
            // Handle other types of literals (ignore or throw an error)
            syn::NestedMeta::Lit(lit) => {
                if i < 2 {
                    return Err(format!(
                        "Argument {} must be a string literal, got {:?}",
                        i + 1,
                        lit
                    ));
                }
            }
            // Handle other Meta types (ignore or throw errors)
            syn::NestedMeta::Meta(syn::Meta::Path(path)) => {
                return Err(format!(
                    "Unexpected path argument: {}. Use 'param_style = \"...\"' for named arguments",
                    path.to_token_stream()
                ));
            }
            syn::NestedMeta::Meta(syn::Meta::List(list)) => {
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

pub fn parse_query_args(args: &AttributeArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    // Support for multiple syntaxes：
    // 1. query("SELECT * FROM users") - Simple query Connection parameters are required
    // 2. query(akita, "SELECT * FROM users") - Explicit akita parameters
    // 3. query(file = "sql.xml", id = "getUser", param_style = "named") - Named parameter form

    if args.is_empty() {
        return Err("query macro requires at least one argument".to_string());
    }

    // Checks if it is in named argument form
    let mut has_named_args = false;
    for arg in args {
        if let syn::NestedMeta::Meta(syn::Meta::NameValue(_)) = arg {
            has_named_args = true;
            break;
        }
    }

    if has_named_args {
        // Named parameter form
        parse_named_query_args(args)
    } else {
        // Positional parametric form
        parse_positional_query_args(args, target_fn)
    }
}

fn parse_named_query_args(args: &AttributeArgs) -> Result<SqlConfig, String> {
    let mut file = None;
    let mut id = None;
    let mut sql = None;
    let mut param_style = None;
    let mut akita_name = None;

    for arg in args {
        match arg {
            syn::NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                let arg_name = name_value.path.get_ident()
                    .ok_or_else(|| "Argument must have a valid identifier".to_string())?
                    .to_string();

                match arg_name.as_str() {
                    "file" => {
                        if let syn::Lit::Str(lit_str) = &name_value.lit {
                            file = Some(lit_str.value());
                        } else {
                            return Err("file must be a string literal".to_string());
                        }
                    }
                    "id" => {
                        if let syn::Lit::Str(lit_str) = &name_value.lit {
                            id = Some(lit_str.value());
                        } else {
                            return Err("id must be a string literal".to_string());
                        }
                    }
                    "sql" => {
                        if let syn::Lit::Str(lit_str) = &name_value.lit {
                            sql = Some(lit_str.value());
                        } else {
                            return Err("sql must be a string literal".to_string());
                        }
                    }
                    "param_style" => {
                        if let syn::Lit::Str(lit_str) = &name_value.lit {
                            param_style = match lit_str.value().as_str() {
                                "positional" => Some(ParamStyle::Positional),
                                "named" => Some(ParamStyle::Named),
                                "numbered" => Some(ParamStyle::Numbered),
                                _ => return Err(format!(
                                    "Invalid param_style value: {}. Must be 'positional', 'named', or 'numbered'",
                                    lit_str.value()
                                )),
                            };
                        } else {
                            return Err("param_style must be a string literal".to_string());
                        }
                    }
                    "akita" => {
                        if let syn::Lit::Str(lit_str) = &name_value.lit {
                            akita_name = Some(lit_str.value());
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
        // XML
        Ok(SqlConfig {
            mode: SqlMode::Xml { file_path: file, sql_id: id },
            param_style,
        })
    } else if let Some(sql_str) = sql {
        // An explicit SQL schema
        if let Some(akita) = akita_name {
            Ok(SqlConfig {
                mode: SqlMode::Explicit{ conn_field: akita, sql: sql_str },
                param_style,
            })
        } else {
            Ok(SqlConfig {
                mode: SqlMode::Smart(sql_str),
                param_style,
            })
        }
    } else {
        Err("query macro requires either 'file' and 'id' or 'sql' parameter".to_string())
    }
}

pub fn parse_positional_query_args(args: &AttributeArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    // Delegate to standard sql macro parsing
    parse_sql_config(args, target_fn)
}



pub fn impl_sql_with_config(target_fn: &ItemFn, config: &SqlConfig) -> TokenStream {
    let return_ty = &target_fn.sig.output;
    let func_name_ident = &target_fn.sig.ident;
    let func_args = &target_fn.sig.inputs;
    // Generating code
    let code = match &config.mode {
        SqlMode::Explicit{ conn_field, sql } => {
            generate_explicit_sql_code(func_name_ident, func_args, return_ty, conn_field, sql)
        }
        SqlMode::Xml { file_path, sql_id } => {
            generate_xml_sql_code(func_name_ident, func_args, return_ty, file_path, sql_id)
        }
        SqlMode::Smart(sql_expr) => {
            generate_smart_sql_code(func_name_ident, func_args, return_ty, sql_expr)
        }
    };
    code
}

// ========== Configuration parsing ==========
#[derive(Debug)]
pub enum SqlMode {
    Explicit {
        conn_field: String,
        sql: String,
    },
    Xml{
        file_path: String,
        sql_id: String,
    },
    Smart(String),
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

fn parse_sql_config(args: &AttributeArgs, target_fn: &ItemFn) -> Result<SqlConfig, String> {
    match args.len() {
        1 => {
            let arg = &args[0];
            if let syn::NestedMeta::Lit(syn::Lit::Str(lit_str)) = arg {
                let value = lit_str.value();
                let param_style = detect_param_style(&value);

                // Check for the &self argument
                let has_self = target_fn.sig.inputs.iter().any(|input| {
                    if let FnArg::Receiver(_) = input { true } else { false }
                });

                if has_self {
                    // We have the &self argument → Explicit mode, and the default field name is "akita"
                    Ok(SqlConfig {
                        mode: SqlMode::Explicit {
                            conn_field: "akita".to_string(),  // Default field name
                            sql: value,
                        },
                        param_style,
                    })
                } else {
                    let connection_param = get_connection_param_name(&target_fn.sig.inputs);
                    match connection_param {
                        Some(conn_type) => {
                            Ok(SqlConfig {
                                mode: SqlMode::Explicit {
                                    conn_field: conn_type.name,
                                    sql: value
                                },
                                param_style,
                            })
                        }
                        None => {
                            // No akita parameters, depending on the SQL content
                            let has_named_params = value.contains(':') && value.chars().any(|c| c.is_alphabetic());
                            if has_named_params {
                                Ok(SqlConfig {
                                    mode: SqlMode::Smart(value),
                                    param_style,
                                })
                            } else {
                                let func_name = &target_fn.sig.ident;
                                Err(format!(
                                    "Function '{}' requires a connection parameter (Akita, AkitaTransaction, or DbDriver). \
                                 If using repository pattern, add &self parameter.",
                                    func_name
                                ))
                            }
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
            if let syn::NestedMeta::Lit(syn::Lit::Str(sql_lit)) = arg2 {
                let value = sql_lit.value();
                let param_style = detect_param_style(&value);

                Ok(SqlConfig {
                    mode: SqlMode::Explicit { conn_field: akita_ident, sql: value },
                    param_style,
                })
            } else {
                Err("Second argument must be a SQL string literal".to_string())
            }
        }
        3 => {
            // New: Support for specifying XML files
            let arg1 = &args[0];
            let arg2 = &args[1];
            let arg3 = &args[2];

            if let (
                syn::NestedMeta::Lit(syn::Lit::Str(file_path)),
                syn::NestedMeta::Lit(syn::Lit::Str(sql_id)),
            ) = (arg1, arg2) {
                let param_style = if let syn::NestedMeta::Meta(meta) = arg3 {
                    if let syn::Meta::NameValue(name_value) = meta {
                        if name_value.path.is_ident("param_style") {
                            if let syn::Lit::Str(lit_str) = &name_value.lit {
                                match lit_str.value().as_str() {
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
                    }
                } else {
                    None
                };

                Ok(SqlConfig {
                    mode: SqlMode::Xml { file_path: file_path.value(), sql_id: sql_id.value() },
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
    akita_ident: &str,
    sql_expr: &str,
) -> TokenStream {
    let akita_ident_token = Ident::new(akita_ident, Span::call_site());
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
        let call_code = generate_execution_code(
            return_ty,
            sql_expr,
            &Ident::new("conn", Span::call_site())
        );

        // Different connection acquisition codes are generated depending on the field type
        let conn_acquire_code = if let Some(info) = connection_info {
            if is_akita_type(&info.type_name) {
                // The Akita type requires the acquire() call.
                quote! {
                    let mut conn = self.#akita_ident_token.acquire()
                        .expect(&format!("Failed to acquire connection from self.{}", #akita_ident));
                }
            } else if is_transaction_type(&info.type_name) || is_db_driver_type(&info.type_name) {
                // AkitaTransaction and DbDriver are used directly
                quote! {
                    let mut conn = &mut self.#akita_ident_token;
                }
            } else {
                quote! {
                    compile_error!("Unsupported connection type in self.{}", #akita_ident);
                }
            }
        } else {
            // By default, the Akita type is assumed
            quote! {
                let mut conn = self.#akita_ident_token.acquire()
                    .expect(&format!("Failed to acquire connection from self.{}", #akita_ident));
            }
        };
        
        quote! {
            pub fn #func_name(#func_args) #return_ty {
                use #crate_ident::prelude::Params;
                
                #conn_acquire_code
                
                #params_prepare
                
                #call_code
            }
        }
    } else {
        // Function argument pattern
        // Getting the connection type
        let connection_info = connection_info.expect("Should have connection parameter");

        let (call_code, params_prepare) = if is_transaction_type(&connection_info.type_name) ||
            is_db_driver_type(&connection_info.type_name) {
            // AkitaTransaction or DbDriver: Used directly
            generate_call_code_with_params(
                return_ty,
                sql_expr,
                akita_ident,
                func_args,
                Some(akita_ident_token.clone())
            )
        } else {
            // Akita type: Need to get connection
            let (call_code, params_prepare) = generate_call_code_with_params(
                return_ty,
                sql_expr,
                "akita_conn",
                func_args,
                Some(akita_ident_token.clone())
            );
            let params_prepare = quote! {
                let mut akita_conn = #akita_ident_token.acquire()
                    .expect("Akita connection not initialized");
                #params_prepare
            };
            (call_code, params_prepare)
        };

        quote! {
            pub fn #func_name(#func_args) #return_ty {
                use #crate_ident::prelude::Params;
                #params_prepare

                #call_code
            }
        }
    }
}

fn generate_smart_sql_code(
    func_name: &Ident,
    func_args: &Punctuated<FnArg, Comma>,
    return_ty: &ReturnType,
    sql_expr: &str,
) -> TokenStream {
    // Check the parameter style first
    let param_style = detect_param_style(sql_expr);
    let (call_code, params_prepare) = match param_style {
        Some(ParamStyle::Named) => {
            // You need an IndexMap
            generate_named_params_code(return_ty, sql_expr, func_args)
        }
        _ => {
            // You don't need IndexMap
            generate_smart_call_code(return_ty, sql_expr, func_args)
        }
    };

    let crate_ident = crate_ident();

    // Dynamically generate imports based on whether an IndexMap is needed or not
    let imports = if matches!(param_style, Some(ParamStyle::Named)) {
        quote! {
            use #crate_ident::prelude::{AkitaGlobal, Params};
            use indexmap::IndexMap;
        }
    } else {
        quote! {
            use #crate_ident::prelude::{AkitaGlobal, Params};
        }
    };

    quote! {
        pub fn #func_name(#func_args) #return_ty {
            #imports

            let akita = AkitaGlobal::get_global_akita()
                .expect("Global Akita instance not initialized");
            let mut akita = akita.acquire().expect("Akita connection not initialized");
            #params_prepare

            #call_code
        }
    }
}


fn generate_xml_sql_code(
    func_name: &Ident,
    func_args: &Punctuated<FnArg, Comma>,
    return_ty: &ReturnType,
    xml_file: &str,
    sql_id: &str,
) -> TokenStream {
    // XML schemas should also require connection parameters
    // Check if there are connection parameters
    let connection_info = get_connection_param_name(func_args);
    
    let (call_code, params_prepare) = generate_call_code_with_params(
        return_ty,
        "&sql",
        "conn",
        func_args,
        connection_info.as_ref().map(|info| Ident::new(&info.name, Span::call_site()))
    );

    let crate_ident = crate_ident();

    // Different codes are generated depending on the connection type
    if let Some(info) = connection_info {
        let conn_name = info.name;
        if is_akita_type(&info.type_name) {
            quote! {
                pub fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::{Params, XmlSqlLoader};
                    
                    let mut conn = #conn_name.acquire()
                        .expect("Akita connection not initialized");
                    let xml_sql_loader = conn.xml_sql_loader();
                    let sql = xml_sql_loader.load_sql(#xml_file, #sql_id)
                        .expect(&format!("Failed to load SQL from {} with id {}", #xml_file, #sql_id));
                    
                    #params_prepare
                    
                    #call_code
                }
            }
        } else {
            // AkitaTransaction Or DbDriver
            quote! {
                pub fn #func_name(#func_args) #return_ty {
                    use #crate_ident::prelude::{Params, XmlSqlLoader};
                    
                    let mut conn = &#conn_name;
                    let xml_sql_loader = conn.xml_sql_loader();
                    let sql = xml_sql_loader.load_sql(#xml_file, #sql_id)
                        .expect(&format!("Failed to load SQL from {} with id {}", #xml_file, #sql_id));
                    
                    #params_prepare
                    
                    #call_code
                }
            }
        }
    } else {
        // No connection parameters, error
        let func_name_str = func_name.to_string();
        quote! {
            pub fn #func_name(#func_args) #return_ty {
                compile_error!(concat!(
                    "XML mode for '", #func_name_str, 
                    "' requires a connection parameter (&Akita, &AkitaTransaction, or &DbDriver)."
                ));
            }
        }
    }
}
// ========== Core: Generate calling code (with argument handling) ==========

fn generate_call_code_with_params(
    return_ty: &ReturnType,
    sql_expr: &str,
    akita_ident: &str,
    func_args: &Punctuated<FnArg, Comma>,
    exclude_ident: Option<Ident>,
) -> (TokenStream, TokenStream) {
    let akita_ident_token = Ident::new(akita_ident, Span::call_site());

    // Generate the parameter preparation code
    let params_prepare = generate_params_prepare_code(func_args, exclude_ident);

    // Generating execution code
    let call_code = generate_execution_code(return_ty, sql_expr, &akita_ident_token);

    (call_code, params_prepare)
}

// Intelligent code generation (supports named parameters)
fn generate_smart_call_code(
    return_ty: &ReturnType,
    sql_expr: &str,
    func_args: &Punctuated<FnArg, Comma>,
) -> (TokenStream, TokenStream) {
    // Detecting parameter styles
    let param_style = detect_param_style(sql_expr);
    match param_style {
        Some(ParamStyle::Named) => {
            // Named parameter pattern
            generate_named_params_code(return_ty, sql_expr, func_args)
        }
        Some(ParamStyle::Positional) | Some(ParamStyle::Numbered) => {
            // Position parameter mode
            generate_positional_params_code(return_ty, sql_expr, func_args)
        }
        None => {
            // Parameter-free mode
            generate_no_params_code(return_ty, sql_expr)
        }
    }
}

// Generate named parameter code
fn generate_named_params_code(
    return_ty: &ReturnType,
    sql_expr: &str,
    func_args: &Punctuated<FnArg, Comma>,
) -> (TokenStream, TokenStream) {
    // Extracting named Arguments
    let named_params = extract_named_params(sql_expr);

    // Generate the parameter mapping code
    let mut params_prepare = quote! {
        let mut params_map = IndexMap::new();
    };

    for (i, arg) in func_args.iter().enumerate() {
        if let FnArg::Typed(pat_type) = arg {
            let arg_ident = &pat_type.pat;
            let param_name = if i < named_params.len() {
                &named_params[i]
            } else {
                // By default, the parameter name is used
                &extract_ident_name(&arg_ident.to_token_stream())
            };

            params_prepare = quote! {
                #params_prepare
                params_map.insert(#param_name.to_string(), #arg_ident.into_value());
            };
        }
    }

    params_prepare = quote! {
        #params_prepare
        let params = Params::Named(params_map);
    };

    // Generating execution code
    let call_code = generate_execution_code(return_ty, sql_expr, &Ident::new("akita", Span::call_site()));

    (call_code, params_prepare)
}

// Generate the position parameter code
fn generate_positional_params_code(
    return_ty: &ReturnType,
    sql_expr: &str,
    func_args: &Punctuated<FnArg, Comma>,
) -> (TokenStream, TokenStream) {
    let mut params_prepare = quote! {
        let mut params_vec = Vec::new();
    };

    for arg in func_args {
        if let FnArg::Typed(pat_type) = arg {
            let arg_ident = &pat_type.pat;
            params_prepare = quote! {
                #params_prepare
                params_vec.push(#arg_ident.into_value());
            };
        }
    }

    params_prepare = quote! {
        #params_prepare
        let params = Params::Positional(params_vec);
    };

    let call_code = generate_execution_code(return_ty, sql_expr, &Ident::new("akita", Span::call_site()));

    (call_code, params_prepare)
}

// Generate parameter-free code
fn generate_no_params_code(
    return_ty: &ReturnType,
    sql_expr: &str,
) -> (TokenStream, TokenStream) {
    let params_prepare = quote! {
        let params = Params::None;
    };

    let call_code = generate_execution_code(return_ty, sql_expr, &Ident::new("akita", Span::call_site()));

    (call_code, params_prepare)
}
// Extract named parameters from SQL
fn extract_named_params(sql: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == ':' {
            let mut param = String::new();
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_alphanumeric() || next_ch == '_' {
                    param.push(chars.next().unwrap());
                } else {
                    break;
                }
            }
            if !param.is_empty() && !params.contains(&param) {
                params.push(param);
            }
        }
    }

    params
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
    akita_ident: &Ident,
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
                            #akita_ident.exec_drop(#sql_expr, params)?;
                            Ok(#akita_ident.last_insert_id())
                        }
                    } else if is_update {
                        quote! {
                            #akita_ident.exec_drop(#sql_expr, params)?;
                            Ok(#akita_ident.affected_rows())
                        }
                    } else {
                        if is_option {
                            quote! {
                                #akita_ident.exec_first_opt(#sql_expr, params)
                            }
                        } else {
                            quote! {
                                #akita_ident.exec_first(#sql_expr, params)
                            }
                        }
                    }
                } else if type_string_no_space.contains("Result<Vec<") {
                    // Result<Vec<T>> - Querying multiple records
                    quote! {
                        #akita_ident.exec_raw(#sql_expr, params)
                    }
                } else if type_string_no_space.contains("Result<u64") {
                    // Result<u64> - Update/delete operations
                    if is_insert {
                        quote! {
                            #akita_ident.exec_drop(#sql_expr, params)?;
                            Ok(#akita_ident.last_insert_id())
                        }
                    } else if is_update {
                        quote! {
                            #akita_ident.exec_drop(#sql_expr, params)?;
                            Ok(#akita_ident.affected_rows())
                        }
                    } else {
                        quote! {
                            #akita_ident.exec_first(#sql_expr, params)
                        }
                    }
                } else if type_string_no_space.contains("Result<()") {
                    // Result<()> - An operation that returns no value
                    quote! {
                        #akita_ident.exec_drop(#sql_expr, params)
                    }
                } else {
                    // The default Result type, assuming a single record query
                    quote! {
                        #akita_ident.exec_first(#sql_expr, params)
                    }
                }
            } else if is_collection {
                // Return collection type directly - Query multiple records
                quote! {
                    #akita_ident.exec_raw(#sql_expr, params)
                }
            } else if is_option {
                // Return Option type directly - query a single record
                if is_insert {
                    quote! {
                        #akita_ident.exec_drop(#sql_expr, params)
                            .map(|_| #akita_ident.last_insert_id())
                            .unwrap_or(0)
                    }
                } else if is_update {
                    quote! {
                        #akita_ident.exec_drop(#sql_expr, params)
                            .map(|_| #akita_ident.affected_rows())
                            .unwrap_or(0)
                    }
                } else {
                    quote! {
                        #akita_ident.exec_first_opt(#sql_expr, params)
                    }
                }
            } else {
                // Other types, assuming a single record query
                if is_insert {
                    quote! {
                        #akita_ident.exec_drop(#sql_expr, params)
                            .map(|_| #akita_ident.last_insert_id())
                            .unwrap_or(0)
                    }
                } else if is_update {
                    quote! {
                        #akita_ident.exec_drop(#sql_expr, params)
                            .map(|_| #akita_ident.affected_rows())
                            .unwrap_or(0)
                    }
                } else {
                    if is_option {
                        quote! {
                                #akita_ident.exec_first_opt(#sql_expr, params)
                            }
                    } else {
                        quote! {
                                #akita_ident.exec_first(#sql_expr, params)
                            }
                    }
                }
            }
        }
        ReturnType::Default => {
            // Case with no return type - perform update/delete operation
            quote! {
                #akita_ident.exec_drop(#sql_expr, params)
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

/// 提Take the identifier name (without modifiers such as mut)
fn extract_ident_name(ident: &TokenStream) -> String {
    let ident_str = ident.to_string();
    ident_str
        .trim_start_matches("mut ")
        .trim()
        .to_string()
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    name: String,
    type_name: String,
}

fn is_akita_type(type_str: &str) -> bool {
    type_str.contains("Akita") && !type_str.contains("Transaction")
}

fn is_transaction_type(type_str: &str) -> bool {
    type_str.contains("AkitaTransaction")
}

fn is_db_driver_type(type_str: &str) -> bool {
    type_str.contains("DbDriver")
}

/// 获取连接参数的信息（Akita、AkitaTransaction 或 DbDriver）
fn get_connection_param_name(inputs: &Punctuated<FnArg, Comma>) -> Option<ConnectionInfo> {
    for input in inputs {
        if let FnArg::Typed(pat_type) = input {
            let type_str = pat_type.ty.to_token_stream().to_string();
            let type_str_no_space = type_str.replace(' ', "");

            // 检查是否是支持的连接类型
            if is_akita_type(&type_str_no_space) ||
                is_transaction_type(&type_str_no_space) ||
                is_db_driver_type(&type_str_no_space) {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    return Some(ConnectionInfo {
                        name: pat_ident.ident.to_string(),
                        type_name: type_str_no_space,
                    });
                }
            }
        }
    }
    None
}