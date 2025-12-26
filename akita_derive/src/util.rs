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

use quote::{quote, ToTokens};
use syn::{self, Type, Ident, parse_quote, spanned::Spanned, ItemFn, ReturnType, FnArg, Pat, PathArguments};
use std::collections::HashMap;
use proc_macro2::{Span};
use proc_macro_error::{abort};
use crate::{comm::{FieldExtra, FieldInformation, CustomArgument, NUMBER_TYPES, CUSTOM_ARG_LIFETIME, CUSTOM_ARG_ALLOWED_COPY_TYPES, ValueOrPath}};
use crate::comm::ALLOW_TABLE_ID_TYPES;


/// get the field orignal type
#[allow(unused)]
pub fn get_field_type(ty: &syn::Type) -> Option<String> {
    match ty {
        Type::Path(r#path) => {
            let p = &r#path.path.segments[0];
            if p.ident == "Option" {
                match &p.arguments {
                    syn::PathArguments::AngleBracketed(path_arg) => {
                        let mut fy = String::default();
                        let _ = path_arg.args.iter().map(|arg| {
                            match arg {
                                syn::GenericArgument::Type(arg_type) => {
                                    match arg_type {
                                        Type::Path(arg_path) => {
                                            if let Some(arg_path_res) = arg_path.path.get_ident() {
                                                fy = arg_path_res.to_string();
                                            }
                                        },
                                        _ => {}
                                    }
                                },
                                _ => {},
                            }
                        }).collect::<Vec<_>>();
                        fy.into()
                    },
                    _ => {
                        None
                    },
                }
            } else {
                p.ident.to_string().into()
            }
        }
        _ => {
            None
        }
    }
}


#[allow(unused)]
/// Get the field default value with the FromAkitaValue check
pub fn get_field_default_value(ty: &Type, ident: &Ident) -> proc_macro2::TokenStream {
    let ident_name = ident.to_string();

    // Check if it's <T>an Option
    if is_option_type(ty) {
        // For Option<T>, check whether T implements FromAkitaValue
        if let Some(inner_ty) = extract_option_inner_type(ty) {
            if !is_builtin_type(&inner_ty) && !is_known_type(&inner_ty) {
                // Generates the FromAkitaValue check
                return generate_from_value_check(&inner_ty, &ident_name, "Option");
            }
        }
        return quote!(None);
    }

    // Checks for a type that requires special handling
    let type_name = get_type_name(ty);

    match type_name.as_str() {
        // Known built-in types (for which FromAkitaValue has been implemented)
        "f64" | "f32" | "u8" | "u16" | "u32" | "u64" | "u128" | "usize" |
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "bool" |
        "String" | "char" | "NaiveDate" | "NaiveDateTime" | "DateTime" |
        "Vec" | "Value" | "JsonValue" | "Uuid" | "BigDecimal" => {
            // These types already have default implementations that use default values
            get_builtin_default_value(ty, &type_name)
        }

        // Reference types
        _ if type_name.starts_with('&') => {
            // A reference type cannot have a default value; it must be an Option
            let error_message = format!(
                "Field `{}` has reference type `{}`.\n\
                 Reference fields must be `Option<{}>` to allow NULL values.",
                ident_name, type_name, type_name.trim_start_matches('&')
            );
            quote! { compile_error!(#error_message) }
        }

        // Custom type: Checks if FromAkitaValue is implemented
        _ => {
            if is_builtin_type(ty) || is_known_type(ty) {
                get_builtin_default_value(ty, &type_name)
            } else {
                // generate_from_value_check(ty, &ident_name, &type_name)
                quote!(<#ty as std::default::Default>::default())
            }
        }
    }
}

/// Checks if it is a built-in type
fn is_builtin_type(ty: &Type) -> bool {
    let type_name = get_type_name(ty);
    matches!(
        type_name.as_str(),
        "bool" | "i8" | "i16" | "i32" | "i64" | "i128" |
        "u8" | "u16" | "u32" | "u64" | "u128" |
        "f32" | "f64" | "isize" | "usize" |
        "char" | "str" | "String"
    )
}

/// Check if it's a known type (FromAkitaValue implemented)
fn is_known_type(ty: &Type) -> bool {
    let type_name = get_type_name(ty);
    matches!(
        type_name.as_str(),
        "NaiveDate" | "NaiveDateTime" | "DateTime" |
        "Vec" | "Value" | "JsonValue" | "Uuid" | "BigDecimal"
    )
}

/// Check if it's an Option<T>
pub fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Extract T from Option<T>
pub fn extract_option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        return Some(inner_ty);
                    }
                }
            }
        }
    }
    None
}

fn get_type_name(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                let mut type_name = segment.ident.to_string();

                // If there are generic arguments, add Angle brackets
                match &segment.arguments {
                    PathArguments::AngleBracketed(args) => {
                        if !args.args.is_empty() {
                            type_name.push('<');
                            // Generic parameters can be handled further
                            type_name.push_str("...");
                            type_name.push('>');
                        }
                    }
                    _ => {}
                }

                type_name
            } else {
                "Unknown".to_string()
            }
        }

        Type::Reference(type_ref) => {
            let base_type = get_type_name(&type_ref.elem);
            if type_ref.mutability.is_some() {
                format!("&mut {}", base_type)
            } else {
                format!("&{}", base_type)
            }
        }

        Type::Slice(type_slice) => {
            // Handling slice types, such as [u8]
            format!("[{}]", get_type_name(&type_slice.elem))
        }

        Type::Array(type_array) => {
            // Handle array types, such as [u8; 32]
            format!("[{}; {:?}]", get_type_name(&type_array.elem), type_array.len)
        }

        Type::Tuple(type_tuple) => {
            // Working with tuple types such as (i32, String)
            let elements: Vec<String> = type_tuple.elems.iter().map(get_type_name).collect();
            format!("({})", elements.join(", "))
        }

        _ => "Unknown".to_string(),
    }
}

fn get_builtin_default_value(ty: &Type, type_name: &str) -> proc_macro2::TokenStream {
    match type_name {
        "f64" | "f32" => quote!(0.0),
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" => quote!(0),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" => quote!(0),
        "bool" => quote!(false),
        "String" => quote!(String::new()),
        "char" => quote!(' '),
        "NaiveDate" => quote!(chrono::Local::now().naive_local().date()),
        "NaiveDateTime" => quote!(chrono::Local::now().naive_local()),
        "DateTime" => quote!(chrono::Local::now().fixed_offset()),
        "Vec" => quote!(Vec::new()),
        "Value" | "JsonValue" => quote!(serde_json::Value::Null),
        "Uuid" => quote!(uuid::Uuid::nil()),
        "BigDecimal" => quote!(bigdecimal::BigDecimal::from(0)),
        _ => quote!(<#ty as std::default::Default>::default()),
    }
}

/// Generate the FromAkitaValue check code
fn generate_from_value_check(ty: &Type, field_name: &str, type_name: &str) -> proc_macro2::TokenStream {
    let error_message = format!(
        "Type `{}` for field `{}` must implement `FromAkitaValue`.\n\n\
         To fix this:\n\
         1. Implement `FromAkitaValue` for `{}`\n\
         2. Or wrap it in `Option<{}>` if the field can be NULL\n\
         3. Or use a supported type that already implements `FromAkitaValue`\n\n\
         Supported types include:\n\
         - All primitive types (i32, f64, bool, etc.)\n\
         - String, char\n\
         - chrono types (NaiveDate, NaiveDateTime, DateTime)\n\
         - Option<T> (if T implements FromAkitaValue)\n\
         - Vec<T> (if T implements FromAkitaValue)\n\
         - serde_json::Value\n\
         - And any type that implements FromAkitaValue",
        type_name, field_name, type_name, type_name
    );
    quote! {
        compile_error!(#error_message)
    }
}

/// Finds all struct schema annotion
pub fn find_struct_annotations(struct_attrs: &[syn::Attribute]) -> Vec<FieldExtra> {
    let mut annotations = Vec::new();

    for attr in struct_attrs {
        if attr.path == parse_quote!(table) {
            if let Ok(extras) = parse_table_attribute(attr) {
                annotations.extend(extras);
            }
        } else if attr.path == parse_quote!(schema) {
            if let Ok(extra) = parse_schema_attribute(attr) {
                annotations.push(extra);
            }
        }
    }
    annotations
}

fn parse_table_attribute(attr: &syn::Attribute) -> syn::Result<Vec<FieldExtra>> {
    parse_attribute(attr, false)
}

fn parse_schema_attribute(attr: &syn::Attribute) -> syn::Result<FieldExtra> {
    let extras = parse_attribute(attr, true)?;

    if extras.len() == 1 {
        Ok(extras.into_iter().next().unwrap())
    } else {
        Err(syn::Error::new(attr.span(), "schema attribute must contain exactly one name"))
    }
}

fn parse_attribute(attr: &syn::Attribute, is_schema: bool) -> syn::Result<Vec<FieldExtra>> {
    let meta = attr.parse_meta()?;

    let extract_name = |meta_item: &syn::Meta| -> syn::Result<String> {
        match meta_item {
            syn::Meta::Path(path) => {
                path.get_ident()
                    .map(|ident| ident.to_string())
                    .ok_or_else(|| syn::Error::new(path.span(), "expected identifier"))
            }
            syn::Meta::NameValue(nv) if nv.path.is_ident("name") => {
                match &nv.lit {
                    syn::Lit::Str(s) => Ok(s.value()),
                    _ => Err(syn::Error::new(nv.lit.span(), "name must be a string")),
                }
            }
            _ => Err(syn::Error::new(
                meta_item.span(),
                format!("expected `name = \"...\"` or a single identifier")
            )),
        }
    };

    match &meta {
        syn::Meta::Path(_) | syn::Meta::NameValue(_) => {
            let name = extract_name(&meta)?;
            Ok(vec![create_field_extra(is_schema, &name)])
        }
        syn::Meta::List(list) => {
            let mut names = Vec::new();

            for nested in &list.nested {
                if let syn::NestedMeta::Meta(meta_item) = nested {
                    let name = extract_name(meta_item)?;
                    names.push(create_field_extra(is_schema, &name));
                } else {
                    return Err(syn::Error::new(
                        nested.span(),
                        "unexpected nested meta item"
                    ));
                }
            }

            if names.is_empty() {
                Err(syn::Error::new(list.span(), "must specify at least one name"))
            } else {
                Ok(names)
            }
        }
    }
}

fn create_field_extra(is_schema: bool, name: &str) -> FieldExtra {
    if is_schema {
        FieldExtra::Schema(name.to_string())
    } else {
        FieldExtra::Table(name.to_string())
    }
}

pub fn collect_field_info(ast: &syn::DeriveInput) -> Vec<FieldInformation> {
    let mut fields = collect_fields(ast);
    let field_types = find_fields_type(&fields);
    fields.drain(..).fold(vec![], |mut acc, field| {
        let key = field.ident.clone().unwrap().to_string();
        let (name, extra) = find_extra_for_field(&field, &field_types);
        // TABLE_ID ONLY SUPPORTS NUMERIC AND STRING TYPES
        let has_table_id = extra.iter().find(|ext| match ext {
            FieldExtra::TableId => true,
            _ => false,
        }).is_some();
        let file_type = field_types.get(&key).map(Clone::clone).unwrap_or_default().to_string();
        let type_string_no_space = file_type.replace(' ', "");
        if has_table_id && !ALLOW_TABLE_ID_TYPES.contains(&type_string_no_space.as_str()) {
            abort!(ast.span(), "#[id] can only be used with Long、Integer or String Types.")
        }
        acc.push(FieldInformation::new(
            field,
            file_type,
            name,
            extra,
        ));
        acc
    })
}

/// Find the types (as string) for each field of the struct
/// Needed for the `must_match` filter
pub fn find_fields_type(fields: &[syn::Field]) -> HashMap<String, String> {
    let mut types = HashMap::new();

    for field in fields {
        let field_ident = field.ident.clone().unwrap().to_string();
        let field_type = match field.ty {
            syn::Type::Path(syn::TypePath { ref path, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                path.to_tokens(&mut tokens);
                tokens.to_string().replace(' ', "")
            }
            syn::Type::Reference(syn::TypeReference { ref lifetime, ref elem, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                elem.to_tokens(&mut tokens);
                let mut name = tokens.to_string().replace(' ', "");
                if lifetime.is_some() {
                    name.insert(0, '&')
                }
                name
            }
            syn::Type::Group(syn::TypeGroup { ref elem, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                elem.to_tokens(&mut tokens);
                tokens.to_string().replace(' ', "")
            }
            _ => {
                let mut field_type = proc_macro2::TokenStream::new();
                field.ty.to_tokens(&mut field_type);
                abort!(
                    field.ty.span(),
                    "Type `{}` of field `{}` not supported",
                    field_type,
                    field_ident
                )
            }
        };
        types.insert(field_ident, field_type);
    }

    types
}

/// collect the ast fields
pub fn collect_fields(ast: &syn::DeriveInput) -> Vec<syn::Field> {
    match ast.data {
        syn::Data::Struct(syn::DataStruct { ref fields, .. }) => {
            if fields.iter().any(|field| field.ident.is_none()) {
                abort!(
                    fields.span(),
                    "struct has unnamed fields";
                    help = "#[derive(Entity)] can only be used on structs with named fields";
                );
            }
            fields.iter().cloned().collect::<Vec<_>>()
        }
        _ => abort!(ast.span(), "#[derive(Entity)] can only be used with structs"),
    }
}
/// Find everything we need to know about a field
pub fn find_extra_for_field(
    field: &syn::Field,
    _field_types: &HashMap<String, String>,
) -> (String, Vec<FieldExtra>) {
    let rust_ident = field.ident.clone().unwrap().to_string();
    let field_ident = field.ident.clone().unwrap().to_string();

    let error = |span: Span, msg: &str| -> ! {
        abort!(
            span,
            "Invalid attribute #[field] on field `{}`: {}",
            field.ident.clone().unwrap().to_string(),
            msg
        );
    };

    let mut extras = vec![];
    let mut has_field = false;

    for attr in &field.attrs {
        if attr.path != parse_quote!(field) && attr.path != parse_quote!(id) {
            continue;
        }
        if attr.path == parse_quote!(field) || attr.path != parse_quote!(id) {
            has_field = true;
        }

        match attr.parse_meta() {
            Ok(syn::Meta::List(syn::MetaList { ref nested, path, .. })) => {
                let meta_items = nested.iter().collect::<Vec<_>>();
                let tfield_type = path.get_ident().unwrap().to_string();
                if tfield_type.eq("id") {
                    extras.push(FieldExtra::TableId)
                } else if tfield_type.eq("field") {
                    extras.push(FieldExtra::Field)
                }
                // only field from there on
                for meta_item in meta_items {
                    match *meta_item {
                        syn::NestedMeta::Meta(ref item) => match *item {
                            // name, exist, fill, select
                            syn::Meta::Path(ref name) => {
                                match name.get_ident().unwrap().to_string() {
                                    // "fill" => {
                                    //     extras.push(FieldExtra::Name());
                                    // }
                                    _ => {
                                        let mut ident = proc_macro2::TokenStream::new();
                                        name.to_tokens(&mut ident);
                                        abort!(name.span(), "Unexpected annotion: {}", ident)
                                    }
                                }
                            }
                            // fill, name, select, numberic_scale, exist
                            syn::Meta::NameValue(syn::MetaNameValue {
                                                     ref path, ref lit, ..
                                                 }) => {
                                let ident = path.get_ident().unwrap();
                                match ident.to_string().as_ref() {
                                    "fill" => {
                                        match lit_to_string(lit) {
                                            Some(s) => extras.push(FieldExtra::Fill{
                                                function: s,
                                                mode: None,
                                                argument: None,
                                            }),
                                            None => error(lit.span(), "invalid argument for `fill` annotion: only strings are allowed"),
                                        };
                                    }
                                    "converter" => {
                                        match lit_to_string(lit) {
                                            Some(s) => extras.push(FieldExtra::Converter(s)),
                                            None => error(lit.span(), "invalid argument for `converter` annotion: only strings are allowed"),
                                        };
                                    }
                                    "name" => {
                                        match lit_to_string(lit) {
                                            Some(s) => extras.push(FieldExtra::Name(s)),
                                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                                        };
                                    }
                                    "id_type" => {
                                        match lit_to_string(lit) {
                                            Some(s) => match s.to_lowercase().as_ref() {
                                                "auto" | "none" | "input" | "assign_id" | "assign_uuid" => extras.push(FieldExtra::IdType(s)),
                                                _=> error(lit.span(), "invalid argument for `id_type` annotion: only `auto` `none` `input` `assign_id` `assign_uuid` are allowed")
                                            },
                                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                                        };
                                    }
                                    "select" => {
                                        match lit_to_bool(lit) {
                                            Some(s) => extras.push(FieldExtra::Select(s)),
                                            None => error(lit.span(), "invalid argument for `select` annotion: only boolean are allowed"),
                                        };
                                    }
                                    "exist" => {
                                        match lit_to_bool(lit) {
                                            Some(s) => extras.push(FieldExtra::Exist(s)),
                                            None => error(lit.span(), "invalid argument for `exist` annotion: only boolean are allowed"),
                                        };
                                    }
                                    "numberic_scale" => {
                                        match lit_to_u64_or_path(lit) {
                                            Some(s) => {
                                                assert_has_number(rust_ident.clone(), "numberic_scale", &field.ty);
                                                extras.push(FieldExtra::NumericScale(s));
                                            },
                                            None => error(lit.span(), "invalid argument for `numberic_scale` annotion: only strings are allowed"),
                                        };
                                    }
                                    v => abort!(
                                        path.span(),
                                        "unexpected name value annotion: {:?}",
                                        v
                                    ),
                                };
                            }
                            // Annotion with several args.
                            syn::Meta::List(syn::MetaList { ref path, ref nested, .. }) => {
                                let meta_items = nested.iter().cloned().collect::<Vec<_>>();
                                let ident = path.get_ident().unwrap();
                                match ident.to_string().as_ref() {
                                    "fill" => {
                                        extras.push(extract_fill_custom(
                                            rust_ident.clone(),
                                            attr,
                                            &meta_items,
                                        ));
                                    }
                                    "id_type"
                                    | "select"
                                    | "exist"
                                    | "name"
                                    | "numberic_scale" => {
                                        extras.push(extract_one_arg_annotion(
                                            "value",
                                            ident.to_string(),
                                            rust_ident.clone(),
                                            &meta_items,
                                        ));
                                    }
                                    v => abort!(path.span(), "unexpected list annotion: {:?}", v),
                                }
                            }
                        },
                        _ => unreachable!("Found a non Meta while looking for annotions"),
                    };
                }
            }
            Ok(syn::Meta::Path(ref name)) => {
                let ident = name.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "id" => {
                        extras.push(FieldExtra::TableId)
                    },
                    _ => extras.push(FieldExtra::Field),
                }
            },
            Ok(syn::Meta::NameValue(syn::MetaNameValue { ref lit, ref path, .. })) => {
                let ident = path.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "fill" => {
                        match lit_to_string(lit) {
                            Some(s) => extras.push(FieldExtra::Fill{
                                function: s,
                                mode: None,
                                argument: None,
                            }),
                            None => error(lit.span(), "invalid argument for `fill` annotion: only strings are allowed"),
                        };
                    }
                    "name" => {
                        match lit_to_string(lit) {
                            Some(s) => extras.push(FieldExtra::Name(s)),
                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                        };
                    }
                    "id_type" => {
                        match lit_to_string(lit) {
                            Some(s) => {
                                match s.to_lowercase().as_ref() {
                                    "auto" | "none" | "input" | "assign_id" | "assign_uuid" => extras.push(FieldExtra::IdType(s)),
                                    _=> error(lit.span(), "invalid argument for `id_type` annotion: only `auto` `none` `input` `assign_id` `assign_uuid` are allowed")
                                }

                            },
                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                        };
                    }
                    "select" => {
                        match lit_to_bool(lit) {
                            Some(s) => extras.push(FieldExtra::Select(s)),
                            None => error(lit.span(), "invalid argument for `select` annotion: only boolean are allowed"),
                        };
                    }
                    "exist" => {
                        match lit_to_bool(lit) {
                            Some(s) => extras.push(FieldExtra::Exist(s)),
                            None => error(lit.span(), "invalid argument for `exist` annotion: only boolean are allowed"),
                        };
                    }
                    "numberic_scale" => {
                        match lit_to_u64_or_path(lit) {
                            Some(s) => {
                                assert_has_number(rust_ident.clone(), "numberic_scale", &field.ty);
                                extras.push(FieldExtra::NumericScale(s));
                            },
                            None => error(lit.span(), "invalid argument for `numberic_scale` annotion: only strings are allowed"),
                        };
                    }
                    v => abort!(
                                        path.span(),
                                        "unexpected name value annotion: {:?}",
                                        v
                                    ),
                };
            },
            Err(e) => {
                let error_string = format!("{:?}", e);
                if error_string == "Error(\"expected literal\")" {
                    abort!(attr.span(),
                        "This attributes for the field `{}` seem to be misformed, please annotion the syntax with the documentation",
                        field_ident
                    );
                } else {
                    abort!(attr.span(),
                        "Unable to parse this attribute for the field `{}` with the error: {:?}",
                        field_ident, e
                    );
                }
            },
        }

        if has_field && extras.is_empty() {
            extras.push(FieldExtra::Field);
        }
    }

    (field_ident, extras)
}

/// For fill, name, exist, select, numberic_scale
pub fn extract_one_arg_annotion(
    val_name: &str,
    name: String,
    field: String,
    meta_items: &[syn::NestedMeta],
) -> FieldExtra {
    let mut value = None;
    for meta_item in meta_items {
        match *meta_item {
            syn::NestedMeta::Meta(ref item) => match *item {
                syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                    let ident = path.get_ident().unwrap();
                    match ident.to_string().as_str() {
                        v if v == val_name => {
                            value = match lit_to_string(lit) {
                                Some(s) => Some(s),
                                None => abort!(
                                    item.span(),
                                    "Invalid argument type for `{}` for annotion `{}` on field `{}`: only a string is allowed",
                                    val_name, name, field
                                ),
                            };
                        }
                        v => abort!(
                            path.span(),
                            "Unknown argument `{}` for annotion `{}` on field `{}`",
                            v,
                            name,
                            field
                        ),
                    }
                }
                _ => abort!(
                    item.span(),
                    "unexpected item {:?} while parsing `range` annotion",
                    item
                ),
            },
            _ => unreachable!(),
        }

        if value.is_none() {
            abort!(
                meta_item.span(),
                "Missing argument `{}` for annotion `{}` on field `{}`",
                val_name,
                name,
                field
            );
        }
    }

    let extra = match name.as_ref() {
        "fill" => FieldExtra::Fill { function: value.unwrap(), argument: None, mode: None },
        "id_type" => FieldExtra::IdType(value.unwrap()),
        "select" => FieldExtra::Select(value.unwrap().parse::<bool>().unwrap_or(true)),
        "exist" => FieldExtra::Exist(value.unwrap().parse::<bool>().unwrap_or(true)),
        "name" => FieldExtra::Name(value.unwrap()),
        // "numberic_scale" => FieldExtra::NumericScale(value.unwrap()),
        _ => unreachable!(),
    };
    extra
}

pub fn extract_fill_custom(
    field: String,
    attr: &syn::Attribute,
    meta_items: &[syn::NestedMeta],
) -> FieldExtra {
    let mut function = None;
    let mut argument = None;
    let mut mode = None;

    let error = |span: Span, msg: &str| -> ! {
        abort!(span, "Invalid attribute #[field] on field `{}`: {}", field, msg);
    };

    for meta_item in meta_items {
        match *meta_item {
            syn::NestedMeta::Meta(ref item) => match *item {
                syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                    let ident = path.get_ident().unwrap();
                    match ident.to_string().as_ref() {
                        "function" => {
                            function = match lit_to_string(lit) {
                                Some(s) => Some(s),
                                None => error(lit.span(), "invalid argument type for `function` of `fill` annotion: expected a string")
                            };
                        }
                        "mode" => {
                            mode = match lit_to_string(lit) {
                                Some(s) => match s.as_ref() {
                                    "default" | "insert" | "update" => {
                                        Some(s)
                                    }
                                    _ => {
                                        error(lit.span(), "invalid argument type for `mode` of `fill` annotion: expected `default`,`insert`,`update` ")
                                    }
                                },
                                None => error(lit.span(), "invalid argument type for `mode` of `fill` annotion: expected a string")
                            };
                        }
                        "arg" => {
                            match lit_to_string(lit) {
                                Some(s) => {
                                    match syn::parse_str::<syn::Type>(s.as_str()) {
                                        Ok(arg_type) => {
                                            assert_custom_arg_type(&lit.span(), &arg_type);
                                            argument = Some(CustomArgument::new(lit.span().clone(), arg_type));
                                        }
                                        Err(_) => {
                                            let mut msg = "invalid argument type for `arg` of `fill` annotion: The string has to be a single type.".to_string();
                                            msg.push_str("\n(Tip: You can combine multiple types into one tuple.)");

                                            error(lit.span(), msg.as_str());
                                        }
                                    }
                                },
                                None => error(lit.span(), "invalid argument type for `arg` of `fill` annotion: expected a string")
                            };
                        }
                        v => error(path.span(), &format!(
                            "unknown argument `{}` for annotion `fill` (it only has `function`, `arg`)",
                            v
                        )),
                    }
                }
                _ => abort!(
                    item.span(),
                    "unexpected item {:?} while parsing `fill` annotion",
                    item
                ),
            },
            _ => unreachable!(),
        }
    }

    if function.is_none() {
        error(attr.span(), "The annotion `custom` requires the `function` parameter.");
    }
    let extra = FieldExtra::Fill { function: function.unwrap(), argument, mode };
    extra
}

pub fn assert_has_number(field_name: String, type_name: &str, field_type: &syn::Type) {
    let type_string_no_space = type_name.replace(' ', "");
    if !NUMBER_TYPES.contains(&type_string_no_space.as_str()) {
        abort!(
            field_type.span(),
            "Entity `numberic_scale` can only be used on number types but found `{}` for field `{}`",
            type_name,
            field_name
        );
    }
}

pub fn assert_custom_arg_type(field_span: &Span, field_type: &syn::Type) {
    match field_type {
        syn::Type::Reference(reference) => {
            if let Some(lifetime) = &reference.lifetime {
                let lifetime_ident = lifetime.ident.to_string();
                if lifetime_ident != CUSTOM_ARG_LIFETIME {
                    abort!(
                        field_span,
                        "Invalid argument reference: The lifetime `'{}` is not supported. Please use the field lifetime `'{}`",
                        lifetime_ident,
                        CUSTOM_ARG_LIFETIME
                    );
                }
            } else {
                abort!(
                    field_span,
                    "Invalid argument reference: All references need to use the field lifetime `'{}`",
                    CUSTOM_ARG_LIFETIME
                );
            }
        }
        // trigger nested annotion
        syn::Type::Paren(paren) => {
            assert_custom_arg_type(field_span, &paren.elem);
        }
        syn::Type::Tuple(tuple) => {
            tuple.elems.iter().for_each(|x| assert_custom_arg_type(field_span, x));
        }
        // assert idents
        syn::Type::Path(path) => {
            let segments = &path.path.segments;
            if segments.len() == 1 {
                let ident = &segments.first().unwrap().ident.to_string();
                let type_string_no_space = ident.replace(' ', "");
                if CUSTOM_ARG_ALLOWED_COPY_TYPES.contains(&type_string_no_space.as_str()) {
                    // A known copy type that can be passed without a reference
                    return;
                }
            }

            abort!(
                field_span,
                "Invalid argument type: All types except numbers and tuples need be passed by reference using the lifetime `'{}`",
                CUSTOM_ARG_LIFETIME,
            );
        }
        // Not allows
        _ => {
            abort!(
                field_span,
                "Invalid argument type: Custom arguments only allow tuples, number types and references using the lifetime `'{}` ",
                CUSTOM_ARG_LIFETIME,
            );
        }
    }
}

pub fn lit_to_string(lit: &syn::Lit) -> Option<String> {
    match *lit {
        syn::Lit::Str(ref s) => Some(s.value()),
        _ => None,
    }
}

pub fn lit_to_int(lit: &syn::Lit) -> Option<u64> {
    match *lit {
        syn::Lit::Int(ref s) => Some(s.base10_parse().unwrap()),
        _ => None,
    }
}

pub fn lit_to_u64_or_path(lit: &syn::Lit) -> Option<ValueOrPath<u64>> {
    let number = lit_to_int(lit);
    if let Some(number) = number {
        return Some(ValueOrPath::Value(number));
    }

    let path = lit_to_string(lit);
    if let Some(path) = path {
        return Some(ValueOrPath::Path(path));
    }

    None
}

pub fn lit_to_bool(lit: &syn::Lit) -> Option<bool> {
    match *lit {
        syn::Lit::Bool(ref s) => Some(s.value),
        _ => None,
    }
}

#[allow(unused)]
pub fn option_to_tokens<T: quote::ToTokens>(opt: &Option<T>) -> proc_macro2::TokenStream {
    match opt {
        Some(ref t) => quote!(::std::option::Option::Some(#t)),
        None => quote!(::std::option::Option::None),
    }
}

//find and check method return type
pub(crate) fn find_return_type(target_fn: &ItemFn) -> proc_macro2::TokenStream {
    let mut return_ty = target_fn.sig.output.to_token_stream();
    match &target_fn.sig.output {
        ReturnType::Type(_, b) => {
            return_ty = b.to_token_stream();
        }
        _ => {}
    }
    let mut s = format!("{}", return_ty);

    if s.trim().is_empty() {
        return_ty = quote! {
            ()
        }
    }

    if !s.contains("::Result") && !s.starts_with("Result") {
        return_ty = quote! {
             Result <#return_ty, akita::AkitaError>
        };
    }
    return_ty
}

pub(crate) fn is_akita_ref(ty_stream: &str) -> bool {
    if ty_stream.contains("Akita")
        || ty_stream.contains("AkitaTransaction") {
        return true;
    }
    false
}

pub(crate) fn is_fetch(return_source: &str) -> bool {
    let is_select = !return_source.contains("()");
    return is_select;
}
pub(crate) fn is_fetch_array(return_source: &str) -> bool {
    let is_array = return_source.contains("Vec");
    return is_array;
}

pub fn to_snake_name(name: &String) -> String {
    let chs = name.chars();
    let mut new_name = String::new();
    let mut index = 0;
    let chs_len = name.len();
    for x in chs {
        if x.is_uppercase() {
            if index != 0 && (index + 1) != chs_len {
                new_name.push_str("_");
            }
            new_name.push_str(x.to_lowercase().to_string().as_str());
        } else {
            new_name.push(x);
        }
        index += 1;
    }
    return new_name;
}


/// find and check method return type
pub(crate) fn find_fn_body(target_fn: &ItemFn) -> proc_macro2::TokenStream {
    let mut target_fn = target_fn.clone();
    let mut new_stmts = vec![];
    for x in &target_fn.block.stmts {
        let token = x.to_token_stream().to_string().replace("\n", "").replace(" ", "");
        if token.eq("todo!()") || token.eq("unimplemented!()") || token.eq("impled!()") {
            //nothing to do
        } else {
            new_stmts.push(x.to_owned());
        }
    }
    target_fn.block.stmts = new_stmts;
    target_fn.block.to_token_stream()
}

pub(crate) fn get_fn_args(target_fn: &ItemFn) -> Vec<Box<Pat>> {
    let mut fn_arg_name_vec = vec![];
    for arg in &target_fn.sig.inputs {
        match arg {
            FnArg::Typed(t) => {
                fn_arg_name_vec.push(t.pat.clone());
                //println!("arg_name {}", arg_name);
            }
            _ => {}
        }
    }
    fn_arg_name_vec
}

pub(crate) fn filter_fn_args(
    target_fn: &ItemFn,
    arg_name: &str,
    arg_type: &str,
) -> std::collections::HashMap<String, String> {
    let mut map = HashMap::new();
    for arg in &target_fn.sig.inputs {
        match arg {
            FnArg::Typed(t) => {
                let arg_name_value = format!("{}", t.pat.to_token_stream());
                if arg_name.eq(&arg_name_value) {
                    map.insert(arg_name.to_string(), arg_name_value.clone());
                }
                let arg_type_name = t.ty.to_token_stream().to_string();
                if arg_type.eq(&arg_type_name) {
                    map.insert(arg_type.to_string(), arg_name_value.clone());
                }
            }
            _ => {}
        }
    }
    map
}

pub(crate) fn get_page_req_ident(target_fn: &ItemFn, func_name: &str) -> Ident {
    let page_reqs = filter_fn_args(target_fn, "", "&PageRequest");
    if page_reqs.len() > 1 {
        panic!(
            "[Akita] {} only support on arg of '**:&PageRequest'!",
            func_name
        );
    }
    if page_reqs.len() == 0 {
        panic!(
            "[Akita] {} method arg must have arg Type '**:&PageRequest'!",
            func_name
        );
    }
    let req = page_reqs
        .get("&PageRequest")
        .unwrap_or(&String::new())
        .to_owned();
    if req.eq("") {
        panic!(
            "[Akita] {} method arg must have arg Type '**:&PageRequest'!",
            func_name
        );
    }
    let req = Ident::new(&req, Span::call_site());
    req
}