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
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Variant, Meta, NestedMeta};
use proc_macro2::Span;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use crate::comm::crate_ident;

/// A derived macro that implements FromAkitaValue and IntoAkitaValue for enums
pub fn derive_akita_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // A variant of parsing enumeration
    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("AkitaEnum can only be derived for enums"),
    };

    // Check for the akita_enum property
    let enum_storage = parse_enum_storage_attr(&input.attrs);

    // Generates an implementation of FromAkitaValue
    let from_value_impl = generate_from_value_impl(name, variants, &enum_storage);

    // Generates an implementation of IntoAkitaValue
    let into_value_impl = generate_into_value_impl(name, variants, &enum_storage);

    let expanded = quote! {
        #from_value_impl
        #into_value_impl
    };
    expanded.into()
}

/// Resolves the storage mode properties of enums
fn parse_enum_storage_attr(attrs: &[syn::Attribute]) -> EnumStorage {
    for attr in attrs {
        if attr.path.is_ident("akita_enum") {
            if let Ok(meta) = attr.parse_meta() {
                if let Meta::List(list) = meta {
                    for nested in list.nested.iter() {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("storage") {
                                if let syn::Lit::Str(lit_str) = &nv.lit {
                                    return match lit_str.value().as_str() {
                                        "string" => EnumStorage::String,
                                        "int" => EnumStorage::Int,
                                        "ordinal" => EnumStorage::Ordinal,
                                        "json" => EnumStorage::Json,
                                        _ => EnumStorage::String, // 默认
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    EnumStorage::String
}

#[derive(Debug, Clone)]
enum EnumStorage {
    String,
    Int,
    Ordinal,
    Json,
}

/// Generates the FromAkitaValue implementation
fn generate_from_value_impl(
    enum_name: &syn::Ident,
    variants: &Punctuated<Variant, Comma>,
    storage: &EnumStorage,
) -> proc_macro2::TokenStream {
    // The main matching branch is generated
    let mut main_match_arms = proc_macro2::TokenStream::new();
    let crate_ident = crate_ident();
    for variant in variants {
        let variant_name = &variant.ident;
        let variant_str = variant.ident.to_string();

        let arm = match storage {
            EnumStorage::String => {
                quote! {
                    #crate_ident::prelude::AkitaValue::Text(ref s) if s == #variant_str => {
                        Ok(#enum_name::#variant_name)
                    },
                }
            }
            EnumStorage::Int | EnumStorage::Ordinal => {
                let variant_index = get_variant_index(variant, storage, variants);
                quote! {
                    #crate_ident::prelude::AkitaValue::Bigint(#variant_index) => {
                        Ok(#enum_name::#variant_name)
                    },
                }
            }
            EnumStorage::Json => {
                generate_json_from_match_arm(enum_name, variant)
            }
        };
        main_match_arms.extend(arm);
    }

    // Generate case-insensitive String matches (for string storage only)
    let case_insensitive_match = if let EnumStorage::String = storage {
        // A list of variant names is generated for error messages
        let variant_names: Vec<String> = variants.iter()
            .map(|v| v.ident.to_string())
            .collect();
        let variant_names_str = variant_names.join(", ");

        // Generate case-insensitive matching branches
        let mut case_insensitive_arms = proc_macro2::TokenStream::new();
        for variant in variants {
            let variant_name = &variant.ident;
            let variant_str_lower = variant.ident.to_string().to_lowercase();
            case_insensitive_arms.extend(quote! {
                _ if s.to_lowercase() == #variant_str_lower => Ok(#enum_name::#variant_name),
            });
        }

        if !variant_names.is_empty() {
            quote! {
                #crate_ident::prelude::AkitaValue::Text(ref s) => {
                    match s.as_str() {
                        #case_insensitive_arms
                        _ => return Err(#crate_ident::prelude::AkitaDataError::ConversionError(
                            #crate_ident::prelude::ConversionError::TypeMismatch {
                                expected: format!("one of: {}", #variant_names_str),
                                found: format!("string '{}'", s),
                            }
                        ).into()),
                    }
                },
            }
        } else {
            quote! {}
        }
    } else {
        quote! {}
    };

    quote! {
        impl #crate_ident::prelude::FromAkitaValue for #enum_name {
            fn from_value_opt(value: &#crate_ident::prelude::AkitaValue) -> std::result::Result<Self, #crate_ident::prelude::AkitaDataError> {
                match value {
                    #main_match_arms
                    #case_insensitive_match
                    _ => Err(#crate_ident::prelude::AkitaDataError::ConversionError(
                        #crate_ident::prelude::ConversionError::TypeMismatch {
                            expected: std::any::type_name::<Self>().to_string(),
                            found: format!("{:?}", value),
                        }
                    ).into()),
                }
            }
        }
    }
}

/// Generate the IntoAkitaValue implementation
fn generate_into_value_impl(
    enum_name: &syn::Ident,
    variants: &Punctuated<Variant, Comma>,
    storage: &EnumStorage,
) -> proc_macro2::TokenStream {
    let mut match_arms = proc_macro2::TokenStream::new();
    let crate_ident = crate_ident();
    for variant in variants {
        let variant_name = &variant.ident;
        let variant_str = variant.ident.to_string();

        let arm = match &variant.fields {
            Fields::Unit => {
                match storage {
                    EnumStorage::String => {
                        quote! {
                            #enum_name::#variant_name => {
                                #crate_ident::prelude::AkitaValue::Text(#variant_str.to_string())
                            },
                        }
                    }
                    EnumStorage::Int | EnumStorage::Ordinal => {
                        let variant_index = get_variant_index(variant, storage, variants);
                        quote! {
                            #enum_name::#variant_name => {
                                #crate_ident::prelude::AkitaValue::Bigint(#variant_index)
                            },
                        }
                    }
                    EnumStorage::Json => {
                        quote! {
                            #enum_name::#variant_name => {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("type".to_string(),
                                    #crate_ident::prelude::AkitaValue::Text(#variant_str.to_string()));
                                #crate_ident::prelude::AkitaValue::Object(map)
                            },
                        }
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let field_count = fields.unnamed.len();
                let field_idents: Vec<syn::Ident> = (0..field_count)
                    .map(|i| syn::Ident::new(&format!("field{}", i), Span::call_site()))
                    .collect();

                match storage {
                    EnumStorage::Json => {
                        // Generate a list of field values
                        let mut field_values = proc_macro2::TokenStream::new();
                        for (i, ident) in field_idents.iter().enumerate() {
                            field_values.extend(quote! {
                                #ident.into_value()
                            });
                            if i < field_count - 1 {
                                field_values.extend(quote! { , });
                            }
                        }

                        quote! {
                            #enum_name::#variant_name(#(#field_idents),*) => {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("type".to_string(),
                                    #crate_ident::prelude::AkitaValue::Text(#variant_str.to_string()));
                                map.insert("data".to_string(),
                                    #crate_ident::prelude::AkitaValue::List(vec![#field_values]));
                                #crate_ident::prelude::AkitaValue::Object(map)
                            },
                        }
                    }
                    _ => {
                        // Non-json stores do not support enums with fields
                        panic!("Enum variants with fields can only be stored as JSON. Use #[akita_enum(storage = \"json\")]");
                    }
                }
            }
            Fields::Named(fields) => {
                let field_idents: Vec<&syn::Ident> = fields.named.iter()
                    .filter_map(|f| f.ident.as_ref())
                    .collect();

                match storage {
                    EnumStorage::Json => {
                        // Generating field mappings
                        let mut field_inserts = proc_macro2::TokenStream::new();
                        for ident in &field_idents {
                            let field_name = ident.to_string();
                            field_inserts.extend(quote! {
                                map.insert(#field_name.to_string(), #ident.into_value());
                            });
                        }

                        // A list of fields is generated for pattern matching
                        let mut field_list = proc_macro2::TokenStream::new();
                        for (i, ident) in field_idents.iter().enumerate() {
                            field_list.extend(quote! { #ident });
                            if i < field_idents.len() - 1 {
                                field_list.extend(quote! { , });
                            }
                        }

                        quote! {
                            #enum_name::#variant_name { #field_list } => {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("type".to_string(),
                                    #crate_ident::prelude::AkitaValue::Text(#variant_str.to_string()));
                                #field_inserts
                                #crate_ident::prelude::AkitaValue::Object(map)
                            },
                        }
                    }
                    _ => {
                        panic!("Enum variants with fields can only be stored as JSON. Use #[akita_enum(storage = \"json\")]");
                    }
                }
            }
        };
        match_arms.extend(arm);
    }

    quote! {
        impl #crate_ident::prelude::IntoAkitaValue for #enum_name {
            fn into_value(&self) -> #crate_ident::prelude::AkitaValue {
                match self {
                    #match_arms
                }
            }
        }
    }
}

/// Gets the index or integer value of the variant
fn get_variant_index(
    variant: &Variant,
    storage: &EnumStorage,
    all_variants: &Punctuated<Variant, Comma>,
) -> i64 {
    // Check for explicit akita_enum(value =...) Attributes
    for attr in &variant.attrs {
        if attr.path.is_ident("akita_enum") {
            if let Ok(meta) = attr.parse_meta() {
                if let Meta::List(list) = meta {
                    for nested in list.nested.iter() {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("value") {
                                if let syn::Lit::Int(lit_int) = &nv.lit {
                                    return lit_int.base10_parse::<i64>().unwrap_or(0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Check if there is an explicit discriminant
    if let Some((_, discriminant)) = &variant.discriminant {
        if let syn::Expr::Lit(expr_lit) = &discriminant {
            if let syn::Lit::Int(lit_int) = &expr_lit.lit {
                return lit_int.base10_parse::<i64>().unwrap_or(0);
            }
        }
    }

    // A default value is calculated based on the storage type
    match storage {
        EnumStorage::Int => {
            // For Int storage, if there is no explicit value, the position index is used
            let position = all_variants.iter()
                .position(|v| v.ident == variant.ident)
                .unwrap_or(0) as i64;
            position
        }
        EnumStorage::Ordinal => {
            // Ordinal stores always use the position index (starting from 0).
            let position = all_variants.iter()
                .position(|v| v.ident == variant.ident)
                .unwrap_or(0) as i64;
            position
        }
        _ => {
            // By default, 0 is used
            0
        }
    }
}

/// Generates the From match branch in JSON format
fn generate_json_from_match_arm(
    enum_name: &syn::Ident,
    variant: &Variant,
) -> proc_macro2::TokenStream {
    let variant_name = &variant.ident;
    let variant_str = variant.ident.to_string();
    let crate_ident = crate_ident();
    match &variant.fields {
        Fields::Unit => {
            quote! {
                #crate_ident::prelude::AkitaValue::Object(ref map) if map.get("type").map(|v|
                    if let #crate_ident::prelude::AkitaValue::Text(s) = v { s == #variant_str } else { false }
                ).unwrap_or(false) => {
                    Ok(#enum_name::#variant_name)
                },
            }
        }
        Fields::Unnamed(fields) => {
            let field_count = fields.unnamed.len();

            if field_count > 0 {
                // Generating field transformations
                let mut field_conversions = proc_macro2::TokenStream::new();
                for i in 0..field_count {
                    field_conversions.extend(quote! {
                        <_ as #crate_ident::prelude::FromAkitaValue>::from_value_opt(
                            iter.next().ok_or_else(|| #crate_ident::prelude::AkitaDataError::ConversionError(
                                #crate_ident::prelude::ConversionError::TypeMismatch {
                                    expected: #field_count.to_string() + " elements",
                                    found: list.len().to_string() + " elements",
                                }
                            ).into())?
                        )?
                    });
                    if i < field_count - 1 {
                        field_conversions.extend(quote! { , });
                    }
                }

                quote! {
                    #crate_ident::prelude::AkitaValue::Object(ref map) if map.get("type").map(|v|
                        if let #crate_ident::prelude::AkitaValue::Text(s) = v { s == #variant_str } else { false }
                    ).unwrap_or(false) => {
                        if let Some(#crate_ident::prelude::AkitaValue::List(ref list)) = map.get("data") {
                            if list.len() == #field_count {
                                let mut iter = list.iter();
                                Ok(#enum_name::#variant_name(
                                    #field_conversions
                                ))
                            } else {
                                Err(#crate_ident::prelude::AkitaDataError::ConversionError(
                                    #crate_ident::prelude::ConversionError::TypeMismatch {
                                        expected: #field_count.to_string() + " elements",
                                        found: list.len().to_string() + " elements",
                                    }
                                ).into())
                            }
                        } else {
                            Err(#crate_ident::prelude::AkitaDataError::ConversionError(
                                #crate_ident::prelude::ConversionError::MissingField {
                                    field: "data".to_string(),
                                    expected_type: "list".to_string(),
                                }
                            ).into())
                        }
                    },
                }
            } else {
                quote! {
                    #crate_ident::prelude::AkitaValue::Object(ref map) if map.get("type").map(|v|
                        if let #crate_ident::prelude::AkitaValue::Text(s) = v { s == #variant_str } else { false }
                    ).unwrap_or(false) => {
                        Ok(#enum_name::#variant_name())
                    },
                }
            }
        }
        Fields::Named(fields) => {
            let field_idents: Vec<&syn::Ident> = fields.named.iter()
                .filter_map(|f| f.ident.as_ref())
                .collect();

            if !field_idents.is_empty() {
                // Generate field extraction and transformation
                let mut field_extractions = proc_macro2::TokenStream::new();
                for ident in &field_idents {
                    let field_name = ident.to_string();
                    field_extractions.extend(quote! {
                        let #ident = map.get(#field_name)
                            .ok_or_else(|| #crate_ident::prelude::AkitaDataError::ConversionError(
                                #crate_ident::prelude::ConversionError::MissingField {
                                    field: #field_name.to_string(),
                                    expected_type: std::any::type_name::<core::any::TypeId>().to_string(),
                                }
                            ).into())?;
                        let #ident = <_ as #crate_ident::prelude::FromAkitaValue>::from_value_opt(#ident)?;
                    });
                }

                // Generate a list of fields
                let mut field_list = proc_macro2::TokenStream::new();
                for (i, ident) in field_idents.iter().enumerate() {
                    field_list.extend(quote! { #ident });
                    if i < field_idents.len() - 1 {
                        field_list.extend(quote! { , });
                    }
                }

                quote! {
                    #crate_ident::prelude::AkitaValue::Object(ref map) if map.get("type").map(|v|
                        if let #crate_ident::prelude::AkitaValue::Text(s) = v { s == #variant_str } else { false }
                    ).unwrap_or(false) => {
                        #field_extractions
                        Ok(#enum_name::#variant_name {
                            #field_list
                        })
                    },
                }
            } else {
                quote! {
                    #crate_ident::prelude::AkitaValue::Object(ref map) if map.get("type").map(|v|
                        if let #crate_ident::prelude::AkitaValue::Text(s) = v { s == #variant_str } else { false }
                    ).unwrap_or(false) => {
                        Ok(#enum_name::#variant_name {})
                    },
                }
            }
        }
    }
}