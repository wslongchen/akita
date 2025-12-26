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
use syn::{self, DeriveInput};


use crate::comm::{crate_ident, FieldExtra};
use crate::util::{extract_option_inner_type, is_option_type};
use crate::{comm::FieldInformation, util::{collect_field_info, get_field_default_value}};

pub fn impl_from_akita(input: TokenStream) -> TokenStream {
    let ast = syn::parse::<DeriveInput>(input).unwrap();
    let generics = &ast.generics;
    let fields = collect_field_info(&ast);
    let struct_info = &ast.ident;
    let res = build_from_akita(struct_info, generics, &fields);
    res.into()
}

pub fn build_from_akita(name: &syn::Ident, _generics: &syn::Generics, fields: &Vec<FieldInformation>) -> proc_macro2::TokenStream {
    let mut revert_fields = Vec::new();
    let crate_ident = crate_ident();
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let field_type = &field.field.ty;
            let mut field_name = field.name.to_string();
            let mut exist = true;
            let field_info = field.field.ident.as_ref().unwrap();
            for ext in field.extra.iter() {
                match ext {
                    FieldExtra::Name(v) => {
                        field_name = v.to_string();
                    }
                    FieldExtra::Exist(v) => {
                        exist = *v;
                    }
                    FieldExtra::Converter(revert) => {
                        validate_converter_field_type(&field.name, &field.field.ty);
                        // Dynamically resolve user-supplied paths (no import required)
                        let reverter_ident: syn::Path = syn::parse_str(revert)
                            .expect("Invalid converter path");
                        revert_fields.push(quote! { data.#field_info = #reverter_ident::revert(&data.#field_info);});
                    }
                    _ => {

                    }
                }
            }
            if exist {
                // Generate the correct conversion code, assuming the type implements FromAkitaValue
                // If it is not implemented, the compiler will throw an error, which is the responsibility of the user
                if is_option_type(field_type) {
                    if let Some(inner_type) = extract_option_inner_type(field_type) {
                        quote! {
                            #field_info: match data.get_obj(#field_name) {
                                Ok(v) => {
                                    if let #crate_ident::prelude::AkitaValue::Null = &v {
                                        None
                                    } else {
                                        // Call FromAkitaValue and let the compiler check
                                        match <#inner_type as #crate_ident::prelude::FromAkitaValue>::from_value_opt(&v) {
                                            Ok(val) => Some(val),
                                            Err(e) => {
                                                tracing::warn!("Failed to convert optional field '{}': {}", #field_name, e);
                                                None
                                            }
                                        }
                                    }
                                }
                                Err(_) => None,
                            },
                        }
                    } else {
                        // Unable to extract internal type, use Option's default value
                        quote!( #field_info: None, )
                    }
                } else {
                    // Non-option, must be convertible
                    quote! {
                        #field_info: match data.get_obj(#field_name) {
                            Ok(v) => {
                                // Call FromAkitaValue directly
                                // This is where the compiler will throw an error if the type is not implemented
                                <#field_type as #crate_ident::prelude::FromAkitaValue>::from_value_opt(&v)
                                    .unwrap_or_else(|e| {
                                        tracing::error!("Failed to convert field '{}': {}", #field_name, e);
                                        // Use Default as a fallback
                                        <#field_type as std::default::Default>::default()
                                    })
                            }
                            Err(e) => {
                                tracing::error!("Failed to get field '{}': {}", #field_name, e);
                                <#field_type as std::default::Default>::default()
                            }
                        },
                    }
                }
            } else {
                // If a field does not exist, use the default value
                let default_value = get_field_default_value(field_type, field_info);
                quote!( #field_info: #default_value, )
            }


        })
        .collect();

    let res = quote!(
        impl #crate_ident::prelude::FromAkitaValue for #name {

            fn from_value_opt(data: &#crate_ident::prelude::AkitaValue) -> std::result::Result<Self, #crate_ident::prelude::AkitaDataError> {
                use #crate_ident::prelude::Converter;
                let mut data = #name {
                    #(#from_fields)*
                };
                #(#revert_fields)*
                Ok(data)
            }
        }
    );
    res
}

pub fn impl_to_akita(input: TokenStream) -> TokenStream {
    let ast = syn::parse::<DeriveInput>(input).unwrap();
    let generics = &ast.generics;
    let fields = collect_field_info(&ast);
    let struct_info = &ast.ident;
    let res = build_to_akita(struct_info, generics, &fields);
    res.into()
}

pub fn build_to_akita(name: &syn::Ident, generics: &syn::Generics, fields: &Vec<FieldInformation>) -> proc_macro2::TokenStream {
    let crate_ident = crate_ident();
    let to_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let mut field_name = field.name.to_string();
            let mut exist = true;
            let field_info = field.field.ident.as_ref().unwrap();
            let mut converter = None;
            for ext in field.extra.iter() {
                match ext {
                    FieldExtra::Name(v) => {
                        field_name = v.to_string();
                    }
                    FieldExtra::Exist(v) => {
                        exist = *v;
                    }
                    FieldExtra::Converter(convert) => {
                        validate_converter_field_type(&field.name, &field.field.ty);
                        converter = Some(convert.to_string());
                    }
                    _ => {

                    }
                }
            }
            // INSERT IF PRESENT
            if exist {
                // insert with alias
                if let Some(converter) = converter {
                    // Dynamically resolve user-supplied paths (no import required)
                    let converter_ident: syn::Path = syn::parse_str(&converter)
                        .expect("Invalid converter path");
                    quote!( data.insert_obj(#field_name, #converter_ident::convert(&self.#field_info));)
                } else {
                    quote!( data.insert_obj(#field_name, &self.#field_info );)
                }
            } else {
                quote!()
            }

        })
        .collect();
    let res = quote!(
        impl #generics #crate_ident::prelude::IntoAkitaValue for #name #generics {

            fn into_value(&self) -> #crate_ident::prelude::AkitaValue {
                use #crate_ident::prelude::Converter;

                let mut data = #crate_ident::prelude::AkitaValue::new_object();
                #(#to_fields)*
                data
            }
        }
    );
    res
}

// Helper function to check if a type is supported
#[allow(unused)]
fn is_supported_type(field_type: &syn::Type) -> bool {
    matches!(
        field_type,
        syn::Type::Path(type_path) if {
            let last_segment = type_path.path.segments.last().unwrap();
            last_segment.ident != "Struct" && last_segment.ident != "Enum"
        }
    )
}

fn validate_converter_field_type(field_name: &str, field_type: &syn::Type) -> proc_macro2::TokenStream {
    let crate_ident = crate_ident();
    // Generate field validation code
    if let syn::Type::Path(type_path) = field_type {
        let field_ident = &type_path.path.segments.last().unwrap().ident;

        // Check if the field type implements the 'Converter' trait
        let trait_check = quote! {
            const _: () = {
                // Verify that the field type implements the 'Converter' trait
                fn _assert_trait_impl<T: #crate_ident::prelude::Converter>() {}
                _assert_trait_impl::<#field_ident>();
            };
        };

        return trait_check;
    }

    // If the type does not support it, a compiler error is inserted
    let error_message = format!(
        "[Akita] Field '{}' has an unsupported type: {:?}. The type must implement the `Converter` trait.",
        field_name, field_type
    );
    quote! {
        compile_error!(#error_message);
    }
}
