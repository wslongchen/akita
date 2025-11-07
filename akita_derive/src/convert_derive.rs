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

use proc_macro::{TokenStream};
use quote::quote;
use syn::{self, DeriveInput};

use crate::{util::{get_field_default_value, collect_field_info}, comm::FieldInformation};
use crate::comm::FieldExtra;

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
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let default_value = get_field_default_value(&field.field.ty, field.field.ident.as_ref().unwrap());
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
                        // 动态解析用户提供的路径（无需导入）
                        let reverter_ident: syn::Path = syn::parse_str(revert)
                            .expect("Invalid converter path");
                        revert_fields.push(quote! { data.#field_info = #reverter_ident::revert(&data.#field_info);});
                    }
                    _ => {

                    }
                }
            }
            if exist {
                quote!( #field_info: match data.get_obj(#field_name) { Ok(v) => v, Err(_) => { #default_value } },)
            } else {
                quote!( #field_info: #default_value,)
            }


        })
        .collect();

    let res = quote!(
        impl akita::core::FromValue for #name {

            fn from_value_opt(data: &akita::core::Value) -> std::result::Result<Self, akita::core::AkitaDataError> {
                use akita::Converter;
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
            // 存在才进行insert
            if exist {
                // insert with alias
                if let Some(converter) = converter {
                    // 动态解析用户提供的路径（无需导入）
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
        impl #generics akita::core::ToValue for #name #generics {

            fn to_value(&self) -> akita::core::Value {
                use akita::Converter;

                let mut data = akita::core::Value::new_object();
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
    // 生成字段验证代码
    if let syn::Type::Path(type_path) = field_type {
        let field_ident = &type_path.path.segments.last().unwrap().ident;

        // 检查字段类型是否实现了 `Converter` trait
        let trait_check = quote! {
            const _: () = {
                // 验证字段类型是否实现了 `Converter` trait
                fn _assert_trait_impl<T: akita::Converter>() {}
                _assert_trait_impl::<#field_ident>();
            };
        };

        return trait_check;
    }

    // 如果类型不支持，插入编译错误
    let error_message = format!(
        "Field '{}' has an unsupported type: {:?}. The type must implement the `Converter` trait.",
        field_name, field_type
    );
    quote! {
        compile_error!(#error_message);
    }
}
