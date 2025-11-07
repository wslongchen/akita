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
use syn::{DeriveInput};
use crate::{convert_derive::{build_to_akita, build_from_akita}, comm::{FieldExtra}, util::{find_struct_annotions, collect_field_info, to_snake_name}};

pub fn impl_get_table(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let res = parse_table(&derive_input);
    res.into()
}

fn parse_table(ast: &syn::DeriveInput) -> TokenStream {
    // extra annotion info
    // Struct specific definitions
    let generics = &ast.generics;
    let fields = collect_field_info(ast);
    let struct_info = &ast.ident;
    let struct_name = &ast.ident.to_string();
    let structs = find_struct_annotions(&ast.attrs);
    let mut table_name = structs.iter().find(|st| match st { FieldExtra::Table(_) => true, _ => false })
    .map(|extra| match extra {
        FieldExtra::Table(name) => name.clone(),
        _ => String::default()
    }).unwrap_or_default();

    if table_name.is_empty() {
        table_name = to_snake_name(struct_name);
    }

    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let name = field.name.clone();
            let mut alias = field.name.clone();
            let mut exist = true;
            let mut select = true;
            let mut identify = false;
            let mut identifier_type = quote!(akita::IdentifierType::None);
            let mut fill_function = String::default();
            let mut fill_mode = None;

            for extra in field.extra.iter() {
                match extra {
                    FieldExtra::Fill { ref function, ref mode, .. } => {
                        fill_function = function.clone();
                        fill_mode = mode.clone();
                    }
                    FieldExtra::Name(v) => {
                        alias = v.clone().into();
                    }
                    FieldExtra::Select(v) => {
                        select = v.clone();
                    }
                    FieldExtra::Exist(v) => {
                        exist = v.clone();
                    }
                    FieldExtra::NumericScale(_v) => {}
                    FieldExtra::TableId => {
                        identify = true;
                    }
                    FieldExtra::IdType(ty) => {
                        identifier_type = match ty.as_str() {
                            "auto" => quote!(akita::IdentifierType::Auto),
                            "input" => quote!(akita::IdentifierType::Input),
                            "assign_id" => quote!(akita::IdentifierType::AssignId),
                            "assign_uuid" => quote!(akita::IdentifierType::AssignUuid),
                            _ => quote!(akita::IdentifierType::None)
                        }

                    }
                    _ => {}
                }
            }

            let field_type = if identify { quote!(akita::FieldType::TableId(#identifier_type)) } else { quote!(akita::FieldType::TableField) };
            let fill_mode = fill_mode.unwrap_or(String::from("default")).to_lowercase();
            let fill = if fill_function.is_empty() { quote!(None) } else {
                let fn_ident: syn::Path = syn::parse_str(&fill_function).unwrap();
                quote!(akita::core::Fill {
                        value: Some(#fn_ident().to_value()),
                        mode: #fill_mode.to_string()
                    }.into())
            };

            quote!(
                akita::core::FieldName {
                    name: #name.to_string(),
                    table: #table_name.to_string().into(),
                    alias: #alias.to_string().into(),
                    field_type: #field_type,
                    fill: #fill,
                    select: #select,
                    exist: #exist,
                },
            )
        }).collect();

    let cols: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let field_name = field.field.ident.as_ref().unwrap();
            let mut name = field.name.clone();
            let mut exist = true;
            for extra in field.extra.iter() {
                match extra {
                    FieldExtra::Name(v) => {
                        name = v.clone();
                    }
                    FieldExtra::Exist(v) => {
                        exist = v.clone();
                    }
                    _ => {}
                }
            }
            if exist {
                quote!(
                    pub fn #field_name() -> String {
                        #name.to_string()
                    }

                    // #[allow(dead_code)]
                    // fn get_value(&self) -> akita::Value {
                    //     // 这里可以根据需要获取结构体值
                    //     self.#field_name.to_value()
                    // }
                )
            } else {
                quote!(
                )
            }
        }).collect();
    let impl_mapper = impl_table_mapper(struct_info);
    let impl_to_akita = build_to_akita(struct_info, generics, &fields);
    let impl_from_akita = build_from_akita(struct_info, generics, &fields);

    quote!(
        #impl_mapper

        #impl_to_akita

        #impl_from_akita

        impl #generics akita::core::GetTableName for #struct_info #generics {
            fn table_name() -> akita::core::TableName {
                akita::core::TableName{
                    name: #table_name.to_string(),
                    schema: None,
                    alias: #struct_name.to_lowercase().into(),
                }
            }
        }

        impl #generics akita::core::GetFields for #struct_info #generics {
            fn fields() -> Vec<akita::core::FieldName> {
                vec![
                    #(#from_fields)*
                ]
            }
        }

        impl #generics #struct_info #generics {

            #(#cols)*

        }

    ).into()
}

fn impl_table_mapper(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote!(
        impl akita::BaseMapper for #name {

            type Item = #name;

            fn insert<I, M: akita::AkitaMapper>(&self, entity_manager: &M) -> akita::Result<Option<I>> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue, I: akita::core::FromValue {
                entity_manager.save(self)
            }

            fn insert_batch<M: akita::AkitaMapper>(datas: &Vec<Self::Item>, entity_manager: &M) -> akita::Result<()> where Self::Item : akita::core::GetTableName + akita::core::GetFields {
                entity_manager.save_batch::<Self::Item>(datas)
            }

            fn update<M: akita::AkitaMapper>(&self, wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<u64> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.update(self, wrapper)
            }

            fn list<M: akita::AkitaMapper>(wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<Vec<Self::Item>> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.list(wrapper)
            }

            fn update_by_id<M: akita::AkitaMapper>(&self, entity_manager: &M) -> akita::Result<u64> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.update_by_id::<Self::Item>(self)
            }

            fn update_batch_by_id<M: akita::AkitaMapper>(datas: &Vec<Self::Item>, entity_manager: &M) -> akita::Result<u64> where Self::Item : akita::core::GetTableName + akita::core::GetFields {
                entity_manager.update_batch_by_id::<Self::Item>(datas)
            }

            fn delete<M: akita::AkitaMapper>(&self, wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<u64> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.remove::<Self::Item>(wrapper)
            }

            fn delete_by_id<I: akita::core::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &M, id: I) -> akita::Result<u64> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.remove_by_id::<Self::Item, I>(id)
            }

            fn page<M: akita::AkitaMapper>(page: usize, size: usize, wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<akita::IPage<Self::Item>> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.page::<Self::Item>(page, size, wrapper)
            }

            fn count<M: akita::AkitaMapper>(&mut self, wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<usize> {
                entity_manager.count::<Self::Item>(wrapper)
            }

            fn find_one<M: akita::AkitaMapper>(wrapper: akita::Wrapper, entity_manager: &M) -> akita::Result<Option<Self::Item>> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.select_one(wrapper)
            }

            /// Find Data With Table's Ident.
            fn find_by_id<I: akita::core::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &M, id: I) -> akita::Result<Option<Self::Item>> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.select_by_id(id)
            }
        }
    )
}
