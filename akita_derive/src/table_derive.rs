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
use proc_macro2::{Span};
use quote::quote;
use syn::{DeriveInput, LitStr};
use crate::{convert_derive::{build_to_akita, build_from_akita}, comm::{FieldExtra}, util::{collect_field_info, to_snake_name}};
use crate::comm::crate_ident;
use crate::util::find_struct_annotations;

pub fn impl_get_table(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let res = parse_table(&derive_input);
    res.into()
}

fn parse_table(ast: &syn::DeriveInput) -> TokenStream {
    // extra annotation info
    // Struct specific definitions
    let crate_ident = crate_ident();
    let generics = &ast.generics;
    let fields = collect_field_info(ast);
    let struct_info = &ast.ident;
    let struct_name = &ast.ident.to_string();
    let structs = find_struct_annotations(&ast.attrs);
    let mut table_name = structs.iter().find(|st| match st { FieldExtra::Table(_) => true, _ => false })
    .map(|extra| match extra {
        FieldExtra::Table(name) => name.clone(),
        _ => String::default()
    }).unwrap_or_default();

    let schema_expr = if let Some(schema) = structs.iter().find_map(|st| {
        match st {
            FieldExtra::Schema(s) => Some(s),
            _ => None
        }
    }) {
        let schema_lit = LitStr::new(schema, Span::call_site());
        quote! { Some(#schema_lit.to_string()) }
    } else {
        quote! { None }
    };

    if table_name.is_empty() {
        table_name = to_snake_name(struct_name);
    }

    let primary_key_field = fields.iter().find(|field| {
        field.extra.iter().any(|extra| matches!(extra, FieldExtra::TableId))
    });

    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let name = field.name.clone();
            let mut alias = field.name.clone();
            let mut exist = true;
            let mut select = true;
            let mut identify = false;
            let mut identifier_type = quote!( #crate_ident::prelude::IdentifierType::None);
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
                        identifier_type = match ty.to_lowercase().as_str() {
                            "auto" => quote!(#crate_ident::prelude::IdentifierType::Auto),
                            "input" => quote!(#crate_ident::prelude::IdentifierType::Input),
                            "assign_id" => quote!(#crate_ident::prelude::IdentifierType::AssignId),
                            "assign_uuid" => quote!(#crate_ident::prelude::IdentifierType::AssignUuid),
                            _ => quote!(#crate_ident::prelude::IdentifierType::None)
                        }

                    }
                    _ => {}
                }
            }

            let field_type = if identify { quote!(#crate_ident::prelude::FieldType::TableId(#identifier_type)) } else { quote!(#crate_ident::prelude::FieldType::TableField) };
            let fill_mode = fill_mode.unwrap_or(String::from("default")).to_lowercase();
            let fill = if fill_function.is_empty() { quote!(None) } else {
                let fn_ident: syn::Path = syn::parse_str(&fill_function).unwrap();
                quote!(#crate_ident::prelude::Fill {
                        value: Some(#fn_ident().into_value()),
                        mode: #fill_mode.to_string()
                    }.into())
            };

            quote!(
                #crate_ident::prelude::FieldName {
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
                )
            } else {
                quote!(
                )
            }
        }).collect();

    // Generate primary key related methods
    let primary_key_methods = if let Some(pk_field) = primary_key_field {
        let _pk_field_ident = pk_field.field.ident.as_ref().unwrap();
        let pk_field_name = &pk_field.name;

        quote! {
            pub fn primary_key_field() -> String {
                #pk_field_name.to_string()
            }


        }
    } else {
        quote! {
            pub fn primary_key_field() -> String {
                "".to_string()
            }
        }
    };

    let impl_mapper = impl_table_mapper(struct_info);
    let impl_to_akita = build_to_akita(struct_info, generics, &fields);
    let impl_from_akita = build_from_akita(struct_info, generics, &fields);

    quote!(
        #impl_mapper

        #impl_to_akita

        #impl_from_akita

        impl #generics #crate_ident::prelude::GetTableName for #struct_info #generics {
            fn table_name() -> #crate_ident::prelude::TableName {
                #crate_ident::prelude::TableName{
                    name: #table_name.to_string(),
                    schema: #schema_expr,
                    alias: #struct_name.to_lowercase().into(),
                    ignore_interceptors: std::collections::HashSet::new(),
                }
            }
        }

        impl #generics #crate_ident::prelude::GetTableName for &#struct_info #generics {
            fn table_name() -> #crate_ident::prelude::TableName {
                #crate_ident::prelude::TableName{
                    name: #table_name.to_string(),
                    schema: #schema_expr,
                    alias: #struct_name.to_lowercase().into(),
                    ignore_interceptors: std::collections::HashSet::new(),
                }
            }
        }

        impl #generics #crate_ident::prelude::GetFields for #struct_info #generics {
            fn fields() -> Vec<#crate_ident::prelude::FieldName> {
                vec![
                    #(#from_fields)*
                ]
            }
        }

        impl #generics #crate_ident::prelude::GetFields for &#struct_info #generics {
            fn fields() -> Vec<#crate_ident::prelude::FieldName> {
                vec![
                    #(#from_fields)*
                ]
            }
        }

        impl #generics #struct_info #generics {

            #(#cols)*

            #primary_key_methods
        }

    ).into()
}

fn impl_table_mapper(name: &syn::Ident) -> proc_macro2::TokenStream {
    let crate_ident = crate_ident();
    quote!(
        
        #[cfg(all(
            any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "oracle-sync",
                feature = "mssql-sync"
            ),
            not(any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "mssql-async",
                feature = "oracle-async"
            ))
        ))]
        impl #name {

            /// Query individual entities
            pub fn select_one<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<Option<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.select_one(wrapper)
            }

            /// Find entities based on ID
            pub fn select_by_id<M, I>(mapper: &M, id: I) -> std::result::Result<Option<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.select_by_id(id)
            }

            /// Pagination query
            pub fn page<M>(mapper: &M, page: u64, size: u64, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<#crate_ident::prelude::IPage<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.page(page, size, wrapper)
            }

            /// Count
            pub fn count<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.count::<Self>(wrapper)
            }

            /// Delete the current entity (according to ID)
            pub fn remove_by_id<M, I>(&self, mapper: &M, id: I) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.remove_by_id::<Self, I>(id)
            }

            /// Removed according to conditions
            pub fn remove<M>(&self, mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.remove::<Self>(wrapper)
            }

            /// Bulk deletion (based on ID list)
            pub fn remove_by_ids<M, I>(mapper: &M, ids: Vec<I>) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.remove_by_ids::<Self, I>(ids)
            }

            /// Update the current entity (based on ID)
            pub fn update_by_id<M>(&self, mapper: &M) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.update_by_id(self)
            }

            /// Save or update the current entity
            pub fn save_or_update<M, I>(&self, mapper: &M) -> std::result::Result<Option<I>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                I: #crate_ident::prelude::FromAkitaValue + Sync + Send,
            {
                mapper.save_or_update(self)
            }

            /// Batch update (based on ID)
            pub fn update_batch_by_id<M>(mapper: &M, entities: &Vec<Self>) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.update_batch_by_id(entities)
            }

            /// Updated according to conditions
            pub fn update<M>(&self, mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.update(self, wrapper)
            }

            /// Save the current entity (insert)
            pub fn save<M, I>(&self, mapper: &M) -> std::result::Result<Option<I>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                I: #crate_ident::prelude::FromAkitaValue + Sync + Send,
            {
                mapper.save(self)
            }

            /// Bulk insertion
            pub fn save_batch<M, E>(mapper: &M, entities: E) -> std::result::Result<(), #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
                E: IntoIterator<Item = Self>
            {
                mapper.save_batch(entities)
            }


            /// Query all records
            pub fn list<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<Vec<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AkitaMapper,
            {
                mapper.list(wrapper)
            }

            /// Create a query wrapper
            pub fn query() -> #crate_ident::prelude::Wrapper {
                #crate_ident::prelude::Wrapper::new()
            }
        }
        
        #[cfg(all(
            any(
                feature = "mysql-async",
                feature = "postgres-async",
                feature = "sqlite-async",
                feature = "oracle-async",
                feature = "mssql-async"
            ),
            not(any(
                feature = "mysql-sync",
                feature = "postgres-sync",
                feature = "sqlite-sync",
                feature = "mssql-sync",
                feature = "oracle-sync"
            ))
        ))]
        impl #name {

            /// Query individual entities
            pub async fn select_one<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<Option<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.select_one(wrapper).await
            }

            /// Find entities based on ID
            pub async fn select_by_id<M, I>(mapper: &M, id: I) -> std::result::Result<Option<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.select_by_id(id).await
            }

            /// Pagination query
            pub async fn page<M>(mapper: &M, page: u64, size: u64, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<#crate_ident::prelude::IPage<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.page(page, size, wrapper).await
            }

            /// Count
            pub async fn count<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.count::<Self>(wrapper).await
            }

            /// Delete the current entity (according to ID)
            pub async fn remove_by_id<M, I>(&self, mapper: &M, id: I) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.remove_by_id::<Self, I>(id).await
            }

            /// Removed according to conditions
            pub async fn remove<M>(&self, mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.remove::<Self>(wrapper).await
            }

            /// Bulk deletion (based on ID list)
            pub async fn remove_by_ids<M, I>(mapper: &M, ids: Vec<I>) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                I: #crate_ident::prelude::IntoAkitaValue + Sync + Send,
            {
                mapper.remove_by_ids::<Self, I>(ids).await
            }

            /// Update the current entity (based on ID)
            pub async fn update_by_id<M>(&self, mapper: &M) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.update_by_id(self).await
            }

            /// Save or update the current entity
            pub async fn save_or_update<M, I>(&self, mapper: &M) -> std::result::Result<Option<I>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                I: #crate_ident::prelude::FromAkitaValue + Sync + Send,
            {
                mapper.save_or_update(self).await
            }

            /// Batch update (based on ID)
            pub async fn update_batch_by_id<M>(mapper: &M, entities: &Vec<Self>) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.update_batch_by_id(entities).await
            }

            /// Updated according to conditions
            pub async fn update<M>(&self, mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<u64, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.update(self, wrapper).await
            }

            /// Save the current entity (insert)
            pub async fn save<M, I>(&self, mapper: &M) -> std::result::Result<Option<I>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                I: #crate_ident::prelude::FromAkitaValue + Sync + Send,
            {
                mapper.save(self).await
            }

            /// Bulk insertion
            pub async fn save_batch<M, E>(mapper: &M, entities: E) -> std::result::Result<(), #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
                E: IntoIterator<Item = Self> + Sync + Send
            {
                mapper.save_batch(entities).await
            }


            /// Query all records
            pub async fn list<M>(mapper: &M, wrapper: #crate_ident::prelude::Wrapper) -> std::result::Result<Vec<Self>, #crate_ident::prelude::AkitaError>
            where
                M: #crate_ident::prelude::AsyncAkitaMapper,
            {
                mapper.list(wrapper).await
            }

            /// Create a query wrapper
            pub fn query() -> #crate_ident::prelude::Wrapper {
                #crate_ident::prelude::Wrapper::new()
            }
        }
    )
}
