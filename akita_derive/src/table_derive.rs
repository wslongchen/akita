use proc_macro::TokenStream;
use quote::quote;
use proc_macro_error::{abort};
use syn::{DeriveInput, spanned::Spanned};
use crate::{convert_derive::{build_to_akita, build_from_akita}, comm::{ FieldExtra},util::{ find_struct_annotions, collect_field_info}};

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
    let table_name = structs.iter().find(|st| match st { FieldExtra::Table(_) => true, _ => false })
        .map(|extra| match extra { FieldExtra::Table(name) => name.clone(), _ => String::default() }).unwrap_or_default();
   if table_name.is_empty() {
       abort!(ast.span(), "Missing table name annotion: {}");
   }
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let mut name = field.name.clone();
            let mut exist = true;
            let mut select = true;
            let mut identify = false;
            let mut fill_function = String::default();
            let mut fill_mode = None;

            for extra in field.extra.iter() {
                match extra {
                    FieldExtra::Fill {ref function, ref mode, .. } => {
                        fill_function = function.clone();
                        fill_mode = mode.clone();
                    }
                    FieldExtra::Name(v) => {
                        name = v.clone();
                    }
                    FieldExtra::Select(v) => {
                        select = v.clone();
                    }
                    FieldExtra::Exist(v) => {
                        exist = v.clone();
                    }
                    FieldExtra::NumericScale(_v) => {}
                    FieldExtra::TableId(_) => {
                        identify = true;
                    }
                    _ => { }
                }
            }

            let field_type = if identify { quote!(akita::FieldType::TableId("none".to_string())) } else { quote!(akita::FieldType::TableField) };
            let fill_mode = fill_mode.unwrap_or(String::from("default")).to_lowercase();
            let fill = if fill_function.is_empty() { quote! (None) } else { let fn_ident: syn::Path = syn::parse_str(&fill_function).unwrap(); quote! (akita::core::Fill {
                        value: Some(#fn_ident().to_value()),
                        mode: #fill_mode.to_string()
                    }.into()) };

            quote!(
                akita::core::FieldName {
                    name: #name.to_string(),
                    table: #table_name.to_string().into(),
                    alias: #name.to_string().into(),
                    field_type: #field_type,
                    fill: #fill,
                    select: #select,
                    exist: #exist,
                },
            )
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
    ).into()
}

fn impl_table_mapper(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! (
        impl akita::BaseMapper for #name {

            type Item = #name;

            fn insert<I, M: akita::AkitaMapper>(&self, entity_manager: &mut M) -> Result<Option<I>, akita::AkitaError> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue, I: akita::core::FromValue {
                let data: Self::Item = self.clone();
                entity_manager.save(&data)
            }

            fn insert_batch<M: akita::AkitaMapper>(datas: &[&Self::Item], entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::core::GetTableName + akita::core::GetFields {
                entity_manager.save_batch::<Self::Item>(datas)
            }

            fn update<M: akita::AkitaMapper>(&self, wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                let data: Self::Item = self.clone();
                entity_manager.update(&data, wrapper)
            }

            fn list<M: akita::AkitaMapper>(wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<Vec<Self::Item>, akita::AkitaError> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.list(wrapper)
            }

            fn update_by_id<M: akita::AkitaMapper>(&self, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                let data: Self::Item = self.clone();
                entity_manager.update_by_id::<Self::Item>(&data)
            }

            fn delete<M: akita::AkitaMapper>(&self, wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.remove::<Self::Item>(wrapper)
            }

            fn delete_by_id<I: akita::core::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<(), akita::AkitaError> where Self::Item : akita::core::GetFields + akita::core::GetTableName + akita::core::ToValue {
                entity_manager.remove_by_id::<Self::Item, I>(id)
            }

            fn page<M: akita::AkitaMapper>(page: usize, size: usize, wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<akita::IPage<Self::Item>, akita::AkitaError> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.page::<Self::Item>(page, size, wrapper)
            }

            fn count<M: akita::AkitaMapper>(&mut self, wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<usize, akita::AkitaError> {
                entity_manager.count::<Self::Item>(wrapper)
            }

            fn find_one<M: akita::AkitaMapper>(wrapper: akita::Wrapper, entity_manager: &mut M) -> Result<Option<Self::Item>, akita::AkitaError> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.select_one(wrapper)
            }

            /// Find Data With Table's Ident.
            fn find_by_id<I: akita::core::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<Option<Self::Item>, akita::AkitaError> where Self::Item : akita::core::GetTableName + akita::core::GetFields + akita::core::FromValue {
                entity_manager.select_by_id(id)
            }
        }
    )
}
