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
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|field| {
            let mut field_name = field.name.to_string();
            let field_info = field.field.ident.as_ref().unwrap();
            for ext in field.extra.iter() {
                match ext {
                    FieldExtra::Name(v) => {
                        field_name = v.to_string();
                    }
                    _ => {

                    }
                }
            }
            let default_value = get_field_default_value(&field.field.ty, field.field.ident.as_ref().unwrap());
            quote!( #field_info: match data.get_obj(#field_name) { Ok(v) => v, Err(_) => { #default_value } },)
        })
        .collect();

    quote!(
        impl akita::core::FromValue for #name {

            fn from_value_opt(data: &akita::core::Value) -> Result<Self, akita::core::AkitaDataError> {
                Ok(#name {
                    #(#from_fields)*
                })
            }
        }
    )
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
            let field_info = field.field.ident.as_ref().unwrap();
            for ext in field.extra.iter() {
                match ext {
                    FieldExtra::Name(v) => {
                        field_name = v.to_string();
                    }
                    _ => {

                    }
                }
            }
            // insert with alias
            quote!( data.insert_obj(#field_name, &self.#field_info );)
        })
        .collect();
    let res = quote!(
        impl #generics akita::core::ToValue for #name #generics {

            fn to_value(&self) -> akita::core::Value {
                let mut data = akita::core::Value::new_object();
                #(#to_fields)*
                data
            }
        }
    );
    res
}

