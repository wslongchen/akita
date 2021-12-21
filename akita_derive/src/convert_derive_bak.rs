use proc_macro::{TokenStream};
use quote::quote;
use syn::{self, Attribute, Data, DeriveInput, Generics, Type};

use crate::util::{get_contract_meta_item_value, get_field_default_value, has_contract_meta, get_field_attr, find_fields_type};

pub fn impl_from_akita(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let fields: Vec<(&syn::Field, &syn::Ident, &Type, &Vec<Attribute>)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    let attrs = &f.attrs;
                    (f, ident, ty, attrs)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(FromValue)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(FromValue)] can only be used with structs"),
    };

    build_from_akita(name, &fields).into()

}


pub fn build_from_akita(name: &proc_macro2::Ident, fields: &Vec<(&syn::Field, &syn::Ident, &Type, &Vec<Attribute>)>) -> proc_macro2::TokenStream {
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(f, field, ty, attrs)| {
            let identify = has_contract_meta(attrs, "table_id");
            let field_name = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" }, "name").unwrap_or(field.to_string());
            let default_value = get_field_default_value(ty, field);
            quote!( #field: match data.get_obj(#field_name) { Ok(v) => v, Err(_) => { #default_value } },)
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

pub fn build_to_akita(name: &proc_macro2::Ident, generics: &Generics, fields: &Vec<(&syn::Field, &syn::Ident, &Type, &Vec<Attribute>)>) -> proc_macro2::TokenStream {

    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(fie, field, _ty, attrs)| {
            let identify = has_contract_meta(attrs, "table_id");
            let field_name = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" }, "name").unwrap_or(field.to_string());
            let fill_value = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" },"fill").unwrap_or_default();
            let types = find_fields_type(&[fie.clone()]);
            let s = get_field_attr(fie, &types, "fill");
            let is_fill =  fill_value.is_empty();
            if is_fill {
                quote!( data.insert_obj(#field_name, &self.#field );)
            } else {
                let fn_ident: syn::Path = syn::parse_str(&fill_value).unwrap();
                quote!( data.insert_obj(#field_name, #fn_ident());)
            }
        })
        .collect();

    let res = quote!(
        impl #generics akita::core::ToValue for #name #generics {

            fn to_value(&self) -> akita::core::Value {
                let mut data = akita::core::Value::new_object();
                #(#from_fields)*
                println!(" ====={:?}", data);
                data
            }
        }
    );

    res
}

pub fn impl_to_akita(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let generics = &derive_input.generics;
    let fields: Vec<(&syn::Field, &syn::Ident, &Type, &Vec<Attribute>)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    let attrs = &f.attrs;
                    (f, ident, ty, attrs)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(ToValue)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(ToValue)] can only be used with structs"),
    };
    build_to_akita(name, generics, &fields).into()
}

