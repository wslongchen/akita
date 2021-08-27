use proc_macro::{TokenStream};
use quote::quote;
use syn::{self, Attribute, Data, DeriveInput, Type};

use crate::table_derive::{get_contract_meta_item_value, has_contract_meta};

pub fn impl_from_akita(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let fields: Vec<(&syn::Ident, &Type, &Vec<Attribute>)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    let attrs = &f.attrs;
                    (ident, ty, attrs)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(FromAkita)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(FromAkita)] can only be used with structs"),
    };

    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty, attrs)| {
            let identify = has_contract_meta(attrs, "table_id");
            let field_name = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" }, "name").unwrap_or(field.to_string());
            quote!( #field: data.get(#field_name).unwrap(),)
        })
        .collect();
    quote!(
        impl akita::FromAkita for #name {
            
            fn from_data(data: &akita::AkitaData) -> Self {
                #name {
                    #(#from_fields)*
                }
            }
        }
    ).into()
}



pub fn impl_to_akita(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let generics = &derive_input.generics;
    let fields: Vec<(&syn::Ident, &Type, &Vec<Attribute>)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    let attrs = &f.attrs;
                    (ident, ty, attrs)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(ToAkita)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(ToAkita)] can only be used with structs"),
    };
    
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty, attrs)| {
            let identify = has_contract_meta(attrs, "table_id");
            let field_name = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" }, "name").unwrap_or(field.to_string());
            quote!( data.insert(#field_name, &self.#field);)
        })
        .collect();

    quote!(
        impl #generics akita::ToAkita for #name #generics {

            fn to_data(&self) -> akita::AkitaData {
                let mut data = akita::AkitaData::new();
                #(#from_fields)*
                data
            }
        }
    ).into()
}

