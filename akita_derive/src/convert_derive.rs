use proc_macro::{Ident, TokenStream};
use quote::quote;
use syn::{self, Data, DeriveInput, Field, Fields, Type};

pub fn impl_from_akita(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let fields: Vec<(&syn::Ident, &Type)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    (ident, ty)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(FromAkita)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(FromAkita)] can only be used with structs"),
    };

    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty)| {
            quote!( #field: data.get(stringify!(#field)).unwrap(),)
        })
        .collect();
    quote!(
        impl FromAkita for #name {
            
            fn from_data(data: &AkitaData) -> Self {
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
    let fields: Vec<(&syn::Ident, &Type)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    (ident, ty)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(ToAkita)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(ToAkita)] can only be used with structs"),
    };
    
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty)| {
            quote!( data.insert(stringify!(#field), &self.#field);)
        })
        .collect();

    quote!(
        impl #generics ToAkita for #name #generics {

            fn to_data(&self) -> AkitaData {
                let mut data = AkitaData::new();
                #(#from_fields)*
                data
            }
        }
    ).into()
}

