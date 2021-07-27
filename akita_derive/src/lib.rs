//! 
//! Generate Database Methods.
//! 
use proc_macro::TokenStream;


#[macro_use]
mod table_derive;
#[macro_use]
mod convert_derive;


#[proc_macro_derive(Table, attributes(column, table, id, exist))]
pub fn table(input: TokenStream) -> TokenStream {
    table_derive::impl_to_table(input)
}


#[proc_macro_derive(FromAkita)]
pub fn from_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_from_akita(input)
}

#[proc_macro_derive(ToAkita)]
pub fn to_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_to_akita(input)
}