//! 
//! Generate Database Methods.
//! 
use proc_macro::TokenStream;


#[macro_use]
mod table_derive;
#[macro_use]
mod convert_derive;


// #[proc_macro_derive(Table, attributes(column, table, id, exist))]
// pub fn table(input: TokenStream) -> TokenStream {
//     table_derive::impl_to_table(input)
// }


#[proc_macro_derive(FromAkita)]
pub fn from_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_from_akita(input)
}

#[proc_macro_derive(ToAkita)]
pub fn to_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_to_akita(input)
}

#[proc_macro_derive(Table, attributes(field, table))]
pub fn to_table(input: TokenStream) -> TokenStream {
    table_derive::impl_get_table(input)
}

#[proc_macro_derive(GetTableName)]
pub fn to_table_name(input: TokenStream) -> TokenStream {
    table_derive::impl_get_table_name(input)
}

#[proc_macro_derive(GetColumnNames)]
pub fn to_column_names(input: TokenStream) -> TokenStream {
    table_derive::impl_get_column_names(input)
}