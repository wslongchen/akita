//! 
//! Generate Database Methods.
//! 
use proc_macro::TokenStream;


#[macro_use]
mod table_derive;
#[macro_use]
mod convert_derive;


// #[proc_macro_derive(AkitaTable, attributes(column, table, id, exist))]
// pub fn table(input: TokenStream) -> TokenStream {
//     table_derive::impl_to_table(input)
// }

/// Generate table info data
#[proc_macro_derive(FromValue)]
pub fn from_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_from_akita(input)
}

/// Format table info data
#[proc_macro_derive(ToValue)]
pub fn to_akita(input: TokenStream) -> TokenStream {
    convert_derive::impl_to_akita(input)
}

/// Generate table info
/// ```rust
/// /// Annotion Support: Table、table_id、field (name, exist)
/// #[derive(Debug, FromValue, ToValue, AkitaTable, Clone)]
/// #[table(name="t_system_user")]
/// struct SystemUser {
///     #[field = "name"]
///     id: Option<i32>,
///     #[table_id]
///     username: String,
///     #[field(name="ages", exist = "false")]
///     age: i32,
/// }
/// ```
/// 
#[proc_macro_derive(AkitaTable, attributes(field, table, table_id))]
pub fn to_table(input: TokenStream) -> TokenStream {
    table_derive::impl_get_table(input)
}

/// Generate table name
#[proc_macro_derive(GetTableName)]
pub fn to_table_name(input: TokenStream) -> TokenStream {
    table_derive::impl_get_table_name(input)
}

/// Generate table fields
#[proc_macro_derive(GetFields)]
pub fn to_column_names(input: TokenStream) -> TokenStream {
    table_derive::impl_get_column_names(input)
}