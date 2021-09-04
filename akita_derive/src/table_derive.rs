use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use std::{collections::HashMap, iter::FromIterator};
use syn::{Attribute, Data, DeriveInput, Fields, Ident, Type};

#[derive(Debug)]
struct FieldAttribute {
    pub name: &'static str,
    pub value: String
}

#[allow(unused)]
pub fn impl_to_table(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let mut table_name = "".to_string();
    if let Data::Struct(r#struct) = derive_input.data {
        let fields = r#struct.fields;
        if matches!(&fields, Fields::Named(_)) {
            let mut fields_info: HashMap<&Ident, (&Type, String, bool, bool)> = HashMap::new();
            let mut field_ids: Vec<(&Ident, &Type, String)> = Vec::new();
            for field in fields.iter() {
                let name = field.ident.as_ref().unwrap();
                let identify = has_contract_meta(&field.attrs, "id");
                let name_value = get_contract_meta_item_value(&field.attrs, if identify { "id" } else { "column" }, "name");
                let exist_value = get_contract_meta_item_value(&field.attrs, "column", "exist");
                let exist_value = exist_value.unwrap_or_default().ne("false");
                let value = name_value.to_owned().unwrap_or(name.to_string());
                // filter the unsuport type.
                if !valid_type(&field.ty) {
                    continue;
                }
                fields_info.insert(name, (&field.ty, value.to_owned(), identify, exist_value));
                if identify {
                    field_ids.push((name, &field.ty, value));
                }
            }
            let mut field_idents: Vec<String> = Vec::new();
            for ident in fields_info.keys() {
                if let Some((_ty, name, _identify, exist)) = fields_info.get(ident) {
                    if !exist {
                        continue;
                    }
                    field_idents.push(name.to_owned());
                } else {
                    field_idents.push(ident.to_string());
                }
            }
            let builder_set_fields = map_fields(&fields, |(ident, ty, _)| {
                if let Some((_, _, _, exist)) = fields_info.get(ident) { get_type_default_value(ty, ident, *exist) } else { get_type_default_value(ty, ident, true) }
            });
            let build_fields = field_idents.join(",");
            if let Some(table) = get_contract_meta_item_value(&derive_input.attrs, "table", "name") {
                table_name = table;
            }
            let update_id_fields = TokenStream2::from_iter(field_ids.iter().map(|(id, ty, name)| {
                let mut ft = String::from("");
                if let Type::Path(r#path) = ty {
                    ft = r#path.path.segments[0].ident.to_string();
                }
                if ft.eq("Option") {
                    quote!(
                        if let Some(value) = &self.#id {
                            id_fields.push(format!("{} = '{}'", #name, value));
                        }
                    )
                } else {
                    quote!(
                        id_fields.push(format!("{} = '{}'", #name, &self.#id));
                    )
                }
            }));

            let page_id_fields = TokenStream2::from_iter(field_ids.iter().map(|(_id, _ty, name)| {
                quote!(
                    id_fields.push(format!("a.{} = b.{}", #name, #name));
                )
            }));

            let build_values = map_fields(&fields, |(ident, ty, _)| {
                if !valid_type(ty) { return quote!() }
                if let Some((_, _, _, exist)) = fields_info.get(ident) { if !exist { return quote!() } } 
                get_type_value(ty, ident)
            });

            let update_fields = map_fields(&fields, |(ident, ty, attrs)| {
                if !valid_type(ty) { return quote!() }
                if let Some((_, _, _, exist)) = fields_info.get(ident) { if !exist { return quote!() } }
                let name = ident.to_string();
                let name_value = get_contract_meta_item_value(attrs, "column", "name");
                let name = name_value.to_owned().unwrap_or(name.to_string());
                get_type_set_value(ty, ident, &name)
            });
            let build_fields_format = field_idents.iter().map(|_| "'{}'".to_string()).collect::<Vec<_>>().join(",");
            let _build_values_field = map_fields(&fields, |(ident, _ty, _)|  quote!(#ident,));
            let sql_format = format!("insert into {{}}({{}}) values({})", build_fields_format);
            let result = quote!(
                
                impl BaseMapper for #name {

                    type Item = #name;

                    // fn insert<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<u64>, AkitaError> {
                    //     let sql = format!(#sql_format, #table_name, #build_fields,#build_values);
                    //     let last_insert_id = match conn {
                    //         ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.last_insert_id().into()},
                    //         ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.last_insert_id()},
                    //         ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.last_insert_id().into()},
                    //         ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.last_insert_id().into()},
                    //         ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.last_insert_id().into()},
                    //     };
                    //     Ok(last_insert_id)
                    // }

                    // fn list<'a, 'b, 'c, 'p, W: Wrapper>(wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Vec<Self::Item>, AkitaError> where Self::Item: Clone {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let table_fields = Self::Item::get_table_fields()?;
                    //     let select_fields = wrapper.get_select_sql();
                    //     let fields = if select_fields.eq("*") {
                    //         table_fields
                    //     } else { 
                    //         select_fields
                    //     };
                    //     let sql = format!("select {} from {} where {}", &fields, &table_name, wrapper.get_sql_segment());
                    //     let datas = match conn {
                    //         ConnMut::Mut(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                    //         ConnMut::TxMut(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                    //         ConnMut::Owned(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                    //         ConnMut::Pooled(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                    //         ConnMut::R2d2Polled(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                    //     };
                    //     Ok(datas)
                    // }

                    // fn page<'a, 'b, 'c, 'p, W: Wrapper>(page: usize, size: usize, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<IPage<Self::Item>, AkitaError>{
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let table_fields = Self::Item::get_table_fields()?;
                    //     let select_fields = wrapper.get_select_sql();
                    //     let fields = if select_fields.eq("*") {
                    //         table_fields
                    //     } else {
                    //         select_fields
                    //     };
                        
                    //     let count_sql = format!("select count(1) from {} where {}", &table_name, wrapper.get_sql_segment());
                    //     let count = 0usize;
                    //     let mut page = IPage::new(page, size ,count, vec![]);

                    //     let mut id_fields: Vec<String> = Vec::new();
                    //     #page_id_fields
                    //     let sql = if id_fields.is_empty() {
                    //         format!("select {} from {} where {} limit {}, {}", &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size)
                    //     } else {
                    //         format!("select {} from {} a join (select {} from {} where {} limit {}, {}) b on {}", &fields, &table_name, &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size, id_fields.join(" and "))
                    //     };

                    //     // let sql = format!("select {} from {} a join (select {} from {} where {} limit {}, {}) b on a.id = b.id", &fields, &table_name, &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size);
                    //     match conn {
                    //         ConnMut::Mut(ref mut conn) => {
                    //             let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                    //             if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                    //         },
                    //         ConnMut::TxMut(ref mut conn) => {
                    //             let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                    //             if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                    //         },
                    //         ConnMut::Owned(ref mut conn) => {
                    //             let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                    //             if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                    //         },
                    //         ConnMut::Pooled(ref mut conn) => {
                    //             let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                    //             if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                    //         },
                    //         ConnMut::R2d2Polled(ref mut conn) => {
                    //             let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                    //             if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                    //         },
                    //     }
                    //     Ok(page)
                    // }

                    // fn find_one<'a, 'b, 'c, 'p, W: Wrapper>(wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<Self::Item>, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let table_fields = Self::Item::get_table_fields()?;
                    //     let select_fields = wrapper.get_select_sql();
                    //     let fields = if select_fields.eq("*") {
                    //         table_fields
                    //     } else {
                    //         select_fields
                    //     };
                    //     let sql = format!("select {} from {} where {} limit 1", &fields, &table_name, wrapper.get_sql_segment());
                    //     let data = match conn {
                    //         ConnMut::Mut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::TxMut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::Owned(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::Pooled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::R2d2Polled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //     };
                    //     Ok(data)
                    // }
  
                    // fn find_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<Option<Self::Item>, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let table_fields = Self::Item::get_table_fields()?;
                    //     let id_fields = self.get_table_idents()?;
                    //     let sql = format!("select {} from {} where {} limit 1", &table_fields, &table_name, &id_fields);
                    //     let data = match conn {
                    //         ConnMut::Mut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::TxMut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::Owned(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::Pooled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //         ConnMut::R2d2Polled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                    //     };
                    //     Ok(data)
                    // }

                    // fn update<'a, 'b, 'c, 'p>(&self, wrapper: &mut UpdateWrapper, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let update_fields = self.get_update_fields(wrapper.get_set_sql())?;
                    //     let sql = format!("update {} set {} where {}", &table_name, &update_fields, wrapper.get_sql_segment());
                    //     let affected_rows = match conn {
                    //         ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //     };
                    //     Ok(affected_rows > 0)
                    // }
                
                    // fn update_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let update_fields = self.get_update_fields(None)?;
                    //     let id_fields = self.get_table_idents()?;
                    //     let sql = format!("update {} set {} where {}", &table_name, &update_fields, &id_fields);
                    //     let affected_rows = match conn {
                    //         ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //     };
                    //     Ok(affected_rows > 0)
                    // }

                    // fn delete<'a, 'b, 'c, 'p, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let sql = format!("delete from {} where {}", &table_name, wrapper.get_sql_segment());
                    //     let affected_rows = match conn {
                    //         ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //     };
                    //     Ok(affected_rows > 0)
                    // }
                
                    // fn delete_by_id<'a, 'b, 'c, 'p>(&self, conn: &mut ConnMut<'a, 'b, 'c, 'p>) -> Result<bool, AkitaError> {
                    //     let table_name = Self::Item::get_table_name()?;
                    //     let id_fields = self.get_table_idents()?;
                    //     let sql = format!("delete from {} where {}", &table_name, &id_fields);
                    //     let affected_rows = match conn {
                    //         ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //         ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop(&sql, ())?; conn.affected_rows()},
                    //     };
                    //     Ok(affected_rows > 0)
                    // }

                    fn get_table_fields() -> Result<String, AkitaError> {
                        let fields = #build_fields.to_string();
                        if fields.is_empty() {
                            return Err(AkitaError::MissingField("Find Error, Missing Table Fields !".to_string()))
                        }
                        Ok(fields)
                    }

                    fn get_table_idents(&self) -> Result<String, AkitaError> {
                        let mut id_fields: Vec<String>= Vec::new();
                        #update_id_fields
                        if id_fields.is_empty() {
                            return Err(AkitaError::MissingIdent("Missing Id Fields !".to_string()))
                        }
                        Ok(id_fields.join(" and "))
                    }

                    fn get_update_fields(&self, set_sql: Option<String>) -> Result<String, AkitaError> {
                        let mut update_fields: Vec<String> = Vec::new();
                        #update_fields
                        if update_fields.is_empty() && set_sql.is_none() {
                            return Err(AkitaError::MissingField("Missing Update Fields !".to_string()))
                        }
                        if let Some(set) = set_sql {
                            Ok(set)
                        } else {
                            Ok(update_fields.join(","))
                        }
                        
                    }
                
                    fn get_table_name() -> Result<String, AkitaError> {
                        if #table_name.is_empty() {
                            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
                        }
                        Ok(#table_name.to_string())
                    }
                }

                
                
            )
            .into();
            // eprintln!("{}", result);
            return result;
        }
    }
    
    // struct attributes
    
    // if table_name.is_empty() {
    //     panic!("table_name must set.");
    // }
    quote!()
        .into()
}




/// Filter contract attribute like #[table(foo = bar)]
fn _get_contract_meta_items(attr: &syn::Attribute, filter: &str) -> Option<Vec<syn::NestedMeta>> {
    if attr.path.segments.len() == 1 && attr.path.segments[0].ident == filter {
        match attr.parse_meta() {
            Ok(syn::Meta::List(ref meta)) => Some(meta.nested.iter().cloned().collect()),
            _ => {
                // TODO: produce an error
                None
            }
        }
    } else {
        None
    }
}

/// extra the fields info..
#[allow(unused)]
fn map_fields<F>(fields: &Fields, mapper: F) -> TokenStream2
where
    F: FnMut((&Ident, &Type, &Vec<Attribute>)) -> TokenStream2,
{
    TokenStream2::from_iter(
        fields
            .iter()
            .map(|field| (field.ident.as_ref().unwrap(), &field.ty, &field.attrs))
            .map(mapper),
    )
}

/// get the field orignal type
#[allow(unused)]
pub fn get_field_type(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(r#path) => {
            let p = &r#path.path.segments[0];
            if p.ident == "Option" {
                match &p.arguments {
                    syn::PathArguments::AngleBracketed(path_arg) => {
                        let mut fy = String::default();
                        let _ = path_arg.args.iter().map(|arg| {
                            match arg {
                                syn::GenericArgument::Type(arg_type) => {
                                    match arg_type {
                                        Type::Path(arg_path) => {
                                            if let Some(arg_path_res) = arg_path.path.get_ident() {
                                                fy = arg_path_res.to_string();
                                            }
                                        },
                                        _ => {}
                                    }
                                },
                                _ => {},
                            }
                        }).collect::<Vec<_>>();
                        fy.into()
                    },
                    _ => {
                        None
                    },
                }
            } else {
                p.ident.to_string().into()
            }
        }
        _ => {
           None
        }
    }
}

/// Filter contract attribute like #[table(foo = bar)]
pub fn get_contract_meta_item_value(attrs: &Vec<syn::Attribute>, filter: &str, key:&str) -> Option<String> {
    let res = attrs.iter().filter_map(|attr| {
        if attr.path.segments.len() == 1 && attr.path.segments[0].ident == filter {
            match attr.parse_meta() {
                Ok(syn::Meta::List(ref meta)) => {
                    let mut res = None;
                    for meta_item in meta.nested.iter() {
                        
                        match meta_item {
                            syn::NestedMeta::Meta(syn::Meta::NameValue(ref m)) if m.path.is_ident(key) => {
                                if let syn::Lit::Str(ref lit) = m.lit {
                                    res = lit.value().into()
                                } else {}
                            }
                            syn::NestedMeta::Lit(syn::Lit::Str(ref lit)) => {
                                res = lit.value().into()
                            },
                            _ => {}
                        }
                    }
                    res
                },
                Ok(syn::Meta::NameValue(ref meta)) => {
                    let mut res = None;
                    if let syn::Lit::Str(ref lit) = meta.lit {
                        res = lit.value().into()
                    } 
                    res
                },
                _ => {
                    None
                }
            }
        } else {
            None
        }
    }).collect::<Vec<_>>();
    if res.len() > 0 { res[0].to_owned().into() } else { None }
}

#[allow(unused)]
pub fn has_contract_meta(attrs: &Vec<syn::Attribute>, filter: &str) -> bool {
    attrs.iter().find(|attr| attr.path.segments.len() == 1 && attr.path.segments[0].ident == filter).is_some()
}

#[allow(unused)]
pub fn get_type_default_value(ty: &Type, ident: &Ident, exist: bool) -> TokenStream2 {
    let ident_name = ident.to_string();
    let ori_ty = get_field_type(ty).unwrap_or_default();
    let mut ft = String::default();
    if let Type::Path(r#path) = ty {
        ft = r#path.path.segments[0].ident.to_string();
    }
    if ft.eq("Option") {
        if !exist { quote!(value.#ident = None;) } else { 
            match ori_ty.as_str() {
                "bool" => {
                    quote!(
                        let #ident: Option<u8>= row.get(#ident_name).unwrap_or(None);
                        value.#ident = Some(#ident.unwrap_or(0) == 1);
                    )
                }
                "Vec" => quote!(value.#ident = vec![];),
                "f64" | "f32" | "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" 
                | "usize" | "isize" | "str" | "String" | "NaiveDate" | "NaiveDateTime" => quote!(value.#ident = row.get(#ident_name).unwrap_or(None);),
                _ => quote!()
            }
            
        }
    } else {
        match ori_ty.as_str() {
            "f64" | "f32" => if !exist { quote!(value.#ident = 0.0;) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or(0.0).to_owned();
            )},
            "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" | "usize" | "isize" => if !exist { quote!(value.#ident = 0;) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or(0).to_owned();
            )},
            "bool" => if !exist { quote!(value.#ident = false;) } else { quote!(
                let #ident: Option<u8>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or(0) == 1;
            )},
            "str" => if !exist { quote!(value.#ident = "";) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or("").to_owned();
            )},
            "String" => if !exist { quote!(value.#ident = String::default();) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or("".to_string()).to_owned();
            )},
            "NaiveDate"  => if !exist { quote!(value.#ident = Local::now().naive_local().date();) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or(Local::now().naive_local().date());
            )},
            "NaiveDateTime" => if !exist { quote!(value.#ident = Local::now().naive_local();) } else { quote!(
                let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                value.#ident = #ident.unwrap_or(Local::now().naive_local()).to_owned();
            )},
            "Vec" => quote!(value.#ident = vec![];),
            _ => quote!(
            )
        }
    }
}

#[allow(unused)]
pub fn get_field_value(ty: &Type, ident: &Ident) -> TokenStream2 {
    let ori_ty = get_field_type(ty).unwrap_or_default();
    let mut ft = String::default();
    if let Type::Path(r#path) = ty {
        ft = r#path.path.segments[0].ident.to_string();
    }
    // quote!( data.insert(stringify!(#field), &self.#field);)
    if ft.eq("Option") {
        match ori_ty.as_str() {
            "f64" | "f32" => quote!( data.insert(stringify!(#ident), &self.#ident.to_owned().unwrap_or(0.0));),
            "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" | "usize" | "isize" => quote!(data.insert(stringify!(#ident), &self.#ident.to_owned().unwrap_or(0));),
            "bool" => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(false));),
            "str" => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(""));),
            "Vec" => quote!(data.insert(stringify!(#ident),&&self.#ident.to_owned().unwrap_or(vec![]));),
            "String" => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or("".to_string()));),
            // "NaiveDate"  => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(Local::now().naive_local().date()).format("%Y-%m-%d").to_string());),
            // "NaiveDateTime" => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(Local::now().naive_local()).format("%Y-%m-%d %H:%M:%S").to_string());),
            "NaiveDate"  => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(Local::now().naive_local().date()).format("%Y-%m-%d").to_string());),
            "NaiveDateTime" => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or(Local::now().naive_local()).format("%Y-%m-%d %H:%M:%S").to_string());),
            _ => quote!(data.insert(stringify!(#ident),&self.#ident.to_owned().unwrap_or_default());),
        }
    } else {
        match ori_ty.as_str() {
            "NaiveDate"  => quote!(data.insert(stringify!(#ident),&self.#ident.format("%Y-%m-%d").to_string());),
            "NaiveDateTime" => quote!(data.insert(stringify!(#ident),&self.#ident.format("%Y-%m-%d %H:%M:%S").to_string());),
            "bool" => quote!(data.insert(stringify!(#ident),&self.#ident);),
            _ => quote!(data.insert(stringify!(#ident),&self.#ident);),
        }
    }
}

#[allow(unused)]
pub fn get_field_default_value(ty: &Type, ident: &Ident) -> TokenStream2 {
    let ident_name = ident.to_string();
    let ori_ty = get_field_type(ty).unwrap_or_default();
    let mut ft = String::default();
    if let Type::Path(r#path) = ty {
        ft = r#path.path.segments[0].ident.to_string();
    }
    if ft.eq("Option") {
        quote!(None)
    } else {
        match ori_ty.as_str() {
            "f64" | "f32" => quote!(0.0),
            "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" | "usize" | "isize" => quote!(0),
            "bool" => quote!(false),
            "str" => quote!(""),
            "String" => quote!(String::default()),
            "NaiveDate"  => quote!(chrono::Local::now().naive_local().date()),
            "NaiveDateTime" => quote!(chrono::Local::now().naive_local()),
            "Vec" => quote!(Vec::new()),
            _ => quote!(None)
        }
    }
}

#[allow(unused)]
pub fn valid_type(ty: &Type) -> bool {
    let ori_ty = get_field_type(ty).unwrap_or_default();
    match ori_ty.as_str() {
        "f64" | "f32" | "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" 
        | "usize" | "isize" | "bool" | "str" | "String" | "NaiveDate" | "NaiveDateTime" => true,
        _ => false
    }
}

fn get_type_value(ty: &Type, ident: &Ident) -> TokenStream2 {
    let ori_ty = get_field_type(ty).unwrap_or_default();
    let mut ft = String::default();
    if let Type::Path(r#path) = ty {
        ft = r#path.path.segments[0].ident.to_string();
    }
    if ft.eq("Option") {
        match ori_ty.as_str() {
            "f64" | "f32" => quote!(&self.#ident.to_owned().unwrap_or(0.0),),
            "u8" | "u128" | "u16" | "u64" | "u32" | "i8" | "i16" | "i32" | "i64" | "i128" | "usize" | "isize" => quote!(&self.#ident.to_owned().unwrap_or(0),),
            "bool" => quote!(if self.#ident.to_owned().unwrap_or(false) { 1 } else { 0 },),
            "str" => quote!(&self.#ident.to_owned().unwrap_or(""),),
            "Vec" => quote!(&self.#ident.to_owned().unwrap_or(vec![]),),
            "String" => quote!(&self.#ident.to_owned().unwrap_or("".to_string()),),
            "NaiveDate"  => quote!(&self.#ident.to_owned().unwrap_or(Local::now().naive_local().date()).format("%Y-%m-%d").to_string(),),
            "NaiveDateTime" => quote!(&self.#ident.to_owned().unwrap_or(Local::now().naive_local()).format("%Y-%m-%d %H:%M:%S").to_string(),),
            _ => quote!(&self.#ident.to_owned().unwrap_or_default(),)
        }
    } else {
        match ori_ty.as_str() {
            "NaiveDate"  => quote!(&self.#ident.format("%Y-%m-%d").to_string(),),
            "NaiveDateTime" => quote!(&self.#ident.format("%Y-%m-%d %H:%M:%S").to_string(),),
            "bool" => quote!(if self.#ident { 1 } else { 0 },),
            _ => quote!(&self.#ident,)
        }
    }
}

#[allow(unused)]
pub fn get_table_fields(fields: &Fields) -> HashMap<&Ident, (&Ident,&Type, String, bool, bool)> {
    let mut fields_info: HashMap<&Ident, (&Ident, &Type, String, bool, bool)> = HashMap::new();
    for field in fields.iter() {
        let name = field.ident.as_ref().unwrap();
        let identify = has_contract_meta(&field.attrs, "table_id");
        let name_value = get_contract_meta_item_value(&field.attrs, if identify { "table_id" } else { "field" }, "name");
        let exist_value = get_contract_meta_item_value(&field.attrs, "field", "exist");
        let exist_value = exist_value.unwrap_or_default().ne("false");
        let value = name_value.to_owned().unwrap_or(name.to_string());
        // filter the unsuport type.
        if !valid_type(&field.ty) {
            continue;
        }
        fields_info.insert(name, (name ,&field.ty, value.to_owned(), identify, exist_value));
    }
    fields_info
}

#[allow(unused)]
fn get_type_set_value(ty: &Type, ident: &Ident, name: &String) -> TokenStream2 {
    let ori_ty = get_field_type(ty).unwrap_or_default();
    let mut ft = String::default();
    if let Type::Path(r#path) = ty {
        ft = r#path.path.segments[0].ident.to_string();
    }
    if ft.eq("Option") {
        match ori_ty.as_str() {
            "NaiveDate"  => quote!(
                if let Some(value) = &self.#ident {
                    update_fields.push(format!("{} = '{}'", #name, value.format("%Y-%m-%d").to_string()));
                }
            ),
            "NaiveDateTime" => quote!(
                if let Some(value) = &self.#ident {
                    update_fields.push(format!("{} = '{}'", #name, value.format("%Y-%m-%d %H:%M:%S").to_string()));
                }
            ),
            "bool" => quote!(
                if let Some(value) = &self.#ident {
                    update_fields.push(format!("{} = '{}'", #name, if *value { 1 } else { 0 }));
                }
            ),
            _ =>  quote!(
                if let Some(value) = &self.#ident {
                    update_fields.push(format!("{} = '{}'", #name, value));
                }
            )
        }
    } else {
        match ori_ty.as_str() {
            "NaiveDate"  => quote!(
                update_fields.push(format!("{} = '{}'", #name, &self.#ident.#ident.format("%Y-%m-%d").to_string()));
            ),
            "NaiveDateTime" => quote!(
                update_fields.push(format!("{} = '{}'", #name, &self.#ident.format("%Y-%m-%d %H:%M:%S").to_string()));
            ),
            "bool" => quote!(
                update_fields.push(format!("{} = '{}'", #name, if self.#ident { 1 } else { 0 }));
            ),
            _ => quote!(
                update_fields.push(format!("{} = '{}'", #name, &self.#ident));
            )
        }
    }
}

pub fn impl_get_table_name(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let generics = &derive_input.generics;

    quote!(
        impl #generics GetTableName for #name #generics {
            fn table_name() -> TableName {
                TableName{
                    name: stringify!(#name).to_lowercase().into(),
                    schema: None,
                    alias: None,
                }
            }
        }
    ).into()
}

pub fn impl_get_table(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let generics = &derive_input.generics;
    let table_name = get_contract_meta_item_value(&derive_input.attrs, "table", "name").unwrap_or("".to_string());
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
        Data::Enum(_) => panic!("#[derive(Table)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(Table)] can only be used with structs"),
    };
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty, attrs)| {
            let identify = has_contract_meta(attrs, "table_id");
            let field_name = get_contract_meta_item_value(attrs, if identify { "table_id" } else { "field" }, "name").unwrap_or(field.to_string());
            let exist = if identify { "true".to_string() } else { let exist = get_contract_meta_item_value(&attrs, "field", "exist").unwrap_or("true".to_string()); if exist.eq("false") { exist } else { "true".to_string() }  };
            let field_type = if identify {
                let field_type = get_contract_meta_item_value(attrs, "table_id", "type").unwrap_or("none".to_string()).to_lowercase();
                quote!(akita::FieldType::TableId(#field_type.to_string()))
            } else {
                quote!(akita::FieldType::TableField)
            };
            quote!(
                akita::FieldName {
                    name: #field_name.to_string(),
                    table: #table_name.to_string().into(),
                    alias: stringify!(#field).to_string().into(),
                    field_type: #field_type,
                    exist: #exist.eq("true"),
                },
            )
        })
        .collect();
    let result = quote!(
        impl #generics akita::GetTableName for #name #generics {
            fn table_name() -> akita::TableName {
                akita::TableName{
                    name: #table_name.to_string(),
                    schema: None,
                    alias: stringify!(#name).to_lowercase().into(),
                }
            }
        }

        impl #generics akita::GetFields for #name #generics {
            fn fields() -> Vec<akita::FieldName> {
                vec![
                    #(#from_fields)*
                ]
            }
        }

        impl akita::BaseMapper for #name {

            type Item = #name;

            fn insert<M: akita::AkitaMapper>(&self, entity_manager: &mut M) -> Result<usize, akita::AkitaError> where Self::Item : akita::GetFields + akita::GetTableName + akita::ToAkita {
                let data: Self::Item = self.clone();
                entity_manager.save(&data)
            }

            fn insert_batch<M: akita::AkitaMapper>(datas: &[&Self::Item], entity_manager: &mut M) -> Result<Vec<usize>, akita::AkitaError> where Self::Item : akita::GetTableName + akita::GetFields {
                entity_manager.save_batch::<Self::Item>(datas)
            }

            fn update<W: akita::Wrapper, M: akita::AkitaMapper>(&self, wrapper: &mut akita::UpdateWrapper, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::GetFields + akita::GetTableName + akita::ToAkita {
                let data: Self::Item = self.clone();
                entity_manager.update(&data, wrapper)
            }

            fn list<W: akita::Wrapper, M: akita::AkitaMapper>(wrapper: &mut W, entity_manager: &mut M) -> Result<Vec<Self::Item>, akita::AkitaError> where Self::Item : akita::GetTableName + akita::GetFields + akita::FromAkita {
                entity_manager.list(wrapper)
            }

            fn update_by_id<M: akita::AkitaMapper>(&self, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::GetFields + akita::GetTableName + akita::ToAkita {
                let data: Self::Item = self.clone();
                entity_manager.update_by_id::<Self::Item>(&data)
            }

            fn delete<W: akita::Wrapper, M: akita::AkitaMapper>(&self, wrapper: &mut W, entity_manager: &mut M) -> Result<(), akita::AkitaError> where Self::Item : akita::GetFields + akita::GetTableName + akita::ToAkita {
                entity_manager.remove::<Self::Item, W>(wrapper)
            }

            fn delete_by_id<I: akita::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<(), akita::AkitaError> where Self::Item : akita::GetFields + akita::GetTableName + akita::ToAkita {
                entity_manager.remove_by_id::<Self::Item, I>(id)
            }

            fn page<W: akita::Wrapper, M: akita::AkitaMapper>(page: usize, size: usize, wrapper: &mut W, entity_manager: &mut M) -> Result<akita::IPage<Self::Item>, akita::AkitaError> where Self::Item : akita::GetTableName + akita::GetFields + akita::FromAkita {
                entity_manager.page::<Self::Item, W>(page, size, wrapper)
            }

            fn count<T, W: akita::Wrapper, M: akita::AkitaMapper>(&mut self, wrapper: &mut W, entity_manager: &mut M) -> Result<usize, akita::AkitaError> {
                entity_manager.count::<Self::Item, W>(wrapper)
            }

            fn find_one<W: akita::Wrapper, M: akita::AkitaMapper>(wrapper: &mut W, entity_manager: &mut M) -> Result<Option<Self::Item>, akita::AkitaError> where Self::Item : akita::GetTableName + akita::GetFields + akita::FromAkita {
                entity_manager.select_one(wrapper)
            }

            /// Find Data With Table's Ident.
            fn find_by_id<I: akita::ToValue, M: akita::AkitaMapper>(&self, entity_manager: &mut M, id: I) -> Result<Option<Self::Item>, akita::AkitaError> where Self::Item : akita::GetTableName + akita::GetFields + akita::FromAkita {
                entity_manager.select_by_id(id)
            }
        }
    ).into();
    result
}


pub fn impl_get_column_names(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let generics = &derive_input.generics;

    let fields: Vec<(&syn::Ident, &Type, bool)> = match derive_input.data {
        Data::Struct(ref rstruct) => {
            let fields = &rstruct.fields;
            fields
                .iter()
                .map(|f| {
                    let ident = f.ident.as_ref().unwrap();
                    let ty = &f.ty;
                    let exist = get_contract_meta_item_value(&f.attrs, "field", "exist").unwrap_or("true".to_string()).parse::<bool>().unwrap();
                    (ident, ty, exist)
                })
                .collect::<Vec<_>>()
        }
        Data::Enum(_) => panic!("#[derive(ToColumnNames)] can only be used with structs"),
        Data::Union(_) => panic!("#[derive(ToColumnNames)] can only be used with structs"),
    };
    let from_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|&(field, _ty, exist)| {
            quote!(
                FieldName {
                    name: stringify!(#field).into(),
                    table: Some(stringify!(#name).to_lowercase().into()),
                    alias: None,
                    exist: #exist,
                },
            )
        })
        .collect();

    quote! (
        impl #generics GetFields for #name #generics {
            fn fields() -> Vec<FieldName> {
                vec![
                    #(#from_fields)*
                ]
            }
        }
    ).into()
}

#[allow(unused)]
fn camel_to_snack<S: Into<String>>(field: S) -> String {
    let mut field_value: String = field.into();
    while let Some(poi) = field_value.chars().position(|c| c.is_uppercase()) {
        //let poi_char = hello.chars()[poi];
        //hello.replace_range(range, replace_with)
        let mut replace_with = String::default();
        let rng = field_value
            .char_indices()
            .nth(poi)
            .map(|(pos, ch)| {
                if pos == 0 { replace_with = format!("{}", ch.to_lowercase()); } else {replace_with = format!("_{}", ch.to_lowercase());}
                pos..pos + ch.len_utf8()
            })
            .unwrap();
            field_value.replace_range(rng, &replace_with,);
    }
    field_value
}

#[allow(unused)]
fn snack_to_camel<S: Into<String>>(field: S) -> String {
    let field_value: String = field.into();
    field_value.split("_").map(|s| {
        let mut snack = s.to_string();
        make_ascii_titlecase(&mut snack);
        snack
    }).collect::<Vec<_>>().join("")

}

#[allow(unused)]
fn make_ascii_titlecase(s: &mut str) -> String {
    if let Some(r) = s.get_mut(0..1) {
        r.make_ascii_uppercase();
        r.to_string()
    } else {
        s.to_string()
    }
}

#[test]
fn test_name() {
    let camel = camel_to_snack("CamelCase");
    let snack = snack_to_camel("snack_case");
}