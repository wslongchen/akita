//! 
//! Generate Database Methods.
//! 
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

#[proc_macro_derive(Table, attributes(column, table, id))]
pub fn table(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse::<DeriveInput>(input).unwrap();
    let name = &derive_input.ident;
    let mut table_name = "".to_string();
    if let Data::Struct(r#struct) = derive_input.data {
        let fields = r#struct.fields;
        if matches!(&fields, Fields::Named(_)) {
            let mut fields_info: HashMap<&Ident, (&Type, String, bool)> = HashMap::new();
            let mut field_ids: Vec<(&Ident, &Type, String)> = Vec::new();
            for field in fields.iter() {
                let name = field.ident.as_ref().unwrap();
                let identify = has_contract_meta(&field.attrs, "id");
                let name_value = get_contract_meta_item_value(&field.attrs, if identify { "id" } else { "column" }, "name");
                let value = name_value.to_owned().unwrap_or(name.to_string());
                let v = value.to_owned();
                fields_info.insert(name, (&field.ty, v, identify));
                if identify {
                    field_ids.push((name, &field.ty, value));
                }
            }
            let field_idents = fields_info.keys().map(|ident| {
                if let Some((_ty, name, _identify)) = fields_info.get(ident) {
                    name.to_owned()
                } else {
                    ident.to_string()
                }
            }).collect::<Vec<String>>();
            let builder_set_fields = map_fields(&fields, |(ident, ty, _)| {
                let ident_name = ident.to_string();
                if let Type::Path(r#path) = ty {
                    if r#path.path.segments[0].ident == "Option" {
                        quote!(
                            let #ident: #ty= row.get(#ident_name).unwrap_or(None);
                        )
                    } else {
                        quote!(
                            let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                            let #ident = #ident.unwrap_or_default().to_owned();
                        )
                    }
                }  else {
                    quote!(
                        let #ident: Option<#ty>= row.get(#ident_name).unwrap_or(None);
                        let #ident = #ident.unwrap_or_default().to_owned();
                    )
                }
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
                if let Type::Path(r#path) = ty {
                    if r#path.path.segments[0].ident == "Option" {
                        quote!(&self.#ident.to_owned().unwrap_or_default(),)
                    } else {
                        quote!(&self.#ident,)
                    }
                }  else {
                    quote!(&self.#ident,)
                }
            });

            let update_fields = map_fields(&fields, |(ident, ty, attrs)| {
                let name = ident.to_string();
                let mut ft = String::from("");
                if let Type::Path(r#path) = ty {
                    ft = r#path.path.segments[0].ident.to_string();
                }
                let name_value = get_contract_meta_item_value(attrs, "column", "name");
                let name = name_value.to_owned().unwrap_or(name.to_string());
                if ft.eq("Option") {
                    quote!(
                        if let Some(value) = &self.#ident {
                            update_fields.push(format!("{} = '{}'", #name, value));
                        }
                    )
                } else {
                    quote!(
                        update_fields.push(format!("{} = '{}'", #name, &self.#ident));
                    )
                }
            });
            let build_fields_format = field_idents.iter().map(|_| "'{}'".to_string()).collect::<Vec<_>>().join(",");
            let build_values_field = map_fields(&fields, |(ident, _ty, _)|  quote!(#ident,));
            let format = format!("insert into {{}}({{}}) values({})", build_fields_format);
            let result = quote!(
                
                impl BaseMapper for #name {

                    type Item = #name;

                    fn insert<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<u64>, AkitaError> {
                        let sql = format!(#format, #table_name, #build_fields,#build_values);
                        println!("insert :{}", sql);
                        let last_insert_id = match conn {
                            ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.last_insert_id().into()},
                            ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.last_insert_id()},
                            ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.last_insert_id().into()},
                            ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.last_insert_id().into()},
                            ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.last_insert_id().into()},
                        };
                        Ok(last_insert_id)
                    }

                    fn list<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Vec<Self::Item>, AkitaError> where Self::Item: Clone {
                        let table_name = self.get_table_name()?;
                        let table_fields = self.get_table_fields()?;
                        let select_fields = wrapper.get_select_sql();
                        let fields = if select_fields.eq("*") {
                            table_fields
                        } else {
                            select_fields
                        };
                        let sql = format!("select {} from {} where {}", &fields, &table_name, wrapper.get_sql_segment());
                        println!("find_one: {}", sql);
                        let datas = match conn {
                            ConnMut::Mut(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                            ConnMut::TxMut(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                            ConnMut::Owned(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                            ConnMut::Pooled(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                            ConnMut::R2d2Polled(ref mut conn) => if let Ok(result) = conn.query_iter(sql) { result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>() }else { Vec::new() },
                        };
                        Ok(datas)
                    }

                    fn page<'a, 'b, 'c, W: Wrapper>(&self, page: usize, size: usize, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<IPage<Self::Item>, AkitaError>{
                        let table_name = self.get_table_name()?;
                        let table_fields = self.get_table_fields()?;
                        let select_fields = wrapper.get_select_sql();
                        let fields = if select_fields.eq("*") {
                            table_fields
                        } else {
                            select_fields
                        };
                        
                        let count_sql = format!("select count(1) from {} where {}", &table_name, wrapper.get_sql_segment());
                        let count = 0usize;
                        let mut page = IPage::new(page, size ,count, vec![]);

                        let mut id_fields = Vec::new();
                        #page_id_fields
                        let sql = if id_fields.is_empty() {
                            format!("select {} from {} where {} limit {}, {}", &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size)
                        } else {
                            format!("select {} from {} a join (select {} from {} where {} limit {}, {}) b on {}", &fields, &table_name, &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size, id_fields.join(" and "))
                        };

                        // let sql = format!("select {} from {} a join (select {} from {} where {} limit {}, {}) b on a.id = b.id", &fields, &table_name, &fields, &table_name, wrapper.get_sql_segment(),page.offset(),  page.size);
                        println!("find_one: {}", sql);
                        
                        match conn {
                            ConnMut::Mut(ref mut conn) => {
                                let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                                if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                            },
                            ConnMut::TxMut(ref mut conn) => {
                                let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                                if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                            },
                            ConnMut::Owned(ref mut conn) => {
                                let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                                if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                            },
                            ConnMut::Pooled(ref mut conn) => {
                                let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                                if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                            },
                            ConnMut::R2d2Polled(ref mut conn) => {
                                let count: Option<usize> = conn.query_first(&count_sql)?; page.total = count.unwrap_or(0);
                                if let Ok(result) = conn.query_iter(&sql) {  page.records = result.map(|row| { row.map(|row| { from_long_row::<#name>(row) }).ok().unwrap() }).collect::<Vec<_>>(); }
                            },
                        }
                        Ok(page)
                    }

                    fn find_one<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let table_fields = self.get_table_fields()?;
                        let select_fields = wrapper.get_select_sql();
                        let fields = if select_fields.eq("*") {
                            table_fields
                        } else {
                            select_fields
                        };
                        let sql = format!("select {} from {} where {} limit 1", &fields, &table_name, wrapper.get_sql_segment());
                        println!("find_one: {}", sql);
                        let data = match conn {
                            ConnMut::Mut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::TxMut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::Owned(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::Pooled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::R2d2Polled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                        };
                        Ok(data)
                    }
  
                    fn find_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let table_fields = self.get_table_fields()?;
                        let id_fields = self.get_table_idents()?;
                        let sql = format!("select {} from {} where {} limit 1", &table_fields, &table_name, &id_fields);
                        println!("find_by_id: {}", sql);
                        let data = match conn {
                            ConnMut::Mut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::TxMut(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::Owned(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::Pooled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                            ConnMut::R2d2Polled(ref mut conn) => if let Some(raw) = conn.exec_first(&sql, ())? { let data = from_long_row::<#name>(raw); data.into() }else { None },
                        };
                        Ok(data)
                    }

                    fn update<'a, 'b, 'c>(&self, wrapper: &mut UpdateWrapper, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let update_fields = self.get_update_fields(wrapper.get_set_sql())?;
                        let sql = format!("update {} set {} where {}", &table_name, &update_fields, wrapper.get_sql_segment());
                        println!("update: {}", sql);
                        let affected_rows = match conn {
                            ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                        };
                        Ok(affected_rows > 0)
                    }
                
                    fn update_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let update_fields = self.get_update_fields(None)?;
                        let id_fields = self.get_table_idents()?;
                        let sql = format!("update {} set {} where {}", &table_name, &update_fields, &id_fields);
                        println!("update_by_id: {}", sql);
                        let affected_rows = match conn {
                            ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                        };
                        Ok(affected_rows > 0)
                    }

                    fn delete<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let sql = format!("delete from {} where {}", &table_name, wrapper.get_sql_segment());
                        println!("delete: {}", sql);
                        let affected_rows = match conn {
                            ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                        };
                        Ok(affected_rows > 0)
                    }
                
                    fn delete_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError> {
                        let table_name = self.get_table_name()?;
                        let id_fields = self.get_table_idents()?;
                        let sql = format!("delete from {} where {}", &table_name, &id_fields);
                        println!("delete_by_id: {}", sql);
                        let affected_rows = match conn {
                            ConnMut::Mut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::TxMut(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Owned(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::Pooled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                            ConnMut::R2d2Polled(ref mut conn) => { let _ = conn.exec_drop("", ())?; conn.affected_rows()},
                        };
                        Ok(affected_rows > 0)
                    }

                    fn get_table_fields(&self) -> Result<String, AkitaError> {
                        let fields = #build_fields.to_string();
                        if fields.is_empty() {
                            return Err(AkitaError::MissingField("Find Error, Missing Table Fields !".to_string()))
                        }
                        Ok(fields)
                    }

                    fn get_table_idents(&self) -> Result<String, AkitaError> {
                        let mut id_fields = Vec::new();
                        #update_id_fields
                        if id_fields.is_empty() {
                            return Err(AkitaError::MissingIdent("Missing Id Fields !".to_string()))
                        }
                        Ok(id_fields.join(" and "))
                    }

                    fn get_update_fields(&self, set_sql: Option<String>) -> Result<String, AkitaError> {
                        let mut update_fields = Vec::new();
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
                
                    fn get_table_name(&self) -> Result<String, AkitaError> {
                        if #table_name.is_empty() {
                            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
                        }
                        Ok(#table_name.to_string())
                    }
                }

                impl FromRowExt for #name
                {
                    #[inline]
                    fn from_long_row(row: mysql::Row) -> #name {
                        
                        match FromRowExt::from_long_row_opt(row) {
                            Ok(x) => x,
                            Err(mysql::FromRowError(row)) => panic!(
                                "Couldn't convert {:?} to type (T1). (see FromRow documentation)",
                                row
                            ),
                        }
                    }
                    fn from_long_row_opt(
                        row: mysql::Row,
                    ) -> Result<#name, mysql::FromRowError> {
                        if row.len() < 1 {
                            return Err(mysql::FromRowError(row));
                        }
                        #builder_set_fields
                        Ok(
                            #name { #build_values_field }
                        )
                    }
                }
                
            )
            .into();
            eprintln!("{}", result); 
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

/// extra the fields info.
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
fn _get_field_type(ty: &Type) -> Option<String> {
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
fn get_contract_meta_item_value(attrs: &Vec<syn::Attribute>, filter: &str, key:&str) -> Option<String> {
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
                            _ => {}
                        }
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

fn has_contract_meta(attrs: &Vec<syn::Attribute>, filter: &str) -> bool {
    attrs.iter().find(|attr| attr.path.segments.len() == 1 && attr.path.segments[0].ident == filter).is_some()
}