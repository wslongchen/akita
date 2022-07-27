use quote::{quote, ToTokens};
use syn::{self, Type, Ident, parse_quote, spanned::Spanned, ItemFn, ReturnType, FnArg, Pat};
use std::collections::HashMap;
use proc_macro2::{Span};
use proc_macro_error::{abort};
use crate::{comm::{FieldExtra, FieldInformation, CustomArgument, NUMBER_TYPES, COW_TYPE, CUSTOM_ARG_LIFETIME, CUSTOM_ARG_ALLOWED_COPY_TYPES, ValueOrPath}};


/// get the field orignal type
#[allow(unused)]
pub fn get_field_type(ty: &syn::Type) -> Option<String> {
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


#[allow(unused)]
pub fn get_field_default_value(ty: &Type, ident: &Ident) -> proc_macro2::TokenStream {
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
            "NaiveDate"  => quote!(Local::now().naive_local().date()),
            "NaiveDateTime" => quote!(Local::now().naive_local()),
            "Vec" => quote!(Vec::new()),
            "Value" => quote!(serde_json::Value::default()),
            _ => quote!(None)
        }
    }
}

/// Finds all struct schema annotion
pub fn find_struct_annotions(struct_attrs: &[syn::Attribute]) -> Vec<FieldExtra> {
    struct_attrs
        .iter()
        .find(|attribute| {
            attribute.path == parse_quote!(table)
        })
        .map(|attribute| find_struct_annotion(attribute)).unwrap_or(vec![])
}

pub fn find_struct_annotion(attr: &syn::Attribute) -> Vec<FieldExtra> {
    let mut extras = vec![];
    let error = |span: Span, msg: &str| -> ! {
        abort!(span, "Invalid table annotion: {}", msg);
    };

    if attr.path != parse_quote!(table) {
        error(attr.span(), "missing annotion for `table` ");
    }

    match attr.parse_meta() {
        Ok(syn::Meta::List(syn::MetaList { ref nested, .. })) => {
            let meta_items = nested.iter().collect::<Vec<_>>();
            // only field from there on
            for meta_item in meta_items {
                match *meta_item {
                    syn::NestedMeta::Meta(ref item) => match *item {
                        // name
                        syn::Meta::Path(ref name) => {
                            match name.get_ident().unwrap().to_string() {
                                _ => {
                                    let mut ident = proc_macro2::TokenStream::new();
                                    name.to_tokens(&mut ident);
                                    abort!(name.span(), "Unexpected annotion: {}", ident)
                                }
                            }
                        }
                        // fill, name, select, numberic_scale, exist
                        syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                            let ident = path.get_ident().unwrap();
                            match ident.to_string().as_ref() {
                                "name" => {
                                    match lit_to_string(lit) {
                                        Some(s) => extras.push(FieldExtra::Table(s)),
                                        None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                                    };
                                }
                                v => abort!(path.span(),"unexpected name value annotion: {:?}",v),
                            };
                        }
                        _ => unreachable!("Found a non Meta while looking for annotions"),
                    },
                    _ => unreachable!("Found a non Meta while looking for annotions"),
                };
            }
        }
        Ok(syn::Meta::Path(ref name)) => extras.push(FieldExtra::Name(name.get_ident().unwrap().to_string())),
        Ok(syn::Meta::NameValue(syn::MetaNameValue { ref lit, ref path, .. })) => {
            let ident = path.get_ident().unwrap();
            match ident.to_string().as_ref() {
                "name" => {
                    match lit_to_string(lit) {
                        Some(s) => extras.push(FieldExtra::Name(s)),
                        None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                    };
                }
                v => abort!(path.span(),"unexpected name value annotion: {:?}",v),
            };
        },
        Err(e) => {
            abort!(attr.span(), "Unable to parse this attribute for the table with the error: {:?}", e );
        },
    }

    if extras.is_empty() {
        abort!(attr.span(), "Unable to parse this attribute for the table");
    }
    extras
}


pub fn collect_field_info(ast: &syn::DeriveInput) -> Vec<FieldInformation> {
    let mut fields = collect_fields(ast);
    let field_types = find_fields_type(&fields);
    fields.drain(..).fold(vec![], |mut acc, field| {
        let key = field.ident.clone().unwrap().to_string();
        let (name, extra) = find_extra_for_field(&field, &field_types);
        acc.push(FieldInformation::new(
            field,
            field_types.get(&key).unwrap().clone(),
            name,
            extra,
        ));
        acc
    })
}

/// Find the types (as string) for each field of the struct
/// Needed for the `must_match` filter
pub fn find_fields_type(fields: &[syn::Field]) -> HashMap<String, String> {
    let mut types = HashMap::new();

    for field in fields {
        let field_ident = field.ident.clone().unwrap().to_string();
        let field_type = match field.ty {
            syn::Type::Path(syn::TypePath { ref path, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                path.to_tokens(&mut tokens);
                tokens.to_string().replace(' ', "")
            }
            syn::Type::Reference(syn::TypeReference { ref lifetime, ref elem, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                elem.to_tokens(&mut tokens);
                let mut name = tokens.to_string().replace(' ', "");
                if lifetime.is_some() {
                    name.insert(0, '&')
                }
                name
            }
            syn::Type::Group(syn::TypeGroup { ref elem, .. }) => {
                let mut tokens = proc_macro2::TokenStream::new();
                elem.to_tokens(&mut tokens);
                tokens.to_string().replace(' ', "")
            }
            _ => {
                let mut field_type = proc_macro2::TokenStream::new();
                field.ty.to_tokens(&mut field_type);
                abort!(
                    field.ty.span(),
                    "Type `{}` of field `{}` not supported",
                    field_type,
                    field_ident
                )
            }
        };
        types.insert(field_ident, field_type);
    }

    types
}

/// collect the ast fields
pub fn collect_fields(ast: &syn::DeriveInput) -> Vec<syn::Field> {
    match ast.data {
        syn::Data::Struct(syn::DataStruct { ref fields, .. }) => {
            if fields.iter().any(|field| field.ident.is_none()) {
                abort!(
                    fields.span(),
                    "struct has unnamed fields";
                    help = "#[derive(AkitaTable)] can only be used on structs with named fields";
                );
            }
            fields.iter().cloned().collect::<Vec<_>>()
        }
        _ => abort!(ast.span(), "#[derive(AkitaTable)] can only be used with structs"),
    }
}
/// Find everything we need to know about a field
pub fn find_extra_for_field(
    field: &syn::Field,
    _field_types: &HashMap<String, String>,
) -> (String, Vec<FieldExtra>) {
    let rust_ident = field.ident.clone().unwrap().to_string();
    let field_ident = field.ident.clone().unwrap().to_string();

    let error = |span: Span, msg: &str| -> ! {
        abort!(
            span,
            "Invalid attribute #[field] on field `{}`: {}",
            field.ident.clone().unwrap().to_string(),
            msg
        );
    };

    let mut extras = vec![];
    let mut has_field = false;

    for attr in &field.attrs {
        if attr.path != parse_quote!(field) && attr.path != parse_quote!(table_id) {
            continue;
        }
        if attr.path == parse_quote!(field) || attr.path != parse_quote!(table_id) {
            has_field = true;
        }
        match attr.parse_meta() {
            Ok(syn::Meta::List(syn::MetaList { ref nested, .. })) => {
                let meta_items = nested.iter().collect::<Vec<_>>();
                // only field from there on
                for meta_item in meta_items {
                    match *meta_item {
                        syn::NestedMeta::Meta(ref item) => match *item {
                            // name, exist, fill, select
                            syn::Meta::Path(ref name) => {
                                match name.get_ident().unwrap().to_string() {
                                    // "fill" => {
                                    //     extras.push(FieldExtra::Name());
                                    // }
                                    _ => {
                                        let mut ident = proc_macro2::TokenStream::new();
                                        name.to_tokens(&mut ident);
                                        abort!(name.span(), "Unexpected annotion: {}", ident)
                                    }
                                }
                            }
                            // fill, name, select, numberic_scale, exist
                            syn::Meta::NameValue(syn::MetaNameValue {
                                                     ref path, ref lit, ..
                                                 }) => {
                                let ident = path.get_ident().unwrap();
                                match ident.to_string().as_ref() {
                                    "fill" => {
                                        match lit_to_string(lit) {
                                            Some(s) => extras.push(FieldExtra::Fill{
                                                function: s,
                                                mode: None,
                                                argument: None,
                                            }),
                                            None => error(lit.span(), "invalid argument for `fill` annotion: only strings are allowed"),
                                        };
                                    }
                                    "name" => {
                                        match lit_to_string(lit) {
                                            Some(s) => extras.push(FieldExtra::Name(s)),
                                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                                        };
                                    }
                                    "id_type" => {
                                        match lit_to_string(lit) {
                                            Some(s) => match s.to_lowercase().as_ref() {
                                                "auto" | "none" | "input" | "assign_id" | "assign_uuid" => extras.push(FieldExtra::IdType(s)),
                                                _=> error(lit.span(), "invalid argument for `id_type` annotion: only `auto` `none` `input` `assign_id` `assign_uuid` are allowed")
                                            },
                                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                                        };
                                    }
                                    "select" => {
                                        match lit_to_bool(lit) {
                                            Some(s) => extras.push(FieldExtra::Select(s)),
                                            None => error(lit.span(), "invalid argument for `select` annotion: only boolean are allowed"),
                                        };
                                    }
                                    "exist" => {
                                        match lit_to_bool(lit) {
                                            Some(s) => extras.push(FieldExtra::Exist(s)),
                                            None => error(lit.span(), "invalid argument for `exist` annotion: only boolean are allowed"),
                                        };
                                    }
                                    "numberic_scale" => {
                                        match lit_to_u64_or_path(lit) {
                                            Some(s) => {
                                                assert_has_number(rust_ident.clone(), "numberic_scale", &field.ty);
                                                extras.push(FieldExtra::NumericScale(s));
                                            },
                                            None => error(lit.span(), "invalid argument for `numberic_scale` annotion: only strings are allowed"),
                                        };
                                    }
                                    v => abort!(
                                        path.span(),
                                        "unexpected name value annotion: {:?}",
                                        v
                                    ),
                                };
                            }
                            // Annotion with several args.
                            syn::Meta::List(syn::MetaList { ref path, ref nested, .. }) => {
                                let meta_items = nested.iter().cloned().collect::<Vec<_>>();
                                let ident = path.get_ident().unwrap();
                                match ident.to_string().as_ref() {
                                    "fill" => {
                                        extras.push(extract_fill_custom(
                                            rust_ident.clone(),
                                            attr,
                                            &meta_items,
                                        ));
                                    }
                                    "id_type"
                                    | "select"
                                    | "exist"
                                    | "name"
                                    | "numberic_scale" => {
                                        extras.push(extract_one_arg_annotion(
                                            "value",
                                            ident.to_string(),
                                            rust_ident.clone(),
                                            &meta_items,
                                        ));
                                    }
                                    v => abort!(path.span(), "unexpected list annotion: {:?}", v),
                                }
                            }
                        },
                        _ => unreachable!("Found a non Meta while looking for annotions"),
                    };
                }
            }
            Ok(syn::Meta::Path(ref name)) => {
                let ident = name.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "table_id" => extras.push(FieldExtra::TableId(String::from("none"))),
                    _ => extras.push(FieldExtra::Field),
                }
            },
            Ok(syn::Meta::NameValue(syn::MetaNameValue { ref lit, ref path, .. })) => {
                let ident = path.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "fill" => {
                        match lit_to_string(lit) {
                            Some(s) => extras.push(FieldExtra::Fill{
                                function: s,
                                mode: None,
                                argument: None,
                            }),
                            None => error(lit.span(), "invalid argument for `fill` annotion: only strings are allowed"),
                        };
                    }
                    "name" => {
                        match lit_to_string(lit) {
                            Some(s) => extras.push(FieldExtra::Name(s)),
                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                        };
                    }
                    "id_type" => {
                        match lit_to_string(lit) {
                            Some(s) => {
                                match s.to_lowercase().as_ref() {
                                    "auto" | "none" | "input" | "assign_id" | "assign_uuid" => extras.push(FieldExtra::IdType(s)),
                                    _=> error(lit.span(), "invalid argument for `id_type` annotion: only `auto` `none` `input` `assign_id` `assign_uuid` are allowed")
                                }

                            },
                            None => error(lit.span(), "invalid argument for `name` annotion: only strings are allowed"),
                        };
                    }
                    "select" => {
                        match lit_to_bool(lit) {
                            Some(s) => extras.push(FieldExtra::Select(s)),
                            None => error(lit.span(), "invalid argument for `select` annotion: only boolean are allowed"),
                        };
                    }
                    "exist" => {
                        match lit_to_bool(lit) {
                            Some(s) => extras.push(FieldExtra::Exist(s)),
                            None => error(lit.span(), "invalid argument for `exist` annotion: only boolean are allowed"),
                        };
                    }
                    "numberic_scale" => {
                        match lit_to_u64_or_path(lit) {
                            Some(s) => {
                                assert_has_number(rust_ident.clone(), "numberic_scale", &field.ty);
                                extras.push(FieldExtra::NumericScale(s));
                            },
                            None => error(lit.span(), "invalid argument for `numberic_scale` annotion: only strings are allowed"),
                        };
                    }
                    v => abort!(
                                        path.span(),
                                        "unexpected name value annotion: {:?}",
                                        v
                                    ),
                };
            },
            Err(e) => {
                let error_string = format!("{:?}", e);
                if error_string == "Error(\"expected literal\")" {
                    abort!(attr.span(),
                        "This attributes for the field `{}` seem to be misformed, please annotion the syntax with the documentation",
                        field_ident
                    );
                } else {
                    abort!(attr.span(),
                        "Unable to parse this attribute for the field `{}` with the error: {:?}",
                        field_ident, e
                    );
                }
            },
        }

        if has_field && extras.is_empty() {
            extras.push(FieldExtra::Field);
        }
    }

    (field_ident, extras)
}

/// For fill, name, exist, select, numberic_scale
pub fn extract_one_arg_annotion(
    val_name: &str,
    name: String,
    field: String,
    meta_items: &[syn::NestedMeta],
) -> FieldExtra {
    let mut value = None;
    for meta_item in meta_items {
        match *meta_item {
            syn::NestedMeta::Meta(ref item) => match *item {
                syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                    let ident = path.get_ident().unwrap();
                    match ident.to_string().as_str() {
                        v if v == val_name => {
                            value = match lit_to_string(lit) {
                                Some(s) => Some(s),
                                None => abort!(
                                    item.span(),
                                    "Invalid argument type for `{}` for annotion `{}` on field `{}`: only a string is allowed",
                                    val_name, name, field
                                ),
                            };
                        }
                        v => abort!(
                            path.span(),
                            "Unknown argument `{}` for annotion `{}` on field `{}`",
                            v,
                            name,
                            field
                        ),
                    }
                }
                _ => abort!(
                    item.span(),
                    "unexpected item {:?} while parsing `range` annotion",
                    item
                ),
            },
            _ => unreachable!(),
        }

        if value.is_none() {
            abort!(
                meta_item.span(),
                "Missing argument `{}` for annotion `{}` on field `{}`",
                val_name,
                name,
                field
            );
        }
    }

    let extra = match name.as_ref() {
        "fill" => FieldExtra::Fill { function: value.unwrap(), argument: None, mode: None },
        "id_type" => FieldExtra::IdType(value.unwrap()),
        "select" => FieldExtra::Select(value.unwrap().parse::<bool>().unwrap_or(true)),
        "exist" => FieldExtra::Exist(value.unwrap().parse::<bool>().unwrap_or(true)),
        "name" => FieldExtra::Name(value.unwrap()),
        // "numberic_scale" => FieldExtra::NumericScale(value.unwrap()),
        _ => unreachable!(),
    };
    extra
}

pub fn extract_fill_custom(
    field: String,
    attr: &syn::Attribute,
    meta_items: &[syn::NestedMeta],
) -> FieldExtra {
    let mut function = None;
    let mut argument = None;
    let mut mode = None;

    let error = |span: Span, msg: &str| -> ! {
        abort!(span, "Invalid attribute #[field] on field `{}`: {}", field, msg);
    };

    for meta_item in meta_items {
        match *meta_item {
            syn::NestedMeta::Meta(ref item) => match *item {
                syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                    let ident = path.get_ident().unwrap();
                    match ident.to_string().as_ref() {
                        "function" => {
                            function = match lit_to_string(lit) {
                                Some(s) => Some(s),
                                None => error(lit.span(), "invalid argument type for `function` of `fill` annotion: expected a string")
                            };
                        }
                        "mode" => {
                            mode = match lit_to_string(lit) {
                                Some(s) => match s.as_ref() {
                                    "default" | "insert" | "update" => {
                                        Some(s)
                                    }
                                    _ => {
                                        error(lit.span(), "invalid argument type for `mode` of `fill` annotion: expected `default`,`insert`,`update` ")
                                    }
                                },
                                None => error(lit.span(), "invalid argument type for `mode` of `fill` annotion: expected a string")
                            };
                        }
                        "arg" => {
                            match lit_to_string(lit) {
                                Some(s) => {
                                    match syn::parse_str::<syn::Type>(s.as_str()) {
                                        Ok(arg_type) => {
                                            assert_custom_arg_type(&lit.span(), &arg_type);
                                            argument = Some(CustomArgument::new(lit.span().clone(), arg_type));
                                        }
                                        Err(_) => {
                                            let mut msg = "invalid argument type for `arg` of `fill` annotion: The string has to be a single type.".to_string();
                                            msg.push_str("\n(Tip: You can combine multiple types into one tuple.)");

                                            error(lit.span(), msg.as_str());
                                        }
                                    }
                                },
                                None => error(lit.span(), "invalid argument type for `arg` of `fill` annotion: expected a string")
                            };
                        }
                        v => error(path.span(), &format!(
                            "unknown argument `{}` for annotion `fill` (it only has `function`, `arg`)",
                            v
                        )),
                    }
                }
                _ => abort!(
                    item.span(),
                    "unexpected item {:?} while parsing `fill` annotion",
                    item
                ),
            },
            _ => unreachable!(),
        }
    }

    if function.is_none() {
        error(attr.span(), "The annotion `custom` requires the `function` parameter.");
    }
    let extra = FieldExtra::Fill { function: function.unwrap(), argument, mode };
    extra
}

pub fn assert_has_number(field_name: String, type_name: &str, field_type: &syn::Type) {
    if !NUMBER_TYPES.contains(&type_name) {
        abort!(
            field_type.span(),
            "AkitaTable `numberic_scale` can only be used on number types but found `{}` for field `{}`",
            type_name,
            field_name
        );
    }
}

#[allow(unused)]
pub fn assert_string_type(name: &str, type_name: &str, field_type: &syn::Type) {
    if type_name != "String"
        && type_name != "&str"
        && !COW_TYPE.is_match(type_name)
        && type_name != "Option<String>"
        && type_name != "Option<Option<String>>"
        && !(type_name.starts_with("Option<") && type_name.ends_with("str>"))
        && !(type_name.starts_with("Option<Option<") && type_name.ends_with("str>>"))
    {
        abort!(
            field_type.span(),
            "`{}` annotion can only be used on String, &str, Cow<'_,str> or an Option of those",
            name
        );
    }
}

pub fn assert_custom_arg_type(field_span: &Span, field_type: &syn::Type) {
    match field_type {
        syn::Type::Reference(reference) => {
            if let Some(lifetime) = &reference.lifetime {
                let lifetime_ident = lifetime.ident.to_string();
                if lifetime_ident != CUSTOM_ARG_LIFETIME {
                    abort!(
                        field_span,
                        "Invalid argument reference: The lifetime `'{}` is not supported. Please use the field lifetime `'{}`",
                        lifetime_ident,
                        CUSTOM_ARG_LIFETIME
                    );
                }
            } else {
                abort!(
                    field_span,
                    "Invalid argument reference: All references need to use the field lifetime `'{}`",
                    CUSTOM_ARG_LIFETIME
                );
            }
        }
        // trigger nested annotion
        syn::Type::Paren(paren) => {
            assert_custom_arg_type(field_span, &paren.elem);
        }
        syn::Type::Tuple(tuple) => {
            tuple.elems.iter().for_each(|x| assert_custom_arg_type(field_span, x));
        }
        // assert idents
        syn::Type::Path(path) => {
            let segments = &path.path.segments;
            if segments.len() == 1 {
                let ident = &segments.first().unwrap().ident.to_string();
                if CUSTOM_ARG_ALLOWED_COPY_TYPES.contains(&ident.as_str()) {
                    // A known copy type that can be passed without a reference
                    return;
                }
            }

            abort!(
                field_span,
                "Invalid argument type: All types except numbers and tuples need be passed by reference using the lifetime `'{}`",
                CUSTOM_ARG_LIFETIME,
            );
        }
        // Not allows
        _ => {
            abort!(
                field_span,
                "Invalid argument type: Custom arguments only allow tuples, number types and references using the lifetime `'{}` ",
                CUSTOM_ARG_LIFETIME,
            );
        }
    }
}

pub fn lit_to_string(lit: &syn::Lit) -> Option<String> {
    match *lit {
        syn::Lit::Str(ref s) => Some(s.value()),
        _ => None,
    }
}

pub fn lit_to_int(lit: &syn::Lit) -> Option<u64> {
    match *lit {
        syn::Lit::Int(ref s) => Some(s.base10_parse().unwrap()),
        _ => None,
    }
}

pub fn lit_to_u64_or_path(lit: &syn::Lit) -> Option<ValueOrPath<u64>> {
    let number = lit_to_int(lit);
    if let Some(number) = number {
        return Some(ValueOrPath::Value(number));
    }

    let path = lit_to_string(lit);
    if let Some(path) = path {
        return Some(ValueOrPath::Path(path));
    }

    None
}

pub fn lit_to_bool(lit: &syn::Lit) -> Option<bool> {
    match *lit {
        syn::Lit::Bool(ref s) => Some(s.value),
        _ => None,
    }
}

#[allow(unused)]
pub fn option_to_tokens<T: quote::ToTokens>(opt: &Option<T>) -> proc_macro2::TokenStream {
    match opt {
        Some(ref t) => quote!(::std::option::Option::Some(#t)),
        None => quote!(::std::option::Option::None),
    }
}

//find and check method return type
pub(crate) fn find_return_type(target_fn: &ItemFn) -> proc_macro2::TokenStream {
    let mut return_ty = target_fn.sig.output.to_token_stream();
    match &target_fn.sig.output {
        ReturnType::Type(_, b) => {
            return_ty = b.to_token_stream();
        }
        _ => {}
    }
    let mut s = format!("{}", return_ty);

    if s.trim().is_empty() {
        return_ty = quote! {
            ()
        }
    }

    if !s.contains("::Result") && !s.starts_with("Result") {
        return_ty = quote! {
             Result <#return_ty, akita::AkitaError>
        };
    }
    return_ty
}

pub(crate) fn is_akita_ref(ty_stream: &str) -> bool {
    if ty_stream.contains("Akita")
        || ty_stream.contains("AkitaEntityManager") {
        return true;
    }
    false
}

pub(crate) fn is_fetch(return_source: &str) -> bool {
    let is_select = !return_source.contains("()");
    return is_select;
}
pub(crate) fn is_fetch_array(return_source: &str) -> bool {
    let is_array = return_source.contains("Vec");
    return is_array;
}

pub fn to_snake_name(name: &String) -> String {
    let chs = name.chars();
    let mut new_name = String::new();
    let mut index = 0;
    let chs_len = name.len();
    for x in chs {
        if x.is_uppercase() {
            if index != 0 && (index + 1) != chs_len {
                new_name.push_str("_");
            }
            new_name.push_str(x.to_lowercase().to_string().as_str());
        } else {
            new_name.push(x);
        }
        index += 1;
    }
    return new_name;
}


/// find and check method return type
pub(crate) fn find_fn_body(target_fn: &ItemFn) -> proc_macro2::TokenStream {
    let mut target_fn = target_fn.clone();
    let mut new_stmts = vec![];
    for x in &target_fn.block.stmts {
        let token = x.to_token_stream().to_string().replace("\n", "").replace(" ", "");
        if token.eq("todo!()") || token.eq("unimplemented!()") || token.eq("impled!()") {
            //nothing to do
        } else {
            new_stmts.push(x.to_owned());
        }
    }
    target_fn.block.stmts = new_stmts;
    target_fn.block.to_token_stream()
}

pub(crate) fn get_fn_args(target_fn: &ItemFn) -> Vec<Box<Pat>> {
    let mut fn_arg_name_vec = vec![];
    for arg in &target_fn.sig.inputs {
        match arg {
            FnArg::Typed(t) => {
                fn_arg_name_vec.push(t.pat.clone());
                //println!("arg_name {}", arg_name);
            }
            _ => {}
        }
    }
    fn_arg_name_vec
}

pub(crate) fn filter_fn_args(
    target_fn: &ItemFn,
    arg_name: &str,
    arg_type: &str,
) -> std::collections::HashMap<String, String> {
    let mut map = HashMap::new();
    for arg in &target_fn.sig.inputs {
        match arg {
            FnArg::Typed(t) => {
                let arg_name_value = format!("{}", t.pat.to_token_stream());
                if arg_name.eq(&arg_name_value) {
                    map.insert(arg_name.to_string(), arg_name_value.clone());
                }
                let arg_type_name = t.ty.to_token_stream().to_string();
                if arg_type.eq(&arg_type_name) {
                    map.insert(arg_type.to_string(), arg_name_value.clone());
                }
            }
            _ => {}
        }
    }
    map
}

pub(crate) fn get_page_req_ident(target_fn: &ItemFn, func_name: &str) -> Ident {
    let page_reqs = filter_fn_args(target_fn, "", "&PageRequest");
    if page_reqs.len() > 1 {
        panic!(
            "[Akita] {} only support on arg of '**:&PageRequest'!",
            func_name
        );
    }
    if page_reqs.len() == 0 {
        panic!(
            "[Akita] {} method arg must have arg Type '**:&PageRequest'!",
            func_name
        );
    }
    let req = page_reqs
        .get("&PageRequest")
        .unwrap_or(&String::new())
        .to_owned();
    if req.eq("") {
        panic!(
            "[Akita] {} method arg must have arg Type '**:&PageRequest'!",
            func_name
        );
    }
    let req = Ident::new(&req, Span::call_site());
    req
}