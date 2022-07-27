use proc_macro::{TokenStream};
use quote::ToTokens;
use quote::quote;
use syn::{self, ItemFn, AttributeArgs, FnArg, Pat};
use proc_macro2::{Ident, Span};
use crate::util::{find_return_type, is_akita_ref, find_fn_body, get_page_req_ident, is_fetch, get_fn_args, is_fetch_array};

#[allow(unused)]
pub fn impl_sql(
    target_fn: &ItemFn,
    args: &AttributeArgs
) -> TokenStream {
    let return_ty = find_return_type(target_fn);
    let func_name_ident = target_fn.sig.ident.to_token_stream();
    let mut akita_ident = "".to_token_stream();
    let mut akita_name = String::new();
    for x in &target_fn.sig.inputs {
        match x {
            FnArg::Receiver(_) => {}
            FnArg::Typed(t) => {
                let ty_stream = t.ty.to_token_stream().to_string();
                if is_akita_ref(&ty_stream) {
                    akita_ident = t.pat.to_token_stream();
                    akita_name = akita_ident.to_string().trim_start_matches("mut ").to_string();
                    break;
                }
            }
        }
    }

    let sql_ident;
    if args.len() == 1 {
        if akita_name.is_empty() {
            panic!("[Akita] you should add akita ref param  akita:&mut Akita  or akita: &mut AkitaEntityManager  on '{}()'!", target_fn.sig.ident);
        }
        sql_ident = args.get(0).expect("[Akita] miss sql macaro param!").to_token_stream();
    } else if args.len() == 2 {
        akita_ident = args.get(0).expect("[Akita] miss akita ident param!").to_token_stream();
        akita_name = format!("{}", akita_ident);
        sql_ident = args.get(1).expect("[Akita] miss sql macro sql param!").to_token_stream();
    } else {
        panic!("[Akita] Incorrect macro parameter length!");
    }

    let func_args_stream = target_fn.sig.inputs.to_token_stream();
    let fn_body = find_fn_body(target_fn);
    /*let is_async = target_fn.sig.asyncness.is_some();
    if !is_async {
        panic!(
            "[akita] #[AkitaTable] 'fn {}({})' must be  async fn! ",
            func_name_ident, func_args_stream
        );
    }*/

    if akita_ident.to_string().starts_with("mut ") {
        akita_ident = Ident::new(&akita_ident.to_string().trim_start_matches("mut "), Span::call_site()).to_token_stream();
    }

    let mut call_method = quote! {};
    let is_fetch = is_fetch(&return_ty.to_string());
    if is_fetch {
        if is_fetch_array(&return_ty.to_string()) {
            call_method = quote! {exec_raw};
        } else {
            call_method = quote! {exec_first};
        }
    } else {
        call_method = quote! {exec_drop};
    }

    //check use page method
    let mut page_req_str = String::new();
    let mut page_req = quote! {};
    if return_ty.to_string().contains("IPage<")
        && func_args_stream.to_string().contains("&PageRequest")
    {
        let req = get_page_req_ident(target_fn, &func_name_ident.to_string());
        page_req_str = req.to_string();
        page_req = quote! {,#req};
        call_method = quote! {fetch_page};
    }

    //append all args
    let sql_args_gen = filter_args_context_id(&akita_name, &get_fn_args(target_fn), &[page_req_str]);
    //gen rust code templete
    let gen_token_temple = quote! {
       pub fn #func_name_ident(#func_args_stream) -> #return_ty{
           let mut akita_args =vec![];
           #sql_args_gen
           #fn_body
           return #akita_ident.#call_method(#sql_ident,akita_args #page_req);
       }
    };
    return gen_token_temple.into();
}



fn filter_args_context_id(
    akita_name: &str,
    fn_arg_name_vec: &Vec<Box<Pat>>,
    skip_names: &[String],
) -> proc_macro2::TokenStream {
    let mut sql_args_gen = quote! {};
    for item in fn_arg_name_vec {
        let item_ident_name= item.to_token_stream().to_string().trim().trim_start_matches("mut ").to_string();
        if item_ident_name.eq(akita_name) {
            continue;
        }
        let mut do_continue = false;
        for x in skip_names {
            if x.eq(&item_ident_name) {
                do_continue = true;
                break;
            }
        }
        if do_continue {
            continue;
        }
        sql_args_gen = quote! {
             #sql_args_gen
             akita_args.push(#item.to_value());
        };
    }
    sql_args_gen
}