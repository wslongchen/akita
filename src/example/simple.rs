use crate::{UpdateWrapper, Wrapper, wrapper, BaseMapper, AkitaError, value::*};
use crate::pool::{AkitaConfig, LogLevel};
use crate::{pool::Pool, data::*, IPage};
use crate::manager::{GetTableName, TableName, FieldName, GetFields, FieldType, AkitaEntityManager};

#[derive(Debug, FromAkita, ToAkita, Table, Clone)]
#[table(name="t_system_user")]
struct SystemUser {
    #[field = "name"]
    id: Option<i32>,
    #[table_id]
    username: String,
    #[field(name="ages", exist = "false")]
    age: i32,
}

fn get_table_info() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let table = em
        .get_table(&TableName::from("public.film"))
        .expect("must have a table");
    println!("table: {:#?}", table);
}

fn remove() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrap = UpdateWrapper::new();
    wrap.eq(true, "username", "'ussd'");
    match em.remove::<SystemUser, UpdateWrapper>(&mut wrap) {
        Ok(res) => {
            println!("success removed data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn count() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrap = UpdateWrapper::new();
    wrap.eq(true, "username", "'ussd'");
    match em.count::<SystemUser, UpdateWrapper>(&mut wrap) {
        Ok(res) => {
            println!("success count data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}


fn remove_by_id() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    match em.remove_by_id::<SystemUser, String>("'fffsd'".to_string()) {
        Ok(res) => {
            println!("success removed data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn update() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
    let mut wrap = UpdateWrapper::new();
    wrap.eq(true, "username", "'ussd'");
    match em.update(&user, &mut wrap) {
        Ok(res) => {
            println!("success update data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn update_by_id() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
    match em.update_by_id(&user, "id") {
        Ok(res) => {
            println!("success update data by id!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}


fn save() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
    match em.save(&user) {
        Ok(res) => {
            println!("success save data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn save_batch() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
    match em.save_batch::<_>(&vec![&user]) {
        Ok(res) => {
            println!("success save_batch data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn list() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrapper = UpdateWrapper::new();
    wrapper.eq(true, "username", "'ussd'");
    match em.list::<SystemUser, UpdateWrapper>(&mut wrapper) {
        Ok(res) => {
            println!("success list data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn page() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrapper = UpdateWrapper::new();
    wrapper.eq(true, "username", "'ussd'");
    match em.page::<SystemUser, UpdateWrapper>(1, 10,&mut wrapper) {
        Ok(res) => {
            println!("success page data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn select_by_id() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrapper = UpdateWrapper::new();
    wrapper.eq(true, "username", "'ussd'");
    match em.select_by_id::<SystemUser, i32>(1) {
        Ok(res) => {
            println!("success select one data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}

fn select_one() {
    let db_url = "mysql://root:longchen@localhost:3306/akita";
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrapper = UpdateWrapper::new();
    wrapper.eq(true, "username", "'ussd'");
    match em.select_one::<SystemUser, UpdateWrapper>(&mut wrapper) {
        Ok(res) => {
            println!("success select one data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}