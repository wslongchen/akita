/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */

//!
//! Tests.
//!
use std::fs::File;
use std::time::Duration;

use base64::Config;
use chrono::NaiveDateTime;

use akita::*;

/// Connection configuration
#[derive(Debug)]
pub struct Database {
    pub ip: String,
    pub username: String,
    pub password: String,
    pub db_name: String,
    pub port: u16,
}

#[derive(Entity, Clone, Default)]
#[table(name = "t_system_user")]
pub struct User {
    #[id(name = "id")]
    pub pk: i64,
    pub id: String,
    pub headline: Option<NaiveDateTime>,
    /// 状态
    pub status: u8,
    /// 用户等级 0.普通会员 1.VIP会员
    pub level: u8,
    /// 生日
    pub birthday: Option<NaiveDate>,
    /// 性别
    pub gender: u8,
    #[field(exist = false)]
    pub is_org: bool,
    #[field(name = "token")]
    pub url_token: String,
}

// #[allow(unused, non_snake_case, dead_code)]
#[test]
#[cfg(feature = "akita-mysql")]
fn main() {
    let database = Database {
        ip: "101.132.66.102".to_string(),
        username: "weshop".to_string(),
        password: "6&u@f@Q6#".to_string(),
        db_name: "advanced_statistics".to_string(),
        port: 3306,
    };

    // init pool
    let cfg = AkitaConfig::default()
        .set_max_size(5)
        .set_connection_timeout(Duration::from_secs(5))
        .set_log_level(LogLevel::Info)
        .set_platform("mysql")
        .set_password(database.password)
        .set_username(database.username)
        .set_port(database.port)
        .set_db_name(database.db_name)
        .set_ip_or_hostname(database.ip);
    let akita = Akita::new(cfg.clone()).unwrap();

    // Insert
    let insert_id: Option<i32> = akita.save(&User::default()).unwrap();
    let insert_id_second = model.insert::<Option<i32>, _>(&akita).unwrap();
    let insert_ids = akita.save_batch(&[&User::default()]).unwrap();

    // Delete by Wrapper
    let res = akita.remove::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Delete with primary id
    let res = akita.remove_by_id::<User, _>(0).unwrap();

    // Update User property by Wrapper
    let res = akita.update(&User::default(), Wrapper::new().eq("name", "Jack")).unwrap();
    // Update with primary id
    let res = akita.update_by_id(&User::default());

    // The Wrapper to build query condition
    let wrapper = Wrapper::new()
        .select(vec!["id".to_string(), "gender".to_string()])
        .eq("username", "ussd") // username = 'ussd'
        .gt("age", 1) // age > 1
        .lt("age", 10) // age < 10
        .inside("user_type", vec!["admin", "super"]);

    // Select
    // Find all
    let vec: Vec<User> = akita.list::<User>(wrapper.clone()).unwrap();

    // The number of paginated pages starts from subscript one
    let page_no = 1;
    // Number of paginated display
    let page_size = 10;
    let page = akita.page::<User>(page_no, page_size, wrapper.clone()).unwrap();
    // total number of pagination
    let total = page.total;
    // paginated data
    let vec_second = page.records;

    // Get the record count
    let count = akita.count::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Query with original sql
    let user: User = akita.exec_first("select * from t_system_user where name = ? and id = ?", ("Jack", 1)).unwrap();
    // Or
    let user: User = akita.exec_first("select * from t_system_user where name = :name and id = :id", params! {
        "name" => "Jack",
        "id" => 1
    }).unwrap();
    let res = akita.exec_drop("select now()", ()).unwrap();

    // CRUD with Entity
    let model = User::default();
    // update
    let res = model.update_by_id::<_>(&akita).unwrap();
    // delete
    let res = model.delete_by_id::<i32, _>(&akita, 1).unwrap();
    // list
    let list = User::list::<_>(Wrapper::new().eq("name", "Jack"), &akita).unwrap();
    // page
    let page = User::page::<_>(page_no, page_size, Wrapper::new().eq("name", "Jack"), &akita).unwrap();

    // Fast with Akita

    // ...

    // Transaction
    akita.start_transaction().and_then(|mut transaction| {
        // do anything
        transaction.save::<User, i64>(&User {
            pk: 0,
            id: "".to_string(),
            headline: None,
            status: 0,
            level: 0,
            birthday: None,
            gender: 0,
            is_org: false,
            url_token: "".to_string(),
        })?;
        // final commit or rollback
        transaction.commit()
        // transaction.unwrap()?;
    }).unwrap();
}