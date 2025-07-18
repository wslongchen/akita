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

//! This create offers:
//!
//! *   MySql/SQLite database's helper in pure rust;
//! *   A mini orm framework (Just MySQL/SQLite)。
//!
//! Features:
//!
//! *   Other Database support, i.e. support Oracle, MSSQL...;
//! *   support of named parameters for custom condition;
//!
//! ## Installation
//!
//! Put the desired version of the crate into the `dependencies` section of your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! akita = "0.4.0"
//! ```
//!
//! 
//! ## Feature.
//! 
//! * ```akita-mysql``` - to use mysql
//! * ```akita-sqlite``` - to use sqlite
//! 
//! 
//! ## Annotions.
//! * Table - to make Akita work with structs
//! * column - to make struct field with own database.
//! * name - work with column, make the table's field name. default struct' field name.
//! * exist - ignore struct's field with table. default true.
//!
//! ## Support Field Types.
//! 
//! * ```Option<T>```
//! * ```u8, u32, u64```
//! * ```i32, i64```
//! * ```usize```
//! * ```f32, f64```
//! * ```bool```
//! * ```serde_json::Value```
//! * ```str, String```
//! * ```NaiveDate, NaiveDateTime```
//! 
//! ## Example
//! 
//! ```rust
//! # use akita::*;
//! # use chrono::{NaiveDateTime, NaiveDate};
//! # use std::time::Duration;
//! # use once_cell::sync::Lazy;
//! /// Annotion Support: Entity、id、field (name, exist)
//! #[derive(Entity, Clone, Default)]
//! #[table(name = "t_system_user")]
//! pub struct User {
//!     #[id(name = "id")]
//!     pub pk: i64,
//!     pub id: String,
//!     pub headline: Option<NaiveDateTime>,
//!     /// 状态
//!     pub status: u8,
//!     /// 用户等级 0.普通会员 1.VIP会员
//!     pub level: u8,
//!     /// 生日
//!     pub birthday: Option<NaiveDate>,
//!     /// 性别
//!     pub gender: u8,
//!     #[field(exist = "false")]
//!     pub is_org: bool,
//!     #[field(name = "token")]
//!     pub url_token: String,
//! }
//!
//!
//! fn main() {
//!
//! let cfg = AkitaConfig::new(String::from("mysql://root:password@localhost:3306/akita"))
//!         .set_connection_timeout(Duration::from_secs(6))
//!         .set_log_level(LogLevel::Info).set_max_size(6);
//!     let akita = Akita::new(cfg).expect("must be ok");
//!     // The Wrapper to build query condition
//!     let wrapper = Wrapper::new()
//!         .eq("username", "ussd") // username = 'ussd'
//!         .gt("age", 1) //! age > 1
//!         .lt("age", 10) // age < 10
//!         .inside("user_type", vec!["admin", "super"]) // user_type in ('admin', 'super')
//!         .and(|wrapper| { // or
//!             wrapper.like("username", &name)
//!                 .or_direct().like("username", &name)
//!         });
//!     // CRUD with Akita
//!     let insert_id: Option<i32> = akita.save(&User::default()).unwrap();
//!     let _ = akita.save_batch(&[&User::default()]).unwrap();
//!     // Update with wrapper
//!     let res = akita.update(&User::default(), Wrapper::new().eq("name", "Jack")).unwrap();
//!     // Update with primary id
//!     let res = akita.update_by_id(&User::default());
//!     // Query return List
//!     let list: Vec<User> = akita.list(Wrapper::new().eq("name", "Jack")).unwrap();
//!     // Query return Page
//!     let pageNo = 1;
//!     let pageSize = 10;
//!     let page: IPage<User> = akita.page(pageNo, pageSize, Wrapper::new().eq("name", "Jack")).unwrap();
//!     // Remove with wrapper
//!     let res = akita.remove::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
//!     // Remove with primary id
//!     let res = akita.remove_by_id::<User,_>(0).unwrap();
//!     // Get the record count
//!     let count = akita.count::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
//!     // Query with original sql
//!     let user: User = akita.exec_first("select * from t_system_user where name = ? and id = ?", ("Jack", 1)).unwrap();
//!     // Or
//!     let user: User = akita.exec_first("select * from t_system_user where name = :name and id = :id", params! {
//!         "name" => "Jack",
//!         "id" => 1
//!     }).unwrap();
//!     let res = akita.exec_drop("select now()", ()).unwrap();
//!
//!     // Transaction
//!     akita.start_transaction().and_then(|mut transaction| {
//!         let list: Vec<User> = transaction.list(Wrapper::new().eq("name", "Jack"))?;
//!         let insert_id: Option<i32> = transaction.save(&User::default())?;
//!         transaction.commit()
//!     }).unwrap();
//!
//!     // CRUD with Entity
//!     let model = User::default();
//!     // insert
//!     let insert_id = model.insert::<Option<i32>, _>(&akita).unwrap();
//!     // update
//!     let res = model.update_by_id::<_>(&akita).unwrap();
//!     // delete
//!     let res = model.delete_by_id::<i32,_>(&akita, 1).unwrap();
//!     // list
//!     let list = User::list::<_>(Wrapper::new().eq("name", "Jack"), &akita).unwrap();
//!     // page
//!     let page = User::page::<_>(pageNo, pageSize, Wrapper::new().eq("name", "Jack"), &akita).unwrap();
//!
//!     // Fast with sql
//!     pub static AK: Lazy<Akita> = Lazy::new(|| {
//!         let mut cfg = AkitaConfig::new("xxxx".to_string()).set_max_size(5).set_connection_timeout(Duration::from_secs(5)).set_log_level(LogLevel::Info);
//!         Akita::new(cfg).unwrap()
//!     });
//!
//!     #[sql(AK,"select * from user where id = ?")]
//!     fn select_example(id: &str) -> Vec<User> { todo!() }
//!
//!     // or:
//!     #[sql(AK,"select * from user where mch_no = ?")]
//!     fn select_example2(ak: &AKita, id: &str) -> Vec<User> { todo!() }
//! }
//! ```
//! ## API Documentation
//! ## Wrapper
//! ```ignore
//! 
//! let mut wrapper = Wrapper::new().like(true, "column1", "ffff")
//! .eq(true, "column2", 12)
//! .eq(true, "column3", "3333")
//! .inside(true, "column4", vec![1,44,3])
//! .not_between(true, "column5", 2, 8)
//! .set(true, "column1", 4);
//!
//! ```
//! Update At 2021.12.08 10:21
//! By Mr.Pan
//! 
//! 
//! 
mod wrapper;
mod segment;
mod errors;
mod mapper;
mod pool;
mod database;
mod platform;
#[cfg(feature = "akita-auth")]
mod auth;
mod manager;
#[allow(unused)]
mod akita;
mod config;
mod converter;
mod key;


#[doc(inline)]
pub use wrapper::Wrapper;
pub use converter::{*};
pub use key::{IdentifierGenerator};
#[doc(inline)]
pub use database::Platform;
#[doc(inline)]
pub use mapper::{BaseMapper, IPage, AkitaMapper};
#[doc(inline)]
pub use segment::{Segment, AkitaKeyword, ISegment};
#[doc(inline)]
pub use errors::{AkitaError, Result};
pub use config::AkitaConfig;
#[doc(inline)]
pub use pool::{Pool};
#[cfg(feature = "akita-auth")]
pub use auth::*;
pub use akita::*;
#[doc(inline)]
pub use manager::{AkitaEntityManager, AkitaTransaction};
#[doc(inline)]
pub use chrono::{Local, NaiveDate, NaiveDateTime};
// Re-export #[derive(Entity)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[allow(unused_imports)]
#[macro_use]
extern crate akita_derive;
#[doc(hidden)]
pub use akita_derive::*;
pub use akita_core as core;

pub use akita_core::*;

pub use crate::core::{FieldName, FieldType, GetFields, GetTableName, Table, ToValue, FromValue};