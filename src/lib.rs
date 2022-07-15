// Copyright (c) 2021 akita contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

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
//! akita = "0.3.0"
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
//! use akita::*;
//! use chrono::{NaiveDateTime, NaiveDate};
//!
//! /// Annotion Support: AkitaTable、table_id、field (name, exist)
//! #[derive(AkitaTable, Clone, Default)]
//! #[table(name = "t_system_user")]
//! pub struct User {
//!     #[table_id(name = "id")]
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
//!     use std::time::Duration;
//! let db_url = String::from("mysql://root:password@localhost:3306/akita");
//!     let cfg = AkitaConfig::new(db_url).set_connection_timeout(Duration::from_secs(6))
//!         .set_log_level(LogLevel::Debug).set_max_size(6);
//!     let mut pool = Pool::new(cfg).expect("must be ok");
//!     let mut entity_manager = pool.entity_manager().expect("must be ok");
//!     // The Wrapper to build query condition
//!     let wrapper = Wrapper::new()
//!         .eq("username", "ussd") // username = 'ussd'
//!         .gt("age", 1) // age > 1
//!         .lt("age", 10) // age < 10
//!         .inside("user_type", vec!["admin", "super"]); // user_type in ('admin', 'super')
//!     // CRUD with EntityManager
//!    let insert_id: Option<i32> = entity_manager.save(&User::default()).unwrap();
//!    let insert_ids= entity_manager.save_batch(&[&User::default()]).unwrap();
//!    // Update with wrapper
//!    let res = entity_manager.update(&User::default(), Wrapper::new().eq("name", "Jack")).unwrap();
//!    // Update with primary id
//!    let res = entity_manager.update_by_id(&User::default());
//!    // Query return List
//!    let list: Vec<User> = entity_manager.list(Wrapper::new().eq("name", "Jack")).unwrap();
//!    // Query return Page
//!    let pageNo = 1;
//!    let pageSize = 10;
//!    let page: IPage<User> = entity_manager.page(pageNo, pageSize, Wrapper::new().eq("name", "Jack")).unwrap();
//!    // Remove with wrapper
//!    let res = entity_manager.remove::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
//!    // Remove with primary id
//!    let res = entity_manager.remove_by_id::<User,_>(0).unwrap();
//!    // Get the record count
//!    let count = entity_manager.count::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
//!    // Query with original sql
//!    let user: User = entity_manager.execute_first("select * from t_system_user where name = ? and id = ?", ("Jack", 1)).unwrap();
//!    // Or
//!    let user: User = entity_manager.execute_first("select * from t_system_user where name = :name and id = :id", params! {
//!        "name" => "Jack",
//!        "id" => 1
//!    }).unwrap();
//!    let res = entity_manager.execute_drop("select now()", ()).unwrap();
//!
//!     // CRUD with Entity
//!    let model = User::default();
//!    // insert
//!    let insert_id = model.insert::<Option<i32>, _>(&mut entity_manager).unwrap();
//!    // update
//!    let res = model.update_by_id::<_>(&mut entity_manager).unwrap();
//!    // delete
//!    let res = model.delete_by_id::<i32,_>(&mut entity_manager, 1).unwrap();
//!    // list
//!    let list = User::list::<_>(Wrapper::new().eq("name", "Jack"), &mut entity_manager).unwrap();
//!    // page
//!    let page = User::page::<_>(pageNo, pageSize, Wrapper::new().eq("name", "Jack"), &mut entity_manager).unwrap();
//!
//!
//!
//!     // Transaction
//!     let result = entity_manager.start_transaction().and_then(|mut transaction| {
//!         let list: Vec<User> = transaction.list(Wrapper::new().eq("name", "Jack"))?;
//!         let insert_id: Option<i32> = transaction.save(&User::default())?;
//!         transaction.commit()
//!     }).unwrap();
//!
//!
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
#[cfg(feature = "akita-fuse")]
mod fuse;
mod akita;


#[doc(inline)]
pub use wrapper::Wrapper;
#[doc(inline)]
pub use database::Platform;
#[doc(inline)]
pub use mapper::{BaseMapper, IPage, AkitaMapper};
#[doc(inline)]
pub use segment::{Segment, AkitaKeyword, ISegment};
#[doc(inline)]
pub use errors::AkitaError;
#[doc(inline)]
pub use pool::{AkitaConfig, LogLevel, Pool};
#[cfg(feature = "akita-auth")]
pub use auth::*;
#[cfg(feature = "akita-fuse")]
pub use fuse::*;
pub use akita::*;
#[doc(inline)]
pub use manager::{AkitaEntityManager};
#[doc(inline)]
pub use chrono::{Local, NaiveDate, NaiveDateTime};
// Re-export #[derive(AkitaTable)].
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

pub use akita_core::*;

#[cfg(feature = "akita-logging")]
extern crate log;
#[cfg(feature = "akita-tracing")]
extern crate tracing;