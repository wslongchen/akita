// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This create offers:
//!
//! *   MySql database's helper in pure rust;
//! *   A mini orm framework (Just MySQL)。
//!
//! Features:
//!
//! *   Other Database support, i.e. support SQLite, Oracle, MSSQL...;
//! *   support of original SQL;
//! *   support of named parameters for custom condition;
//!
//! ## Installation
//!
//! Put the desired version of the crate into the `dependencies` section of your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! akita = "*"
//! ```
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
//! use akita::prelude::*;
//! 
//! /// Annotion Support: Table、table_id、field (name, exist)
//! #[derive(Debug, FromAkita, ToAkita, Table, Clone)]
//! #[table(name="t_system_user")]
//! struct SystemUser {
//!     #[field = "name"]
//!     id: Option<i32>,
//!     #[table_id]
//!     username: String,
//!     #[field(name="ages", exist = "false")]
//!     age: i32,
//! }
//! 
//! fn main() {
//!     let db_url = String::from("mysql://root:password@localhost:3306/akita");
//!     let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
//!     let mut em = pool.entity_manager().expect("must be ok");
//!     let mut wrap = UpdateWrapper::new();
//!     wrap.eq(true, "username", "'ussd'");
//!     match em.count::<SystemUser, UpdateWrapper>(&mut wrap) {
//!         Ok(res) => {
//!             println!("success count data!");
//!         }
//!         Err(err) => {
//!             println!("error:{:?}",err);
//!         }
//!     }
//! }
//! 
//! ```
//! ## API Documentation
//! ## Wrapper
//! ```ignore
//! 
//! let mut wrapper = UpdateWrapper::new();
//! wrapper.like(true, "column1", "ffff");
//! wrapper.eq(true, "column2", 12);
//! wrapper.eq(true, "column3", "3333");
//! wrapper.in_(true, "column4", vec![1,44,3]);
//! wrapper.not_between(true, "column5", 2, 8);
//! wrapper.set(true, "column1", 4);
//! match wrapper.get_target_sql("t_user") {
//!     Ok(sql) => {println!("ok:{}", sql);}
//!     Err(err) => {println!("err:{}", err);}
//! }
//! ```
//! Update At 2021.08.04 10:21 
//! By Mr.Pan
//! 
//! 
//! 
#![feature(map_first_last)]
#[allow(unused)]
mod comm;
mod wrapper;
mod segment;
mod errors;
mod mapper;
mod mysql;
mod pool;
mod information;
mod value;
mod types;
mod database;
mod data;
mod manager;

#[doc(inline)]
pub use wrapper::{QueryWrapper, UpdateWrapper, Wrapper};
#[doc(inline)]
pub use mapper::{BaseMapper, IPage, AkitaMapper};
#[doc(inline)]
pub use segment::SqlSegment;
#[doc(inline)]
pub use errors::AkitaError;
#[doc(inline)]
pub use value::*;
#[doc(inline)]
pub use pool::{AkitaConfig, LogLevel, Pool};
#[doc(inline)]
pub use data::*;
#[doc(inline)]
pub use information::{GetTableName, TableName, FieldName, GetFields, FieldType};
#[doc(inline)]
pub use manager::{AkitaEntityManager, AkitaManager};
pub use crate as akita;

pub mod prelude {
    #[doc(inline)]
    pub use chrono::{Local, NaiveDate, NaiveDateTime};
}

// Re-export #[derive(Table)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[allow(unused_imports)]
#[macro_use]
extern crate akita_derive;
#[doc(hidden)]
pub use akita_derive::*;

#[macro_use]
extern crate log;