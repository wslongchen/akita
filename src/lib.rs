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
//! 
//! ## Example
//! 
//! ```rust
//! use akita::prelude::*;
//! 
//! 
//! /// Annotion Support: Table、Id、column
//! #[table(name="t_system_user")]
//! #[derive(Table, Clone)]
//! struct User{
//!     #[Id(name="id")]
//!     pub id: i32,
//!     #[column(name="username")]
//!     pub username: String,
//!     #[column]
//!     pub mobile: String,
//!     #[column]
//!     pub password: String,
//! }
//! 
//! // use r2d2 pool
//! let opts = Opts::from_url("mysql://root:127.0.0.1:3306/test").expect("database url is empty.");
//! let pool = Pool::builder().max_size(4).build(MysqlConnectionManager::new(OptsBuilder::from_opts(opts))).unwrap();
//! let mut conn = pool.get().unwrap();
//! 
//! /// build the wrapper.
//! let mut wrapper = UpdateWrapper::new()
//!     .like(true, "username", "ffff");
//!     .eq(true, "username", 12);
//!     .eq(true, "username", "3333");
//!     .in_(true, "username", vec![1,44,3]);
//!     .not_between(true, "username", 2, 8);
//!     .set(true, "username", 4);
//! 
//! let user = User{
//!     id: 2,
//!     username: "username".to_string(),
//!     mobile: "mobile".to_string(),
//!     password: "password".to_string()
//! };
//! 
//! // Transaction
//! conn.start_transaction(TxOpts::default()).map(|mut transaction| {
//!     match user.update( & mut wrapper, &mut ConnMut::TxMut(&mut transaction)) {
//!         Ok(res) => {}
//!         Err(err) => {
//!             println!("error : {:?}", err);
//!         }
//!     }
//! });
//!
//! let mut pool = ConnMut::R2d2Polled(conn);
//! /// update by identify
//! match user.update_by_id(&mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! /// delete by identify
//! match user.delete_by_id(&mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! /// delete by condition
//! match user.delete:: < UpdateWrapper > ( & mut wrapper, &mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! /// insert data
//! match user.insert(&mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! /// find by identify
//! match user.find_by_id(&mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! 
//! /// find one by condition
//! match user.find_one::<UpdateWrapper>(&mut wrapper, &mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
//!     }
//! }
//! 
//! /// find page by condition
//! match user.page::<UpdateWrapper>(1, 10,&mut wrapper, &mut conn) {
//!     Ok(res) => {}
//!     Err(err) => {
//!         println!("error : {:?}", err);
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
//! ```
//! Update At 2021.07.09 13:21 
//! By Mr.Pan
//! 
//! 
//! 
mod comm;
mod wrapper;
mod segment;
mod errors;
mod mapper;
mod mysql;

#[doc(inline)]
pub use wrapper::{QueryWrapper, UpdateWrapper, Wrapper};
#[doc(inline)]
pub use mapper::{BaseMapper, IPage, ConnMut};
#[doc(inline)]
pub use segment::SqlSegment;
#[doc(inline)]
pub use errors::AkitaError;
#[doc(inline)]
pub use crate::mysql::{FromRowExt, from_long_row};
#[cfg(feature = "r2d2_pool")]
pub use crate::mysql::{r2d2Pool, PooledConn, new_pool};

pub mod prelude {
    #[doc(inline)]
    pub use mysql::{params, prelude::*};
    #[doc(inline)]
    pub use mysql::prelude::Queryable;
    #[doc(inline)]
    pub use mysql::error::Error;
    #[doc(inline)]
    pub use mysql::{Conn, Opts, OptsBuilder};
}

// Re-export #[derive(Table)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[cfg(feature = "akita_derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate akita_derive;
#[cfg(feature = "akita_derive")]
#[doc(hidden)]
pub use akita_derive::*;