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
use std::{convert::{TryFrom}, usize};
use mysql::{Conn, Row, Transaction};
use prelude::{PooledConn, r2d2Pool};
use wrapper::{UpdateWrapper, Wrapper};
use errors::AkitaError;
pub mod prelude;
mod comm;
mod wrapper;
mod segment;
mod errors;



#[derive(Clone)]
pub struct IPage <T> 
    where T: Sized + Clone {
    pub total: usize,
    pub size: usize,
    pub current: usize,
    pub records: Vec<T>
}

impl <T> IPage <T> 
where T: Sized + Clone{
    pub fn new(current: usize, size: usize, total: usize, records: Vec<T>) -> Self {
        Self {
            total,
            size,
            current,
            records,
        }
    }

    pub fn offset(&self) -> usize {
        if self.current > 0 { (self.current - 1) * self.size } else { 0 }
    }
}

pub enum ConnMut<'c, 't, 'tc> {
    Mut(&'c mut Conn),
    TxMut(&'t mut Transaction<'tc>),
    Owned(Conn),
    Pooled(mysql::PooledConn),
    R2d2Polled(PooledConn)
}

impl From<Conn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: Conn) -> Self {
        ConnMut::Owned(conn)
    }
}

impl From<mysql::PooledConn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: mysql::PooledConn) -> Self {
        ConnMut::Pooled(conn)
    }
}

impl From<PooledConn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: PooledConn) -> Self {
        ConnMut::R2d2Polled(conn)
    }
}

impl<'a> From<&'a mut Conn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut Conn) -> Self {
        ConnMut::Mut(conn)
    }
}

impl<'a> From<&'a mut mysql::PooledConn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut mysql::PooledConn) -> Self {
        ConnMut::Mut(conn.as_mut())
    }
}

impl<'t, 'tc> From<&'t mut Transaction<'tc>> for ConnMut<'static, 't, 'tc> {
    fn from(tx: &'t mut Transaction<'tc>) -> Self {
        ConnMut::TxMut(tx)
    }
}


impl TryFrom<&mysql::Pool> for ConnMut<'static, 'static, 'static> {
    type Error = mysql::Error;

    fn try_from(pool: &mysql::Pool) -> Result<Self, Self::Error> {
        pool.get_conn().map(From::from)
    }
}

impl TryFrom<&r2d2Pool> for ConnMut<'static, 'static, 'static> {
    type Error = r2d2::Error;

    fn try_from(pool: &r2d2Pool) -> Result<Self, Self::Error> {
        pool.get().map(From::from)
    }
}



pub trait BaseMapper{
    type Item;
    /// Insert Data.
    fn insert<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<u64>, AkitaError>;

    /// Update Data With Wrapper.
    fn update<'a, 'b, 'c>(&self, wrapper: &mut UpdateWrapper, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    fn list<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Vec<Self::Item>, AkitaError> where Self::Item: Clone;

    fn page<'a, 'b, 'c, W: Wrapper>(&self, page: usize, size: usize, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<IPage<Self::Item>, AkitaError> where Self::Item: Clone;

    /// Find One With Wrapper.
    fn find_one<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError>;

    /// Find Data With Table's Ident.
    fn find_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError>;

    /// Update Data With Table's Ident.
    fn update_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Delete Data With Wrapper.
    fn delete<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Delete Data With Table's Ident.
    fn delete_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Get the Table Fields.
    fn get_table_fields(&self) -> Result<String, AkitaError>;

    /// Get Table Idents.
    fn get_table_idents(&self) -> Result<String, AkitaError>;

    /// Get Condition Fields.
    fn get_update_fields(&self, set_sql: Option<String>) -> Result<String, AkitaError>;

    /// Get Table Name.
    fn get_table_name(&self) -> Result<String, AkitaError>;
}

pub trait FromRowExt {
    fn from_long_row(row: mysql::Row) -> Self;
    fn from_long_row_opt(row: mysql::Row) -> Result<Self, mysql::FromRowError>
    where
        Self: Sized;
}

#[inline]
#[allow(unused)]
pub fn from_long_row<T: FromRowExt>(row: Row) -> T {
    FromRowExt::from_long_row(row)
}


// Re-export #[derive(Serialize, Deserialize)].
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