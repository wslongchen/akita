#![allow(unused_imports,unreachable_patterns,dead_code,missing_docs, incomplete_features,unused_variables)]
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
//!
//! Add this to your `Cargo.toml`:
//! 
//! ```toml
//! [dependencies]
//! akita = { version = "0.6", features = ["mysql-sync"] }
//! chrono = "0.4"
//! ```
//! 
//! For SQLite support:
//! ```toml
//! [dependencies]
//! akita = { version = "0.4", features = ["sqlite-sync"] }
//! ```
//! 
//! ## 🚀 Quick Start
//! ### 1. Define Your Entity
//! 
//! ```rust
//! use akita::prelude::*;
//! use chrono::{NaiveDate, NaiveDateTime};
//! use serde_json::Value;
//! 
//! #[derive(Entity, Clone, Default, Debug)]
//! #[table(name = "users")]
//! pub struct User {
//!     #[id(name = "id")]
//!     pub id: i64,
//!     
//!     #[field(name = "user_name")]
//!     pub username: String,
//!     
//!     pub email: String,
//!     
//!     pub age: Option<u8>,
//!     
//!     #[field(name = "is_active")]
//!     pub active: bool,
//!     
//!     pub level: u8,
//!     
//!     pub metadata: Option<Value>,
//!     
//!     pub birthday: Option<NaiveDate>,
//!     
//!     pub created_at: Option<NaiveDateTime>,
//!     
//!     #[field(exist = "false")]
//!     pub full_name: String,
//! }
//! ```
//! 
//! ### 2. Initialize Akita
//! 
//! ```rust
//! use akita::prelude::*;
//! use std::time::Duration;
//!
//! async fn main() -> Result<(), AkitaError> {
//!     // Configuration
//!     let cfg = AkitaConfig::new().url("mysql://!root:password@localhost:3306/mydb")
//!         .max_size(10)                     //! Connection pool size
//!         .connection_timeout(Duration::from_secs(5));
//!     
//!     // Create Akita instance
//!     let akita = Akita::new(cfg)?;
//!     
//!     Ok(())
//! }
//! ```
//! 
//! ### 3. Basic Operations
//! 
//! ```rust
//! // Create
//! let user = User {
//! username: "john_doe".to_string(),
//! email: "john@example.com".to_string(),
//! active: true,
//! level: 1,
//! ..Default::default()
//! };
//! 
//! let user_id: Option<i64> = akita.save(&user)?;
//! 
//! // Read
//! let user: Option<User> = akita.select_by_id(user_id.unwrap())?;
//! 
//! // Update
//! let mut user = user.unwrap();
//! user.level = 2;
//! akita.update_by_id(&user)?;
//! 
//! // Delete
//! akita.remove_by_id::<User, _>(user_id.unwrap())?;
//! ```
//! 
//! ## 📚 Detailed Usage
//! ### Query Builder
//! 
//! Akita provides a powerful, type-safe query builder:
//! ```rust
//! use akita::prelude::*;
//! 
//! let wrapper = Wrapper::new()
//!     // Select specific columns
//!     .select(vec!["id", "username", "email"])
//!     
//!     // Conditions
//!     .eq("status", 1)
//!     .ne("deleted", true)
//!     .gt("age", 18)
//!     .ge("score", 60)
//!     .lt("age", 65)
//!     .le("level", 10)
//!     
//!     // String operations
//!     .like("username", "%john%")
//!     .not_like("email", "%test%")
//!     
//!     // List operations
//!     .r#in("role", vec!["admin", "user"])
//!     .not_in("status", vec![0, 9])
//!     
//!     // Null checks
//!     .is_null("deleted_at")
//!     .is_not_null("created_at")
//!     
//!     // Between
//!     .between("age", 18, 65)
//!     .not_between("score", 0, 60)
//!     
//!     // Logical operations
//!     .and(|w| {
//!         w.eq("status", 1).or_direct().eq("status", 2)
//!     })
//!     .or(|w| {
//!         w.like("username", "%admin%").like("email", "%admin%")
//!     })
//!     
//!     // Ordering
//!     .order_by_asc(vec!["created_at"])
//!     .order_by_desc(vec!["id", "level"])
//!     
//!     // Grouping
//!     .group_by(vec!["department", "level"])
//!     
//!     // Having clause
//!     .having("COUNT(*)", SqlOperator::Gt, 1)
//!     
//!     // Pagination
//!     .limit(10)
//!     .offset(20);
//! ```
//! 
//! ### Complex Queries
//! ```rust
//! // Join queries
//! let users: Vec<User> = akita.list(
//!     Wrapper::new()
//!         .eq("u.status", 1)
//!         .inner_join("departments d","u.department_id = d.id")
//!         .select(vec!["u.*", "d.name as department_name"])
//! )?;
//! 
//! // Subqueries
//! let active_users: Vec<User> = akita.list(
//!     Wrapper::new()
//!         .r#in("id", |w| {
//!             w.select(vec!["user_id"])
//!              .from("user_logs")
//!              .eq("action", "login")
//!              .gt("created_at", "2023-01-01")
//!         })
//! )?;
//! ```
//! 
//! ### Raw SQL Queries
//! ```rust
//! // Parameterized queries
//! let users: Vec<User> = akita.exec_raw(
//!     "SELECT * FROM users WHERE status = ? AND level > ?",
//!     (1, 0)
//! )?;
//! 
//! // Named parameters
//! let user: Option<User> = akita.exec_first(
//!     "SELECT * FROM users WHERE username = :name AND email = :email",
//!     params! {
//!         "name" => "john",
//!         "email" => "john@example.com"
//!     }
//! )?;
//! 
//! // Executing DDL
//! akita.exec_drop(
//!     "CREATE TABLE IF NOT EXISTS users (
//!         id BIGINT PRIMARY KEY AUTO_INCREMENT,
//!         username VARCHAR(50) NOT NULL,
//!         email VARCHAR(100) NOT NULL
//!     )",
//!     ()
//! )?;
//! ```
//! Update At 2025.12.13 12:13
//! By Mr.Pan
//! 
//! 
//!

// Common core module
mod errors;
mod config;
mod converter;
mod key;
mod xml;
mod comm;
mod ext;
mod interceptor;
mod mapper;
mod transaction;
mod driver;
mod core;
mod sql;
mod pool;
#[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))]
mod repository;

pub mod prelude;
