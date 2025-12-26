# Akita

<p align="center">
  <img src="http://img.snackcloud.cn/snackcloud/shop/snack_logo.png" alt="Akita Logo" width="200" height="200">
</p>

<p align="center">
  <strong>A lightweight, fast and easy-to-use ORM framework for Rust</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/akita">
    <img src="https://img.shields.io/crates/v/akita.svg" alt="Crates.io">
  </a>
  <a href="https://docs.rs/akita">
    <img src="https://docs.rs/akita/badge.svg" alt="Documentation">
  </a>
  <a href="https://github.com/wslongchen/akita/actions">
    <img src="https://img.shields.io/badge/akita-rustc__1.31%2B-lightgrey" alt="Minimum Rust">
  </a>
  <a href="https://github.com/wslongchen/akita/blob/master/LICENSE-APACHE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License Apache 2.0">
  </a>
  <a href="https://github.com/wslongchen/akita/blob/master/LICENSE-MIT">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License MIT">
  </a>
</p>

## 🎯 Features

- **🚀 High Performance**: Pure Rust implementation, zero runtime overhead
- **🎯 Easy to Use**: Intuitive API, quick to learn
- **🔧 Flexible Query**: Powerful query builder with type safety
- **📦 Multi-Database**: Native support for MySQL, PostgreSQL, SQLite, and any MySQL-compatible databases (TiDB, MariaDB, etc.)
- **🔌 Dual Runtime**: Both synchronous and asynchronous operation modes
- **🛡️ Type Safe**: Full Rust type system support with compile-time checking
- **🔄 Transaction**: Complete ACID transaction management with savepoint support
- **⚡ Connection Pool**: Built-in high-performance connection pooling
- **🎨 Annotation Driven**: Simplify entity definition with derive macros
- **🔌 Interceptors**: Extensible interceptor system for AOP (Aspect-Oriented Programming)
- **📊 Pagination**: Built-in smart pagination with total count
- **🔍 Complex Query**: Support for joins, subqueries, and complex SQL operations
- **🛠️ Raw SQL**: Direct SQL execution when needed

## 📦 Installation

Add this to your `Cargo.toml`:

### For MySQL (Synchronous)
```toml
[dependencies]
akita = { version = "0.6", features = ["mysql-sync"] }
```

### For MySQL (Asynchronous)
```toml
[dependencies]
akita = { version = "0.6", features = ["mysql-async"] }
```

### For PostgreSQL:
```toml
[dependencies]
akita = { version = "0.6", features = ["postgres-sync"] }
```
### For Oracle:
```toml
[dependencies]
akita = { version = "0.6", features = ["oracle-sync"] }
```
### SqlServer:
```toml
[dependencies]
akita = { version = "0.6", features = ["mssql-sync"] }
```
### For SQLite:
```toml
[dependencies]
akita = { version = "0.6", features = ["sqlite-sync"] }
```
### For TiDB and MySQL-compatible Databases:
TiDB, MariaDB, and other MySQL-compatible databases can use the MySQL features:
```toml
[dependencies]
akita = { version = "0.6", features = ["mysql-sync"] }  # or "mysql-async"
chrono = "0.4"
```

## 🚀 Quick Start
### 1. Define Your Entity

```rust
use akita::*;
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value;

#[derive(Entity, Clone, Default, Debug)]
#[table(name = "users")]
pub struct User {
    #[id(name = "id")]
    pub id: i64,
    
    #[field(name = "user_name")]
    pub username: String,
    
    pub email: String,
    
    pub age: Option<u8>,
    
    #[field(name = "is_active")]
    pub active: bool,
    
    pub level: u8,
    
    pub metadata: Option<Value>,
    
    pub birthday: Option<NaiveDate>,
    
    pub created_at: Option<NaiveDateTime>,
    
    #[field(exist = "false")]
    pub full_name: String,
}
```

### 2. Initialize Akita

#### Synchronous Mode

```rust
use akita::prelude::*;
use std::time::Duration;

fn main() -> Result<()> {
    // Configuration for MySQL
    let cfg = AkitaConfig::new()
        .url("mysql://root:password@localhost:3306/mydb")
        .max_size(10)                     // Connection pool size
        .connection_timeout(Duration::from_secs(5));

    // Create Akita instance
    let akita = Akita::new(cfg)?;

    // For TiDB (uses MySQL protocol)
    let tidb_cfg = AkitaConfig::new()
        .url("mysql://root:@tidb-host:4000/mydb")
        .max_size(20);
    Ok(())
}
```

#### Asynchronous Mode
```rust
use akita::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Async configuration
    let cfg = AkitaConfig::new()
        .url("mysql://root:password@localhost:3306/mydb")
        .max_size(10)
        .connection_timeout(Duration::from_secs(5));
    
    // Create async Akita instance
    let akita = Akita::new(cfg).await?;
    
    Ok(())
}
```

### 3. Basic Operations

#### Synchronous Operations

```rust
fn main() {
    // Create
    let user = User {
        username: "john_doe".to_string(),
        email: "john@example.com".to_string(),
        active: true,
        level: 1,
        ..Default::default()
    };

    let user_id: Option<i64> = akita.save(&user)?;

    // Read
    let user: Option<User> = akita.select_by_id(user_id.unwrap())?;

    // Update
    let mut user = user.unwrap();
    user.level = 2;
    akita.update_by_id(&user)?;

    // Delete
    akita.remove_by_id::<User, _>(user_id.unwrap())?;
}
```

#### Asynchronous Operations
```rust
fn main() {
    // Create
    let user = User {
        username: "jane_doe".to_string(),
        email: "jane@example.com".to_string(),
        active: true,
        level: 1,
        ..Default::default()
    };

    let user_id: Option<i64> = akita.save(&user).await?;

    // Read
    let user: Option<User> = akita.select_by_id(user_id.unwrap()).await?;

    // Update
    let mut user = user.unwrap();
    user.level = 2;
    akita.update_by_id(&user).await?;

    // Delete
    akita.remove_by_id::<User, _>(user_id.unwrap()).await?;
}
```


## ⬆️Database Compatibility Matrix

| Database | Sync Feature | Async Feature | Protocol | Sync Implementation | Async Implementation | Status | Notes |
|----------|--------------|---------------|----------|---------------------|----------------------|--------|-------|
| MySQL | `mysql-sync` | `mysql-async` | MySQL | `mysql` crate | `mysql_async` crate | ✅ Production Ready | Native Rust implementations |
| PostgreSQL | `postgres-sync` | `postgres-async` | PostgreSQL | `tokio-postgres` (blocking) | `tokio-postgres` (async) | ✅ Production Ready | Both use tokio-postgres under the hood |
| SQLite | `sqlite-sync` | `sqlite-async` | SQLite | `rusqlite` crate | `sqlx` with async runtime | ✅ Production Ready | Different implementation strategies |
| Oracle | `oracle-sync` | `oracle-async` | Oracle | `oracle` crate (blocking) | `oracle` crate + async runtime | ✅ Production Ready | Oracle driver with async wrapper |
| SQL Server | `sqlserver-sync` | `sqlserver-async` | TDS | `tiberius` (blocking) | `tiberius` (async) | ✅ Production Ready | Tiberius driver support |
| TiDB | `mysql-sync` | `mysql-async` | MySQL | Same as MySQL | Same as MySQL | ✅ Production Ready | 100% MySQL compatible |
| MariaDB | `mysql-sync` | `mysql-async` | MySQL | Same as MySQL | Same as MySQL | ✅ Production Ready | 100% MySQL compatible |
| OceanBase | `mysql-sync` | `mysql-async` | MySQL | Same as MySQL | Same as MySQL | ✅ Production Ready | MySQL compatible mode |

## Implementation Details Summary

### PostgreSQL Implementation
- **Sync**: Uses `tokio-postgres` with blocking wrapper
- **Async**: Direct `tokio-postgres` async client
- **Both share same underlying library**

### Oracle Implementation
- **Sync**: Native `oracle` crate (synchronous driver)
- **Async**: `oracle` crate wrapped with async runtime
- **Same driver, different execution model**

### SQLite Implementation
- **Sync**: `rusqlite` crate (synchronous SQLite)
- **Async**: `sqlx` with async SQLite support
- **Different libraries, same protocol**

### SQL Server Implementation
- **Sync**: `tiberius` with blocking API
- **Async**: `tiberius` native async API
- **Same library, different APIs**

## Feature Comparison

| Feature | MySQL/TiDB | PostgreSQL | SQLite | Oracle | SQL Server |
|---------|-----------|------------|--------|--------|------------|
| ACID Transactions | ✅ | ✅ | ✅ | ✅ | ✅ |
| Connection Pool | ✅ | ✅ | ✅ | ✅ | ✅ |
| Native Async | ✅ | ✅ | ⚠️ (via sqlx) | ⚠️ (wrapped) | ✅ |
| Sync via Async Runtime | ❌ | ✅ (blocking) | ❌ | ❌ | ✅ (blocking) |
| JSON Support | ✅ (JSON) | ✅ (JSONB) | ✅ (JSON1) | ✅ | ⚠️ (limited) |
| Full-text Search | ✅ | ✅ | ✅ (FTS5) | ✅ | ✅ |
| Spatial Data | ✅ | ✅ | ✅ (R*Tree) | ✅ | ✅ |
| Stored Procedures | ✅ | ✅ | ❌ | ✅ | ✅ |
| Replication | ✅ | ✅ | ❌ | ✅ | ✅ |
| Distributed | TiDB ✅ | ❌ | ❌ | ❌ | ❌ |
| Protocol | MySQL | PostgreSQL | SQLite | Oracle | TDS |

## Key Implementation Notes

1. **PostgreSQL**: Both sync and async use `tokio-postgres`, sync is just a blocking wrapper
2. **Oracle**: Sync is native driver, async is wrapper around same driver
3. **SQLite**: Different libraries for sync (`rusqlite`) and async (`sqlx`)
4. **SQL Server**: `tiberius` provides both sync (blocking) and async APIs
5. **MySQL**: Separate sync (`mysql`) and async (`mysql_async`) crates
6. **TiDB/MariaDB/OceanBase**: Use MySQL drivers with full compatibility


## 📚 Detailed Usage

### Query Builder

Akita provides a powerful, type-safe query builder that works across all supported databases:


```rust
fn main() {
    use akita::*;

    // Database-agnostic query builder
    let wrapper = Wrapper::new()
        // Select specific columns
        .select(vec!["id", "username", "email"])

        // Universal conditions (works on all databases)
        .eq("status", 1)
        .ne("deleted", true)
        .gt("age", 18)
        .ge("score", 60)
        .lt("age", 65)
        .le("level", 10)

        // String operations
        .like("username", "%john%")
        .not_like("email", "%test%")

        // List operations
        .r#in("role", vec!["admin", "user"])
        .not_in("status", vec![0, 9])

        // Null checks
        .is_null("deleted_at")
        .is_not_null("created_at")

        // Between
        .between("age", 18, 65)
        .not_between("score", 0, 60)

        // Logical operations
        .and(|w| {
            w.eq("status", 1).or_direct().eq("status", 2)
        })
        .or(|w| {
            w.like("username", "%admin%").like("email", "%admin%")
        })

        // Ordering
        .order_by_asc(vec!["created_at"])
        .order_by_desc(vec!["id", "level"])

        // Grouping
        .group_by(vec!["department", "level"])

        // Having clause (database-specific optimizations)
        .having("COUNT(*)", SqlOperator::Gt, 1)

        // Pagination (optimized for each database)
        .limit(10)
        .offset(20);
}
```

### Complex Queries with Database Optimizations

```rust
fn main() {
    // Join queries with database-specific optimizations
    let users: Vec<User> = akita.list(
        Wrapper::new()
            .eq("u.status", 1)
            .inner_join("departments d", "u.department_id = d.id")
            .select(vec!["u.*", "d.name as department_name"])
    )?;

    // Subqueries (automatically optimized for target database)
    let active_users: Vec<User> = akita.list(
        Wrapper::new()
            .r#in("id", |w| {
                w.select(vec!["user_id"])
                    .from("user_logs")
                    .eq("action", "login")
                    .gt("created_at", "2023-01-01")
            })
    )?;

    // Database-specific optimizations
    let query = Wrapper::new()
        .eq("status", 1)
        .order_by_desc(vec!["created_at"]);

    // For MySQL/TiDB: Uses LIMIT optimization
    // For PostgreSQL: Uses LIMIT OFFSET
    // For SQLite: Uses LIMIT OFFSET
    let result = akita.list::<User>(query.limit(100))?;
}
```

### Database-Specific Features
#### MySQL/TiDB Specific Features
```rust
fn main() {
    // MySQL JSON functions
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users WHERE JSON_EXTRACT(metadata, '$.premium') = true",
        ()
    )?;

    // MySQL full-text search
    let users: Vec<User> = akita.list(
        Wrapper::new()
            .raw("MATCH(username, email) AGAINST(:search IN BOOLEAN MODE)")
            .set_param("search", "john*")
    )?;
}
```

#### PostgreSQL Specific Features
```rust
fn main() {
    // PostgreSQL JSONB operations
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users WHERE metadata @> '{\"premium\": true}'",
        ()
    )?;

    // PostgreSQL array operations
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users WHERE 'admin' = ANY(roles)",
        ()
    )?;
}
```

#### SQLite Specific Features
```rust
fn main() {
    // SQLite JSON1 extension
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users WHERE json_extract(metadata, '$.premium') = 1",
        ()
    )?;

    // SQLite full-text search (FTS5)
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users_fts WHERE users_fts MATCH 'john'",
        ()
    )?;
}
```

### Raw SQL Queries with Database Portability
```rust
fn main() {
    // Parameterized queries
    let users: Vec<User> = akita.exec_raw(
        "SELECT * FROM users WHERE status = ? AND level > ?",
        (1, 0)
    )?;

    // Named parameters
    let user: Option<User> = akita.exec_first(
        "SELECT * FROM users WHERE username = :name AND email = :email",
        params! {
        "name" => "john",
        "email" => "john@example.com"
    }
    )?;

    // Executing DDL
    akita.exec_drop(
        "CREATE TABLE IF NOT EXISTS users (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL
    )",
        ()
    )?;
}
```

### Transactions with Database-Specific Features

```rust
fn main() {
    // Simple transaction
    akita.start_transaction().and_then(|mut tx| {
        tx.save(&user1)?;
        tx.save(&user2)?;
        tx.update(&user3, wrapper)?;
        tx.commit()
    })?;

    // Nested transactions (savepoints)
    akita.start_transaction().and_then(|mut tx| {
        tx.save(&user1)?;


        match tx.save(&user2) {
            Ok(_) => {
                tx.commit()
            }
            Err(e) => {
                // Continue with other operations or rollback
                tx.rollback()
            }
        }
    })?;
}
```

### Interceptors with Database Awareness

Akita supports powerful interceptor system that can adapt to different databases:

```rust
use akita::*;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    // Create interceptor-enabled Akita
    let akita = Akita::new(config).unwrap()
    .with_interceptor_builder(
        InterceptorBuilder::new()
            .register(tenant_interceptor)
            .register(performance_interceptor)
            .register(logging_interceptor)
            .enable("trackable_tenant").unwrap()
            .enable("trackable_performance").unwrap()
            .enable("trackable_logging").unwrap()
    )?;
}

// Custom interceptor
#[derive(Debug)]
struct AuditInterceptor {
    user_id: String,
}

impl AkitaInterceptor for AuditInterceptor {
    fn name(&self) -> &'static str {
        "audit"
    }
    
    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Audit
    }
    
    fn order(&self) -> i32 {
        50
    }
    
    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<()> {
        // Add audit information to query
        ctx.set_metadata("audit_user", self.user_id.clone());
        ctx.set_metadata("audit_time", chrono::Utc::now().to_rfc3339());
        
        Ok(())
    }
}
```

### Entity Methods with Database Portability
Entities can have their own methods:

```rust
impl User {
    // Custom finder methods
    pub fn find_active(akita: &Akita) -> Result<Vec<User>> {
        akita.list(Wrapper::new().eq("status", 1))
    }

    pub fn find_by_email(akita: &Akita, email: &str) -> Result<Option<User>> {
        akita.exec_first(
            "SELECT * FROM users WHERE email = ?",
            (email,)
        )
    }

    // Business logic methods
    pub fn promote(&mut self) {
        self.level += 1;
        // Add other business logic
    }

    pub fn is_vip(&self) -> bool {
        self.level >= 2
    }
}

fn main() {
    // Usage
    let active_users = User::find_active(&akita)?;
    let user = User::find_by_email(&akita, "john@example.com")?;
    let mut user = create_test_user();

    // Testing entity updates
    let result = user.update_by_id::<_>(&akita);
    assert!(result.is_ok(), "The entity update method should succeed");

    // Testing entity deletion
    let result = user.remove_by_id::<_,i32>(&akita, 1);
    assert!(result.is_ok(), "The entity deletion method should succeed");

    // Test the entity list query

    let result = User::list(&akita, Wrapper::new().eq("name", "Jack"));
    assert!(result.is_ok(), "The entity list query should succeed");

    // Testing entity paging queries
    let result = User::page::<_>(&akita, 1, 1, Wrapper::new().eq("name", "Jack"));
    assert!(result.is_ok(), "The entity paging query should succeed");
}
```

### Pagination with Database Optimization

```rust
fn main() {
    // Simple pagination
    let page: IPage<User> = akita.page(1, 10, Wrapper::new().eq("status", 1))?;

    println!("Page {} of {}", page.current, page.size);
    println!("Total records: {}", page.total);
    println!("Records on this page: {}", page.records.len());

    // Complex pagination with custom ordering
    let _page = akita.page(
        1,
        10,
        Wrapper::new()
            .eq("department", "engineering")
            .ge("level", 3)
    )?;

    // Manual pagination
    let ipage = akita.page::<User>(
        1,
        10,
        Wrapper::new().eq("active", true)
    )?;
}
```

### Batch Operations with Database Optimization

```rust
fn main() {
    // Batch insert
    let users = vec![
        User { username: "user1".to_string(), ..Default::default() },
        User { username: "user2".to_string(), ..Default::default() },
        User { username: "user3".to_string(), ..Default::default() },
    ];

    let _ = akita.save_batch(&users)?;

    // Batch update
    let mut users_to_update = vec![];
    for mut user in users {
        user.level += 1;
        users_to_update.push(user);
    }

    akita.update_batch_by_id(&users_to_update)?;

    // Batch delete
    akita.remove_by_ids::<User, _>(vec![1, 2, 3, 4, 5])?;
}
```

## 🔧 Configuration
### AkitaConfig Options
```rust
fn main() {
    let config = AkitaConfig::new().url("mysql://root:password@localhost:3306/mydb")
        .max_size(20)                    // Maximum connection pool size
        .min_size(Some(5))                     // Minimum connection pool size
        .connection_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800));
}
```

### Environment-based Configuration
```rust
fn main() {
    use std::env;

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost:3306/mydb".to_string());

    let max_connections: u32 = env::var("DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let config = AkitaConfig::new().url(&database_url)
        .max_size(max_connections);
}
```

## 🎨 Advanced Features
### Custom Type Conversion
```rust
use akita::*;

#[derive(Debug, Clone)]
pub struct Email(String);

impl FromAkitaValue for Email {
    fn from_value(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Text(s) => Ok(Email(s.clone())),
            _ => Err(AkitaDataError::ConversionError(ConversionError::conversion_error(format!("Cannot convert {:?} to Email", value)))),
        }
    }
}

impl IntoAkitaValue for Email {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::Text(self.0.clone())
    }
}

#[derive(Entity)]
pub struct UserWithEmail {
    pub id: i64,
    pub email: Email,  // Custom type
}
```


## 📊 Performance Tips
1. **Connection Pooling**: Always configure appropriate pool sizes

+ MySQL/TiDB: 10-100 connections based on workload

+ PostgreSQL: 5-50 connections

+ SQLite: 1 connection (file-based)

2. **Batch Operations**: Use database-specific batch methods

+ MySQL: Multi-value INSERT statements

+ PostgreSQL: COPY command for large datasets

+ SQLite: Transactions around batch operations

3. **Query Optimization**:

+ Use EXPLAIN on MySQL/PostgreSQL to analyze query plans

+ SQLite: Use appropriate indexes and avoid expensive operations in WHERE

4. **Statement Caching**: Akita caches prepared statements automatically

+ Reduces parsing overhead on all databases

+ Especially beneficial for repeated queries

5. **Connection Reuse**: Keep connections alive for related operations

+ Reduces connection establishment overhead

+ Maintains session state

6. **Database-Specific Features**:

+ MySQL/TiDB: Use connection compression for remote connections

+ PostgreSQL: Use prepared statements for complex queries

+ SQLite: Enable WAL mode for better concurrency

## 🤝 Contributing
We welcome contributions! Here's how you can help:

1. ***Report Bugs***: Create an issue with database-specific details

2. ***Suggest Features***: Start a discussion about new database support or features

3. ***Submit PRs***: Follow our contributing guide

4. ***Improve Documentation***: Help us make the docs better for all database backends

5. ***Add Database Support***: Implement support for new databases

## Development Setup

```bash
# Clone the repository
git clone https://github.com/wslongchen/akita.git
cd akita

# Run tests for specific databases
cargo test --features mysql-sync
cargo test --features mysql-async
cargo test --features postgres-sync
cargo test --features sqlite-sync
cargo test --features oracle-sync

# Run all tests
cargo test --all-features

# Run examples
cargo run --example basic --features mysql-sync
cargo run --example async-basic --features mysql-async

# Build documentation
cargo doc --open --all-features
```

## 📄 License
Licensed under either of:

+ Apache License, Version 2.0 (LICENSE-APACHE)

+ MIT license (LICENSE-MIT)

at your option.

## 🙏 Acknowledgments

+ Thanks to all contributors who have helped shape Akita

+ Inspired by great ORMs like Diesel, SQLx, and MyBatis

+ Built with ❤️ by the Cat&Dog Lab team

## 📞 Contact

+ Author: Mr.Pan

+ Email: 1049058427@qq.com

+ GitHub: @wslongchen

+ Project: Akita on GitHub

<p align="center"> Made with ❤️ by <a href="https://github.com/wslongchen">Mr.Pan</a> and the Cat&Dog Lab Team </p>