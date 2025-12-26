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

mod common;

use akita::prelude::*;
use common::*;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

// Use the existing create_test_akita function
pub fn init_test_db() -> Result<Akita, AkitaError> {
    let cfg = create_test_akita_cfg();
    let builder = InterceptorBuilder::new()
        .register(Arc::new(LoggingInterceptor::new()))
        .enable("logging").unwrap();

    let chain = builder.build().unwrap();
    Ok(Akita::new(cfg).unwrap().with_interceptor_chain(chain))
}

// Create the test XML file
fn create_test_xml_file() -> NamedTempFile {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<sqls>
<!-- 用户相关 SQL -->
<sql id="user.insert" description="插入用户">
    INSERT INTO t_system_user (pk, user_type, name, headline, tenant_id, status, level, age, birthday, gender, token) 
    VALUES (:pk, :user_type, :name, :headline, :tenant_id, :status, :level, :age, :birthday, :gender, :token)
</sql>

<sql id="user.select_by_id" description="根据ID查询用户">
    SELECT * FROM t_system_user WHERE id = :id
</sql>

<sql id="user.select_all" description="查询所有用户">
    SELECT id, pk, user_type, name, headline, tenant_id, status, level, age, birthday, gender, token 
    FROM t_system_user ORDER BY id
</sql>

<sql id="user.update" description="更新用户">
    UPDATE t_system_user 
    SET name = :name, status = :status, level = :level 
    WHERE id = :id
</sql>

<sql id="user.delete" description="删除用户">
    DELETE FROM t_system_user WHERE id = :id
</sql>

<sql id="user.count" description="统计用户数">
    SELECT COUNT(*) as count FROM t_system_user
</sql>

<!-- 使用位置参数的 SQL -->
<sql id="user.select_by_status">
    SELECT * FROM t_system_user WHERE status = ? ORDER BY id limit 1
</sql>

<!-- 复杂查询 -->
<sql id="user.search" description="搜索用户">
    SELECT * FROM t_system_user 
    WHERE (name LIKE :keyword OR token LIKE :keyword)
    AND status = :status
    AND level = :level
    ORDER BY id DESC
</sql>
</sqls>"#;

    let mut file = NamedTempFile::new().unwrap();
    write!(file, "{}", xml_content).unwrap();
    file
}

// Test 1: Basic XML SQL loading
#[test]
fn test_xml_sql_loader_basic() -> Result<(), AkitaError> {
    let xml_file = create_test_xml_file();
    let path = xml_file.path().to_str().unwrap();

    let mut loader = XmlSqlLoader::default();

    // Test loading SQL
    let insert_sql = loader.load_sql(path, "user.insert")?;
    assert!(insert_sql.contains("INSERT INTO t_system_user"));
    assert!(insert_sql.contains(":pk"));
    assert!(insert_sql.contains(":user_type"));

    let select_sql = loader.load_sql(path, "user.select_by_id")?;
    assert!(select_sql.contains("SELECT * FROM t_system_user"));
    assert!(select_sql.contains(":id"));

    // The test fetches all SQL ids
    let ids = loader.load_all_sql(path)?;
    assert!(ids.contains_key(&"user.insert".to_string()));
    assert!(ids.contains_key(&"user.select_by_id".to_string()));
    assert!(ids.contains_key(&"user.select_by_status".to_string()));

    Ok(())
}

// Test 2: SQL macro and XML integration
#[test]
fn test_sql_macro_with_xml() -> Result<(), AkitaError> {
    let xml_file = create_test_xml_file();
    let path = xml_file.path().to_str().unwrap();
    let mut akita = init_test_db()?;
    // Since we cannot dynamically change the file path in the test, the actual path simulation is used here
    // In practice, this can be defined as follows:
    // #[sql_xml("path/to/mapper.xml", "getRecordById", param_style = "named")]
    // fn get_user_by_id(id: i64) -> Result<Option<User>> {}

    // To test, we directly test the logic that loads and executes
    let mut loader = XmlSqlLoader::default();
    let sql = loader.load_sql(path, "user.insert")?;

    // Creating a test user
    let test_user = create_test_user();

    // Preparing parameters
    let mut params = IndexMap::new();
    params.insert("pk".to_string(), test_user.pk.clone().into_value());
    params.insert("user_type".to_string(), test_user.user_type.clone().into_value());
    params.insert("name".to_string(), test_user.name.clone().into_value());
    params.insert("headline".to_string(), test_user.headline.unwrap().into_value());
    params.insert("tenant_id".to_string(), test_user.tenant_id.into_value());
    params.insert("status".to_string(), test_user.status.into_value());
    params.insert("level".to_string(), test_user.level.into_value());
    params.insert("age".to_string(), test_user.age.into_value());
    params.insert("birthday".to_string(), test_user.birthday.unwrap().into_value());
    params.insert("gender".to_string(), test_user.gender.into_value());
    params.insert("token".to_string(), test_user.url_token.clone().into_value());

    let result = akita.exec_drop(&sql, Params::Named(params));
    assert!(result.is_ok());

    Ok(())
}

// Test 3: Use of various SQL macros
#[test]
fn test_various_sql_macros() -> Result<(), AkitaError> {
    let akita = init_test_db()?;
    struct Repository<'a> {
        akita: &'a Akita
    }
    
    impl <'a> Repository<'a> {
        // Testing the insert macro
        #[insert("INSERT INTO t_system_user (pk, name, status, token) VALUES (?, ?, ?, ?)")]
        fn insert_user_direct(&self, pk: &str, name: &str, status: u8, token: &str) -> Result<u64, AkitaError> {
        }

        // Test the select_one macro
        #[select_one("SELECT * FROM t_system_user WHERE id = ?")]
        fn get_user_by_id(&self, id: i64) -> Result<Option<User>, AkitaError> {
        }

        // Test the select_many macro
        #[list("SELECT * FROM t_system_user ORDER BY id limit 10")]
        fn get_all_users(&self) -> Result<Vec<User>, AkitaError> {
        }

        // Testing the update macro
        #[update("UPDATE t_system_user SET status = ? WHERE id = ?")]
        fn update_user_status(&self, status: u8, id: i64) -> Result<u64, AkitaError> {
        }

        // Testing the delete macro
        #[delete("DELETE FROM t_system_user WHERE id = ?")]
        fn delete_user(&self, id: i64) -> Result<u64, AkitaError> {
        }
    }
    let repo = Repository {
        akita: &akita
    };

    // Executing tests
    let test_user = create_test_user();
    let user_id = repo.insert_user_direct(
        &test_user.pk,
        &test_user.name.clone().unwrap_or_default(),
        test_user.status,
        &test_user.url_token
    )?;
    assert!(user_id > 0);

    let user = repo.get_user_by_id(user_id as i64)?;
    assert!(user.is_some());
    assert_eq!(user.as_ref().unwrap().pk, test_user.pk);

    let new_status = 2;
    let updated = repo.update_user_status(new_status, user_id as i64)?;
    assert_eq!(updated, 1);

    let updated_user = repo.get_user_by_id(user_id as i64)?.unwrap();
    assert_eq!(updated_user.status, new_status);

    let all_users = repo.get_all_users()?;
    assert!(!all_users.is_empty());

    let deleted = repo.delete_user(user_id as i64)?;
    assert_eq!(deleted, 1);

    let deleted_user = repo.get_user_by_id(user_id as i64)?;
    assert!(deleted_user.is_none());

    Ok(())
}

// Test 4: Named parameter support
#[test]
fn test_named_parameters() -> Result<(), AkitaError> {
    let akita = init_test_db()?;
    struct Repository<'a> {
        akita: &'a Akita
    }
    let repo = Repository {
        akita: &akita
    };
    impl <'a> Repository<'a> {
        // Macros with named parameters
        #[sql("INSERT INTO t_system_user (pk, name, status, token) VALUES (:pk, :name, :status, :token)")]
        fn insert_user_named(&self,pk: &str, name: &str, status: u8,  token: &str) -> Result<u64, AkitaError> {
        }

        #[sql("SELECT * FROM t_system_user WHERE name LIKE :name AND status = :status AND level = :level")]
        fn find_users(&self, name: &str, status: u8, level: Priority) -> Result<Vec<User>, AkitaError> {
        }   
    }

    let test_user = create_test_user();
    let user_id = repo.insert_user_named(
        &test_user.pk,
        &test_user.name.clone().unwrap_or_default(),
        test_user.status,
        &test_user.url_token
    )?;
    assert!(user_id > 0);

    // Test queries
    let users = repo.find_users("%NAME%", 1, Priority::Medium)?;
    assert!(!users.is_empty());

    Ok(())
}

// Test 5: Explicitly passing akita parameters
#[test]
fn test_explicit_akita_parameter() -> Result<(), AkitaError> {
    let mut akita = init_test_db()?;
    #[sql(akita, "INSERT INTO t_system_user (pk, name, token) VALUES (?, ?, ?)")]
    fn insert_with_akita(akita: &Akita, pk: &str, name: &str, token: &str) -> Result<u64, AkitaError> {
        // Macro generation code
    }

    let test_user = create_test_user();
    let user_id = insert_with_akita(
        &mut akita,
        &test_user.pk,
        &test_user.name.clone().unwrap_or_default(),
        &test_user.url_token
    )?;
    assert!(user_id > 0);

    // Validate insertion
    let rows = akita.exec_raw::<User, _, _>(
        "SELECT * FROM t_system_user WHERE id = ?",
        Params::Positional(vec![user_id.into_value()]),
    )?;

    let user = rows.iter().next().unwrap();
    assert_eq!(user.pk, test_user.pk);
    assert_eq!(user.url_token, test_user.url_token);

    Ok(())
}

// Test 6: Caching functionality of XML SQL loader
#[test]
fn test_xml_sql_loader_cache() -> Result<(), AkitaError> {
    let xml_file = create_test_xml_file();
    let path = xml_file.path().to_str().unwrap();

    let config = XmlSqlLoaderConfig {
        auto_reload: false,
        parameter_detection: true,
        sql_formatting: false,
    };

    let mut loader = XmlSqlLoader::new(config);

    // First load
    let sql1 = loader.load_sql(path, "user.insert")?;

    // Second load (should be read from cache)
    let sql2 = loader.load_sql(path, "user.insert")?;

    assert_eq!(sql1, sql2);

    // Testing cache Clearing
    loader.clear_file_cache(path);
    let sql3 = loader.load_sql(path, "user.insert")?;
    assert_eq!(sql1, sql3);

    Ok(())
}

// Test 7: SQL formatting functionality
#[test]
fn test_sql_formatting() {
    let unformatted_sql = "SELECT * FROM t_system_user WHERE id = ? AND status = ? ORDER BY created_at DESC";

    let config = XmlSqlLoaderConfig {
        sql_formatting: true,
        ..Default::default()
    };

    let loader = XmlSqlLoader::new(config);
    let formatted = loader.format_sql(unformatted_sql);

    // Formatting should preserve SQL semantics
    assert!(formatted.contains("SELECT * FROM t_system_user"));
    assert!(formatted.contains("WHERE id = ?"));

    // Testing complex SQL formatting
    let complex_sql = "SELECT u.id, u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' GROUP BY u.id HAVING COUNT(o.id) > 0 ORDER BY order_count DESC";

    let formatted_complex = loader.format_sql(complex_sql);
    println!("Formatted SQL:\n{}", formatted_complex);
}

// Test 8: Parameter replacement functionality
#[test]
fn test_parameter_replacement() -> Result<(), AkitaError> {
    let config = XmlSqlLoaderConfig::default();
    let loader = XmlSqlLoader::new(config);

    // Test named argument replacement
    let sql = "SELECT * FROM t_system_user WHERE id = :id AND status = :status";
    let mut params = HashMap::new();
    params.insert("id", SqlParameter::Number("123".to_string()));
    params.insert("status", SqlParameter::Text("1".to_string()));

    let replaced = loader.replace_sql_parameters(sql, &params)?;
    assert_eq!(replaced, "SELECT * FROM t_system_user WHERE id = 123 AND status = '1'");

    // Testing for missing parameters
    let mut missing_params = HashMap::new();
    missing_params.insert("id", SqlParameter::Number("123".to_string()));

    let result = loader.replace_sql_parameters(sql, &missing_params);
    assert!(result.is_err());

    // Test position parameter (should remain unchanged)
    let positional_sql = "SELECT * FROM t_system_user WHERE id = ?";
    let positional_params = HashMap::new();
    let positional_result = loader.replace_sql_parameters(positional_sql, &positional_params)?;
    assert_eq!(positional_result, positional_sql);

    Ok(())
}

// Test 9: Error handling
#[test]
fn test_error_handling() -> Result<(), AkitaError> {
    let xml_file = create_test_xml_file();
    let path = xml_file.path().to_str().unwrap();

    let mut loader = XmlSqlLoader::default();

    // Testing for nonexistent SQL ids
    let result = loader.load_sql(path, "non_existent_sql");
    assert!(result.is_err());

    // Testing for nonexistent files
    let result = loader.load_sql("non_existent.xml", "user.insert");
    assert!(result.is_err());

    // Testing for invalid XML
    let invalid_file = NamedTempFile::new().unwrap();
    let invalid_path = invalid_file.path().to_str().unwrap();
    fs::write(invalid_path, "invalid xml content").unwrap();

    let result = loader.load_sql(invalid_path, "any_id");
    assert!(result.is_err());

    Ok(())
}

// Test 10: Integration Test - Full CRUD operations
#[test]
fn test_integration_crud() -> Result<(), AkitaError> {
    let akita = init_test_db()?;
    struct Repository<'a> {
        akita: &'a Akita
    }
    let repo = Repository {
        akita: &akita
    };
    impl <'a> Repository<'a> {
        // Define CRUD operations
        #[insert("INSERT INTO t_system_user (pk, name, status, token) VALUES (?, ?, ?, ?)")]
        fn create_user(&self, pk: &str, name: &str, status: u8, token: &str) -> Result<u64, AkitaError> {}

        #[select_one("SELECT * FROM t_system_user WHERE id = ?")]
        fn read_user(&self, id: i64) -> Result<Option<User>, AkitaError> {}

        #[update("UPDATE t_system_user SET name = ?, status = ?, level = ? WHERE id = ?")]
        fn update_user(&self, name: &str, status: u8, level: Priority, id: i64) -> Result<u64, AkitaError> {}

        #[delete("DELETE FROM t_system_user WHERE id = ?")]
        fn delete_user(&self, id: i64) -> Result<u64, AkitaError> {}

        #[list("SELECT * FROM t_system_user limit 1")]
        fn list_users(&self) -> Result<Vec<User>, AkitaError> {}
    }

    // Perform CRUD operations
    let test_user1 = create_test_user();
    let test_user2 = create_test_user();

    let id1 = repo.create_user(
        &test_user1.pk,
        &test_user1.name.clone().unwrap_or_default(),
        test_user1.status,
        &test_user1.url_token
    )?;

    let id2 = repo.create_user(
        &test_user2.pk,
        &test_user2.name.clone().unwrap_or_default(),
        test_user2.status,
        &test_user2.url_token
    )?;

    // READING
    let user1 = repo.read_user(id1 as i64)?.unwrap();
    assert_eq!(user1.pk, test_user1.pk);

    // UPDATE
    let updated = repo.update_user("Updated Name", 2, Priority::High, id1 as i64)?;
    assert_eq!(updated, 1);

    let updated_user = repo.read_user(id1 as i64)?.unwrap();
    assert_eq!(updated_user.name, Some("Updated Name".to_string()));
    assert_eq!(updated_user.status, 2);

    // LISTS
    let all_users = repo.list_users()?;
    assert_eq!(all_users.len(), 1);

    // DELETE
    let deleted = repo.delete_user(id1 as i64)?;
    assert_eq!(deleted, 1);

    let remaining_users = repo.list_users()?;
    assert_eq!(remaining_users.len(), 1);
    
    Ok(())
}

// Test 11: Transaction support
#[test]
fn test_transaction_support() -> Result<(), AkitaError> {
    let mut akita = init_test_db()?;

    // Use macros in transactions
    let result = akita.start_transaction().and_then(|mut tx| {
        #[sql("INSERT INTO t_system_user (pk, name, token) VALUES (?, ?, ?)")]
        fn insert_in_tx(tx: &mut AkitaTransaction, pk: &str, name: &str, token: &str) -> Result<u64, AkitaError> {}

        let test_user = create_test_user();
        let id = insert_in_tx(&mut tx, &test_user.pk, &test_user.name.clone().unwrap_or_default(), &test_user.url_token)?;

        #[sql("SELECT * FROM t_system_user WHERE id = ?")]
        fn select_in_tx(tx: &mut AkitaTransaction, id: i64) -> Result<Option<User>, AkitaError> {}

        let user = select_in_tx(&mut tx, id as i64)?.unwrap();
        assert_eq!(user.pk, test_user.pk);

        Ok::<_, AkitaError>(())
    });

    assert!(result.is_ok());

    // Verify that the transaction has committed
    let rows = akita.exec_raw::<u64, _, _>(
        "SELECT COUNT(*) FROM t_system_user where id = 42",
        Params::None,
    )?;

    let count: u64 = *rows.iter().next().unwrap();
    assert_eq!(count, 1);

    Ok(())
}

// Test 12: SQL execution with XML loading
#[test]
fn test_execute_xml_sql() -> Result<(), AkitaError> {
    let xml_file = create_test_xml_file();
    let path = xml_file.path().to_str().unwrap();
    let mut akita = init_test_db()?;

    let mut loader = XmlSqlLoader::default();

    // Load Insert SQL
    let insert_sql = loader.load_sql(path, "user.insert")?;

    // Creating a test user
    let test_user = create_test_user();

    // Preparing parameters
    let params = Params::Named({
        let mut map = IndexMap::new();
        map.insert("pk".to_string(), test_user.pk.clone().into_value());
        map.insert("user_type".to_string(), test_user.user_type.clone().into_value());
        map.insert("name".to_string(), test_user.name.clone().into_value());
        map.insert("headline".to_string(), test_user.headline.unwrap().into_value());
        map.insert("tenant_id".to_string(), test_user.tenant_id.into_value());
        map.insert("status".to_string(), test_user.status.into_value());
        map.insert("level".to_string(), test_user.level.into_value());
        map.insert("age".to_string(), test_user.age.into_value());
        map.insert("birthday".to_string(), test_user.birthday.unwrap().into_value());
        map.insert("gender".to_string(), test_user.gender.into_value());
        map.insert("token".to_string(), test_user.url_token.clone().into_value());
        map
    });

    // Perform insertion
    let result = akita.exec_drop(&insert_sql, params);
    assert!(result.is_ok());

    // Loading Query SQL
    let select_sql = loader.load_sql(path, "user.select_by_status")?;

    // Querying users
    let users = akita.exec_raw::<User, _, _>(
        &select_sql,
        Params::Positional(vec![test_user.status.into_value()]),
    )?;

    assert!(!users.is_empty());

    Ok(())
}