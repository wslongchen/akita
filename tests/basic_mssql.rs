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

use akita::prelude::*;
use std::sync::Arc;
mod common;
use common::*;

const MSSQL_URL: &str = "jdbc:sqlserver://127.0.0.1:1433;database=test";



fn create_akita() -> Result<Akita, AkitaError> {
    let builder = InterceptorBuilder::new()
        .register(Arc::new(LoggingInterceptor::new()))
        .enable("logging").unwrap();

    let chain = builder.build().unwrap();
    let cfg = AkitaConfig::new().url(MSSQL_URL).username("sa").password("password");
    Ok(Akita::new(cfg).unwrap().with_interceptor_chain(chain))
}


#[test]
#[cfg(feature = "mssql-sync")]
fn test_connection_creation() {
    let result = create_akita();
    assert!(result.is_ok(), "The database connection creation should be successful {}", result.err().unwrap());
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_single_insert() {
    let akita = create_akita().unwrap();
    let user = create_test_user();

    let result = akita.save(&user);
    assert!(result.is_ok(), "The single insertion should succeed:{}",result.err().unwrap());

    let insert_id: Option<i32> = result.unwrap();
    assert!(insert_id.is_some(), "The insertion should return the generated ID");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_chain() {
    let akita = create_akita().unwrap();
    let user = create_test_user();
    let query = akita.query_builder::<User>().eq("name", "Jack").limit(1).list();
    assert!(query.is_ok(), "The query should succeed.:{}",query.err().unwrap());

    let update = akita.update_builder::<User>().eq("name", "Jack").set("tenant_id", 1111).update(&user);
    assert!(update.is_ok(), "The single change should succeed:{}",update.err().unwrap());
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_batch_insert() {
    let akita = create_akita().unwrap();
    let mut users = Vec::new();
    for _i in 0..500 {
        users.push(create_test_user());
    }
    let result = akita.save_batch::<_, _>(&users);
    assert!(result.is_ok(), "The bulk insert should succeed{}", result.err().unwrap());
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_delete_by_wrapper() {
    let akita = create_akita().unwrap();

    let result = akita.remove::<User>(Wrapper::new().eq("pk", "Jack"));
    assert!(result.is_ok(), "The deletion via Wrapper should succeed");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_delete_by_id() {
    let akita = create_akita().unwrap();

    let result = akita.remove_by_id::<User, _>(1);
    assert!(result.is_ok(), "Deletion by ID should succeed");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_update_by_wrapper() {
    let akita = create_akita().unwrap();
    let user = create_test_user();

    let result = akita.update(&user, Wrapper::new().set("headline", SqlExpr("GETDATE()".to_string())).set("age", SqlExpr("age+100".to_string())).eq("id", 537283));
    assert!(result.is_ok(), "Updating via Wrapper should succeed{}", result.err().unwrap());
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_convert() {
    use std::sync::{Arc, Mutex};
    let ak = create_akita().unwrap();
    let ak = Arc::new(ak);
    // Use Arc and Mutex to safely share results between threads
    let result = Arc::new(Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..5)  // Create five threads for testing
        .map(|_| {
            let result_clone = Arc::clone(&result);
            let ak = Arc::clone(&ak);

            std::thread::spawn(move || {
                for i in 0..3 {  // Each thread performs three operations
                    let user = create_test_user();

                    match ak.save::<User, i32>(&user) {
                        Ok(saved_id) => {
                            println!("Thread saved user with ID: {:?}", saved_id);

                            // Save the result to the shared Vec
                            let mut guard = result_clone.lock().unwrap();
                            guard.push(saved_id);
                        }
                        Err(e) => {
                            println!("Failed to save user: {}", e);
                        }
                    }
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Checking the results
    let final_result = result.lock().unwrap();
    println!("Total saved users: {}", final_result.len());
    println!("Saved IDs: {:?}", final_result);

    // Validation results
    assert!(!final_result.is_empty(), "Should have saved at least one user");

}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_update_by_id() {
    let akita = create_akita().unwrap();
    let user = create_test_user();

    let result = akita.update_by_id(&user);
    assert!(result.is_ok(), "Updating by ID should succeed{}", result.err().unwrap());
}


#[test]
#[cfg(feature = "mssql-sync")]
fn test_query_list() {
    let akita = create_akita().unwrap();

    let wrapper = Wrapper::new()
        .select(vec!["id".to_string(), "gender".to_string()])
        .eq("name", "jack")
        .gt("age", 1)
        .lt("age", 10)
        .between("age", 1, 10)
        .r#in("user_type", vec!["admin", "super"]);

    let result = akita.list::<User>(wrapper);
    assert!(result.is_ok(), "Querying the list should succeed{:?}", result.err());

    let users: Vec<User> = result.unwrap();
    assert!(users.len() >= 0, "The length of the returned user list should be greater than or equal to 0");
}


#[test]
#[cfg(feature = "mssql-sync")]
fn test_query_pagination() {
    let akita = create_akita().unwrap();

    let wrapper = Wrapper::new().eq("name", "Jack");
    let page_no = 1;
    let page_size = 10;

    let result = akita.page::<User>(page_no, page_size, wrapper);
    assert!(result.is_ok(), "The pagination query should succeed");

    let page = result.unwrap();
    assert!(page.total >= 0, "The total number of records should be greater than zero");
    assert!(page.records.len() <= page_size as usize, "The number of returned records should not exceed the page size");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_count_query() {
    let akita = create_akita().unwrap();

    let result = akita.count::<User>(Wrapper::new().eq("name", "Jack"));
    assert!(result.is_ok(), "The count query should succeed");

    let count = result.unwrap();
    assert!(count >= 0, "The count should be greater than or equal to 0");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_raw_sql_query() {
    let akita = create_akita().unwrap();

    // Testing parameterized queries
    let result = akita.exec_first::<User, _, _>(
        "select * from t_system_user where name = ? and id = ?",
        ("Jack", 2)
    );
    assert!(result.is_ok(), "The original SQL query should succeed{}", result.err().unwrap());

    // Testing named parameter queries
    let result = akita.exec_first::<User, _, _>(
        "select * from t_system_user where name = ? and id = ?",
        params! {
            "name" => "Jack",
            "id" => 2
        }
    );
    assert!(result.is_ok(), "The named parameter SQL query should succeed");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_entity_methods() {
    let akita = create_akita().unwrap();
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

#[test]
#[cfg(feature = "mssql-sync")]
fn test_repository_methods() {
    let akita = create_akita().unwrap();
    let repository = akita.repository::<User>();
    let user = create_test_user();

    // Testing entity updates
    let result = repository.update_by_id(&user);
    assert!(result.is_ok(), "The entity update method should succeed");

    // Testing entity deletion
    let result = repository.remove_by_id::<_>(1);
    assert!(result.is_ok(), "The entity deletion method should succeed{}",result.err().unwrap());

    // Test the entity list query

    let result = repository.list(Wrapper::new().eq("name", "Jack"));
    assert!(result.is_ok(), "The entity list query should succeed");

    // Testing entity paging queries
    let result = repository.page(1, 1, Wrapper::new().eq("name", "Jack"));
    assert!(result.is_ok(), "The entity paging query should succeed");
}

#[test]
#[cfg(feature = "mssql-sync")]
fn test_transaction() {
    let akita = create_akita().unwrap();

    let result = akita.start_transaction().and_then(|mut transaction| {
        // Perform an action within a transaction
        transaction.save::<User, i64>(&create_test_user())?;
        transaction.commit()
    });

    assert!(result.is_ok(), "The transaction should succeed");
}