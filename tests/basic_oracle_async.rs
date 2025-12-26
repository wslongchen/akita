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
#[cfg(feature = "oracle-async")]


use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;
mod common;
use common::*;
use akita::prelude::*;

const ORACLE_URL: &str = "oracle://127.0.0.1:1521/XE";


async fn create_akita() -> Result<Akita, AkitaError> {
    let builder = AsyncInterceptorBuilder::new()
        .register(Arc::new(LoggingInterceptor::new()))
        .enable("logging").unwrap();
    let chain = builder.build().unwrap();
    let cfg = AkitaConfig::new().url(ORACLE_URL).username("system").password("password").database("XE");
    Ok(Akita::new(cfg).await.unwrap().with_interceptor_chain(chain))
}


#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_connection_creation() {
    let result = create_akita().await;
    assert!(result.is_ok(), "The database connection creation should be successful {}", result.err().unwrap());
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_single_insert() {
    let akita = create_akita().await.unwrap();
    let user = create_test_user();

    let result = akita.save(&user).await;
    assert!(result.is_ok(), "The single insertion should succeed:{}",result.err().unwrap());

    let insert_id: Option<i32> = result.unwrap();
    assert!(insert_id.is_some(), "The insertion should return the generated ID");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_chain() {
    let akita = create_akita().await.unwrap();
    let user = create_test_user();
    let query = akita.query_builder::<User>().eq("name", "Jack").limit(1).list().await;
    assert!(query.is_ok(), "The query should succeed.:{}",query.err().unwrap());

    let update = akita.update_builder::<User>().eq("name", "Jack").set("tenant_id", 1111).update(&user).await;
    assert!(update.is_ok(), "The single change should succeed:{}",update.err().unwrap());
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_batch_insert() {
    let akita = create_akita().await.unwrap();
    let mut users = Vec::new();
    for _i in 0..1 {
        users.push(create_test_user());
    }
    let result = akita.save_batch::<_, _>(&users).await;
    assert!(result.is_ok(), "The bulk insert should succeed{}", result.err().unwrap());
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_delete_by_wrapper() {
    let akita = create_akita().await.unwrap();

    let result = akita.remove::<User>(Wrapper::new().eq("pk", "Jack")).await;
    assert!(result.is_ok(), "The deletion via Wrapper should succeed");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_delete_by_id() {
    let akita = create_akita().await.unwrap();

    let result = akita.remove_by_id::<User, _>(1).await;
    assert!(result.is_ok(), "Deletion by ID should succeed");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_update_by_wrapper() {
    let akita = create_akita().await.unwrap();
    let user = create_test_user();

    let result = akita.update(&user, Wrapper::new().set("headline", SqlExpr("SYSDATE".to_string())).set("age", SqlExpr("age+100".to_string())).eq("id", 537283)).await;
    assert!(result.is_ok(), "Updating via Wrapper should succeed{}", result.err().unwrap());
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_convert() {
    let ak = create_akita().await.unwrap();
    let ak = Arc::new(ak);

    // Use Arc and Mutex to securely share results between asynchronous tasks
    let result = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for _ in 0..5 {  // Five asynchronous tasks were created for testing
        let result_clone = Arc::clone(&result);
        let ak_clone = Arc::clone(&ak);

        let handle = task::spawn(async move {
            for i in 0..3 {  // Each task is performed three times
                let user = create_test_user();

                match ak_clone.save::<User, i32>(&user).await {
                    Ok(saved_id) => {
                        println!("Task saved user with ID: {:?}", saved_id);

                        // 保存结果到共享的 Vec
                        let mut guard = result_clone.lock().await;
                        guard.push(saved_id);
                    }
                    Err(e) => {
                        println!("Failed to save user: {}", e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all asynchronous tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Checking the results
    let final_result = result.lock().await;
    println!("Total saved users: {}", final_result.len());
    println!("Saved IDs: {:?}", final_result);

    // Validation results
    assert!(!final_result.is_empty(), "Should have saved at least one user");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_update_by_id() {
    let akita = create_akita().await.unwrap();
    let user = create_test_user();

    let result = akita.update_by_id(&user).await;
    assert!(result.is_ok(), "Updating by ID should succeed{}", result.err().unwrap());
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_query_list() {
    let akita = create_akita().await.unwrap();

    let wrapper = Wrapper::new()
        .select(vec!["id".to_string(), "gender".to_string()])
        .eq("name", "jack")
        .gt("age", 1)
        .lt("age", 10)
        .between("age", 1, 10)
        .r#in("user_type", vec!["admin", "super"]);

    let result = akita.list::<User>(wrapper).await;
    assert!(result.is_ok(), "Querying the list should succeed{:?}", result.err());

    let users: Vec<User> = result.unwrap();
    assert!(users.len() >= 0, "The length of the returned user list should be greater than or equal to 0");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_query_pagination() {
    let akita = create_akita().await.unwrap();

    let wrapper = Wrapper::new().eq("name", "Jack");
    let page_no = 1;
    let page_size = 10;

    let result = akita.page::<User>(page_no, page_size, wrapper).await;
    assert!(result.is_ok(), "The pagination query should succeed");

    let page = result.unwrap();
    assert!(page.total >= 0, "The total number of records should be greater than zero");
    assert!(page.records.len() <= page_size as usize, "The number of returned records should not exceed the page size");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_count_query() {
    let akita = create_akita().await.unwrap();

    let result = akita.count::<User>(Wrapper::new().eq("name", "Jack")).await;
    assert!(result.is_ok(), "The count query should succeed");

    let count = result.unwrap();
    assert!(count >= 0, "The count should be greater than or equal to 0");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_raw_sql_query() {
    let akita = create_akita().await.unwrap();

    // Testing parameterized queries
    let result = akita.exec_first::<User, _, _>(
        "select * from TEST.t_system_user where \"NAME\" = ? and \"ID\" = ?",
        ("Jack", 42)
    ).await;
    assert!(result.is_ok(), "The original SQL query should succeed{}", result.err().unwrap());

    // Testing named parameter queries
    let result = akita.exec_first::<User, _, _>(
        "select * from TEST.t_system_user where \"NAME\" = :name and \"ID\" = :id",
        params! {
            "name" => "Jack",
            "id" => 42
        }
    ).await;
    assert!(result.is_ok(), "The named parameter SQL query should succeed");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_entity_methods() {
    let akita = create_akita().await.unwrap();
    let mut user = create_test_user();
    
    // Testing entity updates
    let result = user.update_by_id::<_>(&akita).await;
    assert!(result.is_ok(), "The entity update method should succeed");
    
    // Testing entity deletion
    let result = user.remove_by_id::<_,i32>(&akita, 1).await;
    assert!(result.is_ok(), "The entity deletion method should succeed");
    
    // Test the entity list query
    let result = User::list(&akita, Wrapper::new().eq("name", "Jack")).await;
    assert!(result.is_ok(), "The entity list query should succeed");
    
    // Testing entity paging queries
    let result = User::page::<_>(&akita, 1, 1, Wrapper::new().eq("name", "Jack")).await;
    assert!(result.is_ok(), "The entity paging query should succeed");
}

#[tokio::test]
#[cfg(feature = "oracle-async")]
async fn test_transaction() {
    let akita = create_akita().await.unwrap();

    let mut transaction = akita.start_transaction().await.unwrap();
    // Perform actions within a transaction
    transaction.save::<User, i64>(&create_test_user()).await.unwrap();
    let result = transaction.commit().await;
    
    assert!(result.is_ok(), "The transaction should succeed");
}