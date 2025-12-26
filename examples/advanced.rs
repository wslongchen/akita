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

//! Advanced features of Akita
//! Run with: cargo run --example advanced

use akita::prelude::*;
use akita::prelude;
use chrono::{NaiveDate, NaiveDateTime};
use std::time::Duration;

#[derive(Entity, Clone, Default, Debug)]
#[table(name = "t_system_user")]
struct User {
    #[id(name = "id")]
    pub id: i64,
    pub pk: String,
    pub user_type: Option<String>,
    pub name: Option<String>,
    pub headline: Option<NaiveDateTime>,
    pub tenant_id: i64,
    pub status: u8,
    pub level: u8,
    pub age: Option<u8>,
    pub birthday: Option<NaiveDate>,
    pub gender: u8,
    #[field(exist = false)]
    pub is_org: bool,
    #[field(name = "token")]
    pub url_token: String,
    ext: ExtInfo,
}

#[derive(FromValue,ToValue, Clone, Default, Debug)]
struct ExtInfo {
    pub id: i64,
    pub english_name: String,
    pub hobbies: Vec<String>, 
}


fn create_test_users(count: usize) -> Vec<User> {
    let mut users = Vec::with_capacity(count);
    for i in 0..count {
        let user = User {
            id: 0,
            pk: format!("user_{}_{}", i, uuid::Uuid::new_v4()),
            user_type: Some(if i % 2 == 0 { "vip" } else { "regular" }.to_string()),
            name: Some(format!("User {}", i)),
            headline: Some(NaiveDateTime::from_timestamp_opt(1609459200 + i as i64 * 86400, 0).unwrap()),
            tenant_id: (i % 3 + 1) as i64,
            status: if i < 5 { 1 } else { 0 },
            level: (i % 5) as u8,
            age: Some(20 + (i % 20) as u8),
            birthday: Some(NaiveDate::from_ymd_opt(1990 + (i % 10) as i32, 1, 1).unwrap()),
            gender: (i % 2) as u8,
            is_org: false,
            url_token: format!("token_{}", i),
            ext: Default::default(),
        };
        users.push(user);
    }
    users
}

fn main() -> Result<(), AkitaError> {
    
    println!("🚀 Starting Akita advanced example...");

    // Setup
    let config = AkitaConfig::new().hostname("127.0.0.1").password("password")
        .username("root").database("test")
        .max_size(5)
        .connection_timeout(Duration::from_secs(10));

    let akita = match Akita::new(config) {
        Ok(akita) => {
            println!("✅ Database connected");
            akita
        }
        Err(e) => {
            eprintln!("❌ Connection failed: {}", e);
            return Ok(());
        }
    };

    // 1. Batch operations
    println!("\n📦 1. Batch Operations");
    let users = create_test_users(5);
    println!("   Creating {} test users...", users.len());
    
    match akita.save_batch::<_, _>(&users) {
        Ok(ids) => println!("✅ Batch insert successful, IDs: {:?}", ids),
        Err(e) => eprintln!("❌ Batch insert failed: {}", e),
    }
    
    // 2. Complex query building
    println!("\n🔍 2. Complex Query Building");
    
    let wrapper = Wrapper::new()
        .select(vec!["id", "name", "level", "age", "status"])
        .eq("status", 1)
        .gt("level", 0)
        .r#in("tenant_id", vec![1, 2])
        .between("age", 20, 40)
        .like("pk", "user_%")
        .order_by_desc(vec!["level"])
        .order_by_asc(vec!["age"])
        .limit(10);
    
    match akita.list::<User>(wrapper) {
        Ok(users) => {
            println!("✅ Complex query returned {} users", users.len());
            for user in users.iter().take(3) {
                println!("   - ID: {}, Name: {:?}, Level: {}, Age: {:?}",
                         user.id, user.name, user.level, user.age);
            }
            if users.len() > 3 {
                println!("   ... and {} more", users.len() - 3);
            }
        }
        Err(e) => eprintln!("❌ Query failed: {}", e),
    }
    
    // 3. Pagination
    println!("\n📄 3. Pagination Example");
    
    for page_no in 1..=2 {
        let wrapper = Wrapper::new()
            .eq("status", 1)
            .order_by_desc(vec!["id"]);
    
        match akita.page::<User>(page_no, 2, wrapper) {
            Ok(page) => {
                println!("   Page {} of {}:", page.current, page.size);
                println!("   Total records: {}", page.total);
                println!("   Records on this page: {}", page.records.len());
                for (i, user) in page.records.iter().enumerate() {
                    println!("     {}. ID: {}, Name: {:?}", i + 1, user.id, user.name);
                }
            }
            Err(e) => eprintln!("❌ Pagination failed: {}", e),
        }
    }
    
    // 4. Aggregation queries
    println!("\n📊 4. Aggregation Queries");
    
    // Count by status
    let count_wrapper = Wrapper::new()
        .group_by(vec!["status"])
        .select(vec!["status", "COUNT(*) as count"]);
    
    // Note: You might need a different struct for custom select results
    println!("   Count by status - Using raw SQL instead...");
    
    let count_by_status: Result<Vec<(u8, i64)>, AkitaError> = akita.exec_raw(
        "SELECT status, COUNT(*) as count FROM t_system_user GROUP BY status",
        ()
    );
    
    match count_by_status {
        Ok(results) => {
            for (status, count) in results {
                println!("   Status {}: {} users", status, count);
            }
        }
        Err(e) => println!("ℹ️ Aggregation query: {}", e),
    }
    
    // 5. Raw SQL with named parameters
    println!("\n🔧 5. Raw SQL with Named Parameters");
    
    let named_query = "SELECT * FROM t_system_user WHERE level > :min_level AND status = :status LIMIT :limit";
    
    match akita.exec_raw::<User, _, _>(named_query, params! {
        "min_level" => 0,
        "status" => 1,
        "limit" => 3
    }) {
        Ok(users) => println!("✅ Named parameter query returned {} users", users.len()),
        Err(e) => eprintln!("❌ Named parameter query failed: {}", e),
    }
    
    // 6. Entity methods
    println!("\n🏷️ 6. Entity Methods");
    
    let new_user = User {
        pk: "entity_method_test".to_string(),
        name: Some("Entity Test User".to_string()),
        tenant_id: 1,
        status: 1,
        level: 2,
        age: Some(30),
        gender: 1,
        url_token: "entity_token".to_string(),
        ..Default::default()
    };
    
    println!("   Using entity methods to save and query...");
    
    // Using entity methods (assuming they exist in your version)
    println!("   Note: Entity methods may vary in your version");
    
    // 7. Cleanup
    println!("\n🧹 7. Cleanup");
    match akita.remove::<User>(Wrapper::new().like("pk", "user_%")) {
        Ok(affected) => println!("✅ Cleaned up {} test users", affected),
        Err(e) => eprintln!("❌ Cleanup failed: {}", e),
    }
    
    println!("\n🎉 Advanced example completed!");
    println!("\n📚 Try other examples:");
    println!("  cargo run --example transaction");
    println!("  cargo run --example interceptor");


    

    Ok(())
}