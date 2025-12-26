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

//! Basic CRUD operations with Akita
//! Run with: cargo run --example basic

use akita::prelude::*;
use akita::*;
use chrono::{NaiveDate, NaiveDateTime};
use std::time::Duration;

// Define the User entity
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
}

fn create_test_user() -> User {
    User {
        id: 0,
        pk: uuid::Uuid::new_v4().to_string(),
        user_type: Some("regular".to_string()),
        name: Some("Test User".to_string()),
        headline: Some(NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
        tenant_id: 1,
        status: 1,
        level: 0,
        age: Some(25),
        birthday: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        gender: 1,
        is_org: false,
        url_token: "test_token".to_string(),
    }
}

fn main() -> Result<(), AkitaError> {
    println!("🚀 Starting Akita basic example...");

    // 1. Create Akita configuration
    let config = AkitaConfig::new().hostname("127.0.0.1").password("password")
        .username("root").database("test")
        .max_size(5)
        .connection_timeout(Duration::from_secs(10));

    // 2. Create Akita instance
    let akita = match Akita::new(config) {
        Ok(akita) => {
            println!("✅ Successfully connected to database");
            akita
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to database: {}", e);
            eprintln!("💡 Please make sure:");
            eprintln!("   1. Database server is running");
            eprintln!("   2. Connection string is correct");
            eprintln!("   3. Database 'test_db' exists");
            eprintln!("\nYou can set DATABASE_URL environment variable:");
            eprintln!("  export DATABASE_URL=mysql://user:pass@host:port/database");
            return Ok(());
        }
    };

    // 3. Create a test user
    let user = create_test_user();
    println!("📝 Test user created: {:?}", user.name);

    // 4. Save the user
    println!("💾 Saving user to database...");
    match akita.save::<_, i64>(&user) {
        Ok(Some(id)) => {
            println!("✅ User saved with ID: {}", id);

            // 5. Retrieve the user
            println!("🔍 Retrieving user by ID: {}...", id);
            match akita.select_by_id::<User, _>(id) {
                Ok(Some(retrieved_user)) => {
                    println!("✅ User retrieved successfully");
                    println!("   ID: {}", retrieved_user.id);
                    println!("   Name: {:?}", retrieved_user.name);
                    println!("   Status: {}", retrieved_user.status);

                    // 6. Update the user
                    println!("🔄 Updating user...");
                    let mut user_to_update = retrieved_user.clone();
                    user_to_update.level = 1;
                    user_to_update.name = Some("Updated Name".to_string());

                    match akita.update_by_id(&user_to_update) {
                        Ok(affected) => {
                            println!("✅ User updated successfully (affected rows: {})", affected);

                            // 7. Query with conditions
                            println!("🔍 Querying users with conditions...");
                            let wrapper = Wrapper::new()
                                .eq("status", 1)
                                .gt("level", 0)
                                .order_by_asc(vec!["id"]);

                            match akita.list::<User>(wrapper) {
                                Ok(users) => {
                                    println!("✅ Found {} users", users.len());
                                    for (i, u) in users.iter().enumerate() {
                                        println!("   {}. ID: {}, Name: {:?}", i + 1, u.id, u.name);
                                    }

                                    // 8. Delete the user
                                    println!("🗑️ Deleting user...");
                                    match akita.remove_by_id::<User, _>(id) {
                                        Ok(_) => println!("✅ User deleted successfully"),
                                        Err(e) => eprintln!("❌ Failed to delete user: {}", e),
                                    }
                                }
                                Err(e) => eprintln!("❌ Failed to query users: {}", e),
                            }
                        }
                        Err(e) => eprintln!("❌ Failed to update user: {}", e),
                    }
                }
                Ok(None) => println!("❓ User not found"),
                Err(e) => eprintln!("❌ Failed to retrieve user: {}", e),
            }
        }
        Ok(None) => println!("⚠️ User saved but no ID returned"),
        Err(e) => eprintln!("❌ Failed to save user: {}", e),
    }

    // 9. Show raw SQL query example
    println!("\n📊 Raw SQL query example...");
    let query_result: Result<Vec<User>, AkitaError> = akita.exec_raw(
        "SELECT * FROM t_system_user WHERE status = ? LIMIT 5",
        (1,)
    );

    match query_result {
        Ok(users) => println!("✅ Raw query returned {} users", users.len()),
        Err(e) => println!("ℹ️ Raw query error (may be expected if no users): {}", e),
    }

    // 10. Count users
    println!("\n📈 Counting users...");
    let count_result = akita.count::<User>(Wrapper::new());

    match count_result {
        Ok(count) => println!("✅ Total users in table: {}", count),
        Err(e) => println!("ℹ️ Count error: {}", e),
    }

    println!("\n🎉 Basic example completed!");
    println!("\n📚 Next steps:");
    println!("  1. Run: cargo run --example advanced");
    println!("  2. Run: cargo run --example transaction");
    println!("  3. Check: https://docs.rs/akita for full documentation");

    Ok(())
}