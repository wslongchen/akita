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
//! Transaction examples with Akita
//! Run with: cargo run --example transaction

use akita::prelude::*;
use akita::*;
use std::time::Duration;

#[derive(Entity, Clone, Default, Debug)]
#[table(name = "t_system_user")]
struct User {
    #[id(name = "id")]
    pub id: i64,
    pub pk: String,
    pub name: Option<String>,
    pub tenant_id: i64,
    pub status: u8,
    pub level: u8,
    pub balance: f64,
    #[field(name = "token")]
    pub url_token: String,
}

fn create_transfer_users() -> (User, User) {
    let user1 = User {
        pk: "user_a".to_string(),
        name: Some("User A".to_string()),
        tenant_id: 1,
        status: 1,
        level: 1,
        balance: 1000.0,
        url_token: "token_a".to_string(),
        ..Default::default()
    };

    let user2 = User {
        pk: "user_b".to_string(),
        name: Some("User B".to_string()),
        tenant_id: 1,
        status: 1,
        level: 1,
        balance: 500.0,
        url_token: "token_b".to_string(),
        ..Default::default()
    };

    (user1, user2)
}

fn main() -> std::result::Result<(), AkitaError> {
    println!("🚀 Starting Akita transaction example...");

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

    // Clean up any existing test data
    let _ = akita.remove::<User>(Wrapper::new().r#in("pk", vec!["user_a", "user_b"]));

    // Example 1: Basic transaction
    println!("\n1️⃣ Basic Transaction");
    let (user1, user2) = create_transfer_users();

    println!("   Initial balances:");
    println!("   - User A: ${}", user1.balance);
    println!("   - User B: ${}", user2.balance);
    println!("   Transferring $200 from A to B...");

    let transfer_amount = 200.0;

    match akita.start_transaction() {
        Ok(mut tx) => {
            println!("   Transaction started");

            // Save both users
            match tx.save::<_, i64>(&user1) {
                Ok(Some(id1)) => {
                    println!("   User A saved with ID: {}", id1);

                    match tx.save::<_, i64>(&user2) {
                        Ok(Some(id2)) => {
                            println!("   User B saved with ID: {}", id2);

                            // Simulate transfer
                            println!("   Simulating transfer...");

                            // Update balances within transaction
                            let update_a = tx.exec_drop(
                                "UPDATE t_system_user SET balance = balance - ? WHERE id = ?",
                                (transfer_amount, id1)
                            );

                            let update_b = tx.exec_drop(
                                "UPDATE t_system_user SET balance = balance + ? WHERE id = ?",
                                (transfer_amount, id2)
                            );

                            if update_a.is_ok() && update_b.is_ok() {
                                println!("   Transfer successful!");
                                match tx.commit() {
                                    Ok(_) => println!("   ✅ Transaction committed"),
                                    Err(e) => {
                                        eprintln!("   ❌ Commit failed: {}", e);
                                        let _ = tx.rollback();
                                    }
                                }
                            } else {
                                eprintln!("   ❌ Transfer failed, rolling back...");
                                let _ = tx.rollback();
                            }
                        }
                        Ok(None) => {
                            eprintln!("   ❌ User B not saved, rolling back...");
                            let _ = tx.rollback();
                        }
                        Err(e) => {
                            eprintln!("   ❌ Failed to save User B: {}, rolling back...", e);
                            let _ = tx.rollback();
                        }
                    }
                }
                Err(e) => {
                    eprintln!("   ❌ Failed to save User A: {}, rolling back...", e);
                    let _ = tx.rollback();
                }
                _ => {
                    eprintln!("   ❌ Unexpected error, rolling back...");
                    let _ = tx.rollback();
                }
            }
        }
        Err(e) => eprintln!("❌ Failed to start transaction: {}", e),
    }

    // Example 2: Transaction with error handling
    println!("\n2️⃣ Transaction with Error Handling");

    println!("   Testing transaction rollback on error...");

    let result: Result<(), AkitaError> = akita.start_transaction().and_then(|mut tx| {
        // Create test user
        let test_user = User {
            pk: "test_rollback".to_string(),
            name: Some("Rollback Test".to_string()),
            tenant_id: 1,
            status: 1,
            level: 1,
            balance: 100.0,
            url_token: "rollback_token".to_string(),
            ..Default::default()
        };

        // Save user
        tx.save::<_, i64>(&test_user)?;
        println!("   User saved in transaction");

        // Simulate an error
        println!("   Simulating business error...");

        // This will cause the transaction to rollback
        Err(AkitaError::DataError("Business validation failed".to_string()))?;

        // This line won't be reached
        tx.commit()
    });

    match result {
        Ok(_) => println!("   ❌ Unexpected: Transaction should have rolled back"),
        Err(e) => println!("   ✅ Transaction rolled back as expected: {}", e),
    }

    // Verify user was not saved (due to rollback)
    let check_user: Result<Option<User>, AkitaError> = akita.select_by_id(1);
    match check_user {
        Ok(Some(_)) => println!("   ⚠️ User exists (might be from previous run)"),
        Ok(None) => println!("   ✅ User not found (rollback worked)"),
        Err(_) => println!("   ℹ️ Could not verify (table might not exist)"),
    }

    // Example 3: Nested operations in transaction
    println!("\n3️⃣ Complex Transaction Operations");

    println!("   Performing multiple operations in single transaction...");

    match akita.start_transaction() {
        Ok(mut tx) => {
            // Create multiple users
            let users = vec![
                User {
                    pk: "multi_1".to_string(),
                    name: Some("Multi User 1".to_string()),
                    tenant_id: 1,
                    status: 1,
                    level: 1,
                    balance: 100.0,
                    url_token: "multi_1_token".to_string(),
                    ..Default::default()
                },
                User {
                    pk: "multi_2".to_string(),
                    name: Some("Multi User 2".to_string()),
                    tenant_id: 1,
                    status: 1,
                    level: 2,
                    balance: 200.0,
                    url_token: "multi_2_token".to_string(),
                    ..Default::default()
                },
            ];

            // Batch insert
            match tx.save_batch::<_, _>(&users) {
                Ok(ids) => {
                    println!("   Batch inserted users Sucess");

                    // Update one of them
                    let update_wrapper = Wrapper::new().eq("pk", "multi_1");
                    let update_user = User {
                        pk: "multi_1".to_string(),
                        name: Some("Updated Multi User".to_string()),
                        level: 3,
                        ..users[0].clone()
                    };

                    match tx.update(&update_user, update_wrapper) {
                        Ok(affected) => {
                            println!("   Updated {} user(s)", affected);

                            // Query within transaction
                            match tx.list::<User>(Wrapper::new().like("pk", "multi_%")) {
                                Ok(queried_users) => {
                                    println!("   Found {} users in transaction", queried_users.len());

                                    // Commit all changes
                                    match tx.commit() {
                                        Ok(_) => println!("   ✅ All operations committed successfully"),
                                        Err(e) => eprintln!("   ❌ Commit failed: {}", e),
                                    }
                                }
                                Err(e) => {
                                    eprintln!("   ❌ Query failed: {}, rolling back...", e);
                                    let _ = tx.rollback();
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("   ❌ Update failed: {}, rolling back...", e);
                            let _ = tx.rollback();
                        }
                    }
                }
                Err(e) => {
                    eprintln!("   ❌ Batch insert failed: {}, rolling back...", e);
                    let _ = tx.rollback();
                }
            }
        }
        Err(e) => eprintln!("❌ Failed to start transaction: {}", e),
    }

    // Cleanup
    println!("\n🧹 Cleaning up test data...");
    let _ = akita.remove::<User>(Wrapper::new().like("pk", "user_%"));
    let _ = akita.remove::<User>(Wrapper::new().like("pk", "multi_%"));
    let _ = akita.remove::<User>(Wrapper::new().eq("pk", "test_rollback"));

    println!("\n🎉 Transaction example completed!");
    println!("\n📚 Key takeaways:");
    println!("  • Transactions ensure data consistency");
    println!("  • Rollback happens automatically on error");
    println!("  • All operations in transaction succeed or fail together");

    Ok(())
}
