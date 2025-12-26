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
use std::fmt::Debug;
use uuid::Uuid;
use akita_core::Snowflake;

/// ID generator
pub trait IdentifierGenerator: Send + Sync + Debug {
    fn next_id(&self) -> u64;

    /// By default, UUIDs without a breakdown are generated
    fn next_uuid(&self) -> String {
        Uuid::new_v4().simple().to_string()
    }
}

#[derive(Debug)]
pub struct SnowflakeGenerator {
    snowflake: Snowflake
}

impl SnowflakeGenerator {
    pub fn new() -> Self {
        Self {
            snowflake: Snowflake::default()
        }
    }

    fn generate_id(&self) -> u64 {
        self.snowflake.generate()
    }
}

impl IdentifierGenerator for SnowflakeGenerator {
    fn next_id(&self) -> u64 {
        self.generate_id()
    }
}


#[test]
fn test_key() {
    let generator = SnowflakeGenerator::new();

    let id_as_u64: u64 = generator.next_id();

    println!("u64 ID: {}", id_as_u64);
}