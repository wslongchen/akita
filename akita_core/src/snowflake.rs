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
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use pnet::datalink;
use std::hash::DefaultHasher;
use std::hash::Hasher;

#[derive(Debug)]
pub struct Snowflake {
    epoch: u64,               // Start timestamp
    // datacenter_id: u64,       // Data Centers ID
    machine_id: u64,          // machine ID
    sequence: AtomicU64,      // Serial number
    last_timestamp: AtomicU64, // The timestamp of the last generated ID
}

impl Default for Snowflake {
    fn default() -> Self {
        let machine_id = generate_machine_id();
        // let _datacenter_id = get_datacenter_id();
        Self::new(machine_id.into())
    }
}

#[allow(unused)]
impl Snowflake {
    pub fn new(machine_id: u64) -> Self {
        if machine_id >= 102400 {
            panic!("machine_id must be less than 1024");
        }
        Snowflake {
            epoch: 1_609_459_200_000, // Start timestamp (milliseconds)
            machine_id,
            sequence: AtomicU64::new(0),
            last_timestamp: AtomicU64::new(0),
        }
    }

    pub fn generate(&self) -> u64 {
        loop {
            let current_time = Self::current_time_nanos(); // 纳秒级时间戳
            let last_time = self.last_timestamp.load(Ordering::Relaxed);

            if current_time > last_time {
                // Generate ID normally
                if self
                    .last_timestamp
                    .compare_exchange(last_time, current_time, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    self.sequence.store(0, Ordering::SeqCst); // 重置序列号
                    return self.compose_id(current_time, 0);
                }
            } else if current_time == last_time {
                // Same timestamp, use serial number
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) & 0xFFF; // Serial number 4096
                if seq == 0 {
                    // Serial number exhausted，Spin waiting for the next nanosecond
                    while Self::current_time_nanos() <= current_time {}
                } else {
                    return self.compose_id(current_time, seq);
                }
            } else {
                // Handle clock backoff: Logical time increment
                let adjusted_time = last_time + 1;
                if self
                    .last_timestamp
                    .compare_exchange(last_time, adjusted_time, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    self.sequence.store(0, Ordering::SeqCst);
                    return self.compose_id(adjusted_time, 0);
                }
            }
        }
    }

    fn compose_id(&self, timestamp: u64, sequence: u64) -> u64 {
        ((timestamp - self.epoch) << 22)  // Timestamp section, moved 22 bits to the left
            | (self.machine_id << 12)    // Machine ID, shifted 12 digits to the left
            | sequence                   // Serial number section
    }

    fn current_time_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}


pub fn generate_machine_id() -> u16 {
    let interfaces = datalink::interfaces();
    let mac = interfaces.iter().find_map(|iface| iface.mac).unwrap();
    let mut hasher = DefaultHasher::new();
    mac.hash(&mut hasher);
    (hasher.finish() & 0xFFFF) as u16 // Limit to 16 bits
}

// 动态分配 datacenter_id
#[allow(unused)]
fn get_datacenter_id() -> u16 {
    let interfaces = datalink::interfaces();
    let mac = interfaces.iter().find_map(|iface| iface.mac).unwrap();
    let mut hasher = DefaultHasher::new();
    mac.hash(&mut hasher);
    (hasher.finish() & 0x1F) as u16 // Limit to 5 bits
}

#[test]
fn test_snowflake() {

    let generator = std::sync::Arc::new(Snowflake::default());

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let generator = generator.clone();
            std::thread::spawn(move || {
                for _ in 0..10 {
                    let id = generator.generate();
                    println!("Generated ID: {}", id);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}