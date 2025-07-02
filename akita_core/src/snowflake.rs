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
    epoch: u64,               // 起始时间戳
    // datacenter_id: u64,       // 数据中心 ID
    machine_id: u64,          // 机器 ID
    sequence: AtomicU64,      // 序列号
    last_timestamp: AtomicU64, // 上次生成 ID 的时间戳
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
            epoch: 1_609_459_200_000, // 起始时间戳（毫秒）
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
                // 正常生成 ID
                if self
                    .last_timestamp
                    .compare_exchange(last_time, current_time, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    self.sequence.store(0, Ordering::SeqCst); // 重置序列号
                    return self.compose_id(current_time, 0);
                }
            } else if current_time == last_time {
                // 同一时间戳，使用序列号
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) & 0xFFF; // 序列号取模4096
                if seq == 0 {
                    // 序列号耗尽，自旋等待下一纳秒
                    while Self::current_time_nanos() <= current_time {}
                } else {
                    return self.compose_id(current_time, seq);
                }
            } else {
                // 处理时钟回退：逻辑时间递增
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
        ((timestamp - self.epoch) << 22)  // 时间戳部分，左移 22 位
            | (self.machine_id << 12)    // 机器 ID，左移 12 位
            | sequence                   // 序列号部分
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
    (hasher.finish() & 0xFFFF) as u16 // 限制在 16 位以内
}

// 动态分配 datacenter_id
#[allow(unused)]
fn get_datacenter_id() -> u16 {
    let interfaces = datalink::interfaces();
    let mac = interfaces.iter().find_map(|iface| iface.mac).unwrap();
    let mut hasher = DefaultHasher::new();
    mac.hash(&mut hasher);
    (hasher.finish() & 0x1F) as u16 // 限制为 5 位
}

// pub fn generate_machine_id() -> u16 {
//     use std::process;
//     use rand::Rng;
//     let pid = process::id() as u16;
//     let random: u16 = rand::thread_rng().gen_range(0..=0xFFF);
//     pid ^ random // 组合进程 ID 和随机数
// }

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