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

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

mod common;
use common::*;

pub fn bench_batch_operations(c: &mut Criterion) {
    let akita = create_bench_akita();
    let repository = akita.repository::<SysUser>();

    let mut group = c.benchmark_group("batch_operations");
    group.sample_size(10); 
    group.measurement_time(Duration::from_secs(20)); 

    // Reduce the batch size to avoid long testing times
    for batch_size in [10, 50, 100, 200,1000].iter() {  // 从 [1, 10, 50, 100] 减少到 [1, 3, 5]
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_insert", batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    let users: Vec<SysUser> = (0..size)
                        .map(|_| create_test_user())
                        .collect();
                    black_box(repository.save_batch::<_>(black_box(users))).unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .sample_size(10) 
        .measurement_time(Duration::from_secs(20));
    targets = bench_batch_operations
);

criterion_main!(benches);