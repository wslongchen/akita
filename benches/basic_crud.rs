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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod common;
use common::*;

pub fn bench_basic_crud_operations(c: &mut Criterion) {
    let akita = create_bench_akita();
    let repository = akita.repository::<SysUser>();

    let mut group = c.benchmark_group("basic_crud");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);

    // Benchmark: Insert operation
    group.bench_function("insert", |b| {
        b.iter(|| {
            let user = create_test_user();
            black_box(repository.save::<i64>(black_box(&user))).unwrap();
        });
    });

    // Benchmark: Query operations
    group.bench_function("query_by_id", |b| {
        let user = create_test_user();
        let id = repository.save::<i64>(&user).unwrap();

        b.iter(|| {
            black_box(repository.select_by_id::<i64>(black_box(id.unwrap()))).unwrap();
        });
    });

    // Benchmark: Update operation
    group.bench_function("update_by_id", |b| {
        let user = create_test_user();
        let id: Option<i64> = repository.save(&user).unwrap();
        let mut user_to_update = user.clone();
        user_to_update.name = Some("Updated Name".to_string());

        b.iter(|| {
            black_box(repository.update_by_id(black_box(&user_to_update))).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.05)
        .noise_threshold(0.02)
        .warm_up_time(Duration::from_secs(1));
    targets = bench_basic_crud_operations
);

criterion_main!(benches);