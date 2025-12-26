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

// benches/query_operations_bench.rs
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use akita::prelude::*;

mod common;
use common::*;

pub fn bench_query_operations(c: &mut Criterion) {
    let akita = create_bench_akita();
    let repository = akita.repository::<SysUser>();

    // Prepare a small amount of test data
    for _ in 0..10 {
        let user = create_test_user();
        repository.save::<i64>(&user).unwrap();
    }

    let mut group = c.benchmark_group("query_operations");
    group.measurement_time(Duration::from_secs(5));

    // Benchmark: Simple queries
    group.bench_function("simple_query", |b| {
        b.iter(|| {
            let wrapper = Wrapper::new().eq("status", 1).limit(1);
            black_box(repository.list(black_box(wrapper))).unwrap();
        });
    });

    // Benchmark: Complex queries
    group.bench_function("complex_query", |b| {
        b.iter(|| {
            let wrapper = Wrapper::new()
                .eq("status", 1)
                .gt("age", 18)
                .r#in("level", vec![0, 1])
                .limit(5);
            black_box(repository.list(black_box(wrapper))).unwrap();
        });
    });

    // Benchmark: paging queries
    for page_size in [5, 10].iter() {
        group.bench_with_input(
            BenchmarkId::new("pagination", page_size),
            page_size,
            |b, &size| {
                b.iter(|| {
                    black_box(repository.page(black_box(1), black_box(size), Wrapper::new())).unwrap();
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
        .sample_size(10);
    targets = bench_query_operations
);

criterion_main!(benches);