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

// benches/wrapper_building_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;
use akita::prelude::*;

pub fn bench_wrapper_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("wrapper_building");
    group.measurement_time(Duration::from_secs(3));

    // Benchmark: Simple Wrapper build
    group.bench_function("simple_wrapper", |b| {
        b.iter(|| {
            black_box(
                Wrapper::new()
                    .eq("username", "test")
                    .eq("status", 1)
            );
        });
    });

    // Benchmark: Complex Wrapper build
    group.bench_function("complex_wrapper", |b| {
        b.iter(|| {
            black_box(
                Wrapper::new()
                    .select(vec!["id".to_string(), "name".to_string()])
                    .eq("username", "test")
                    .ne("status", 0)
                    .gt("age", 18)
                    .lt("age", 65)
                    .like("name", "%john%")
                    .order_by_asc(vec!["created_at"])
                    .limit(10)
            );
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .sample_size(20);
    targets = bench_wrapper_building
);

criterion_main!(benches);