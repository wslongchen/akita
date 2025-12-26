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

use akita::*;
use akita::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;
use uuid::Uuid;

mod common;
use common::*;

pub fn bench_connection_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("connection_operations");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(10);

    // Benchmark: Create a connection instance
    group.bench_function("create_akita_instance", |b| {
        b.iter(|| {
            let config = create_test_akita_cfg();
            black_box(Akita::new(black_box(config))).unwrap();
        });
    });

    // Benchmark: Simple SQL queries (no entity conversions)
    group.bench_function("raw_sql_query", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT 1", ());
            black_box(result).unwrap();
        });
    });

    // Benchmark: Simple query with parameters
    group.bench_function("raw_sql_with_params", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT ?", (42,));
            black_box(result).unwrap();
        });
    });

    // Benchmark: Count queries
    group.bench_function("count_query", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            let result: Result<i64, AkitaError> = akita.exec_first("SELECT COUNT(*) FROM t_system_user", ());
            black_box(result).unwrap();
        });
    });

    // Benchmark: Minimal insertion (only necessary fields are inserted)
    group.bench_function("minimal_insert", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            let pk = Uuid::new_v4().simple().to_string();
            let sql = "INSERT INTO t_system_user (pk, tenant_id, status, level, gender, token) VALUES (?, 0, 1, 0, 1, 'test')";
            let result = akita.exec_drop(sql, (pk,));
            black_box(result).unwrap();
        });
    });

    group.finish();
}

pub fn bench_connection_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("connection_pool");
    group.measurement_time(Duration::from_secs(5));

    // Test the performance of different connection pool configurations
    for pool_size in [1, 3, 5, 10].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::new("pool_size", pool_size),
            pool_size,
            |b, &size| {
                b.iter(|| {
                    let config = create_test_akita_cfg().max_size(size);
                    let akita = Akita::new(config).unwrap();

                    let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT ?", (size,));
                    black_box(result).unwrap();
                });
            },
        );
    }

    group.finish();
}

pub fn bench_concurrent_connections(c: &mut Criterion) {
    use std::sync::Arc;

    let akita = Arc::new(create_bench_akita());

    let mut group = c.benchmark_group("concurrent_connections");
    group.measurement_time(Duration::from_secs(10));

    for thread_count in [1, 2, 4].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::new("concurrent_queries", thread_count),
            thread_count,
            |b, &count| {
                b.iter_custom(|iterations| {
                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..count)
                        .map(|_| {
                            let akita = Arc::clone(&akita);
                            let iterations_per_thread = iterations / count as u64;

                            std::thread::spawn(move || {
                                for i in 0..iterations_per_thread {
                                    let result: Result<Option<i64>, AkitaError> =
                                        akita.exec_first("SELECT ?", (i,));
                                    black_box(result).unwrap();
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

pub fn bench_network_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("network_latency");
    group.measurement_time(Duration::from_secs(3));

    // Test multiple consecutive queries to measure the network round trip time
    group.bench_function("ping_pong_queries", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            for i in 0..10 {
                let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT ?", (i,));
                black_box(result).unwrap();
            }
        });
    });

    // Testing batch queries vs individual queries
    group.bench_function("batch_vs_single", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            // A single query is executed 10 times
            let mut results = Vec::new();
            for i in 0..10 {
                let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT ?", (i,));
                results.push(black_box(result).unwrap());
            }
            results
        });
    });

    group.finish();
}

pub fn bench_prepared_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_statements");
    group.measurement_time(Duration::from_secs(5));

    // Testing multiple executions of the same query (possibly using preprocessed statements)
    group.bench_function("repeated_same_query", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            for _ in 0..10 {
                let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT 1", ());
                black_box(result).unwrap();
            }
        });
    });

    // Test multiple executions of different queries
    group.bench_function("repeated_different_queries", |b| {
        let akita = create_bench_akita();
        b.iter(|| {
            for i in 0..10 {
                let result: Result<Option<i64>, AkitaError> = akita.exec_first("SELECT ?", (i,));
                black_box(result).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.05)
        .noise_threshold(0.02)
        .warm_up_time(Duration::from_secs(2));
    targets = 
        bench_connection_operations,
        bench_connection_pool,
        bench_concurrent_connections,
        bench_network_latency,
        bench_prepared_statements
);

criterion_main!(benches);