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
use akita::prelude::*;

mod common;
use common::*;

pub fn bench_transaction_operations(c: &mut Criterion) {
    let akita = create_bench_akita();

    let mut group = c.benchmark_group("transaction_operations");
    group.measurement_time(Duration::from_secs(5));

    // Benchmark: Transaction operations
    group.bench_function("transaction_commit", |b| {
        b.iter(|| {
            black_box(akita.start_transaction()).and_then(|mut tx| {
                let user = create_test_user();
                tx.save::<SysUser, i64>(black_box(&user))?;
                tx.commit()
            }).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .sample_size(10);
    targets = bench_transaction_operations
);

criterion_main!(benches);