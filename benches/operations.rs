use criterion::{Criterion, criterion_group, criterion_main};
use rust_database::DbStore;
use std::hint::black_box;
use tempfile::tempdir;

fn bench_put_and_get(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bench.db");
    let mut store = DbStore::open(&db_path).unwrap();

    let mut group = c.benchmark_group("dbstore-ops");

    group.bench_function("put_operation", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            store.put(black_box(key), black_box(value)).unwrap();
            i += 1;
        });
    });

    for i in 0..1000 {
        let _ = store.put(
            format!("read_key_{}", i).into_bytes(),
            b"some_value".to_vec(),
        );
    }

    group.bench_function("get_operation", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("read_key_{}", i % 1000).into_bytes();
            let _ = store.get(black_box(&key)).unwrap();
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_put_and_get);
criterion_main!(benches);
