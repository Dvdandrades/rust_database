use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rust_database::{DbStore, WriteBatch};
use std::hint::black_box;
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const VALUE_SIZE: usize = 256;
const LOOKUPS: usize = 4096;

struct Fixture {
    store: DbStore,
    _dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        static NOTICE: Once = Once::new();
        let dir = match std::env::var_os("BENCH_DIR") {
            Some(base) => {
                tempfile::tempdir_in(base).expect("BENCH_DIR must be a writable directory")
            }
            None => {
                NOTICE.call_once(|| {
                    eprintln!("note: BENCH_DIR not set, using the system temp dir");
                });
                tempfile::tempdir().unwrap()
            }
        };
        let store = DbStore::open(dir.path()).unwrap();
        Self { store, _dir: dir }
    }

    fn with_keys(n: u64) -> Self {
        let f = Self::new();
        fill(&f.store, n, VALUE_SIZE, 1);
        f
    }
}

struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn below(&mut self, n: u64) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x % n
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn new_key(i: u64) -> Vec<u8> {
    format!("n{i:012}").into_bytes()
}

fn fill(store: &DbStore, n: u64, value_size: usize, versions: u32) {
    let value = vec![0xAB; value_size];
    for _ in 0..versions {
        let mut start = 0;
        while start < n {
            let end = (start + 1000).min(n);
            let mut batch = WriteBatch::with_capacity((end - start) as usize);
            for i in start..end {
                batch.put(key(i), value.as_slice());
            }
            store.write_batch(batch).unwrap();
            start = end;
        }
    }
}

fn random_lookups(n: u64) -> Vec<Vec<u8>> {
    let mut rng = XorShift::new(0x9E37_79B9_7F4A_7C15);
    (0..LOOKUPS).map(|_| key(rng.below(n))).collect()
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    group.sample_size(30);

    for size in [16usize, 256, 4096] {
        let f = Fixture::new();
        let value = vec![0xAB; size];
        let mut next = 0u64;

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("new_key", size), &size, |b, _| {
            b.iter_batched(
                || {
                    next += 1;
                    (new_key(next), value.clone())
                },
                |(k, v)| f.store.put(black_box(k), black_box(v)).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }

    {
        let f = Fixture::with_keys(1000);
        let value = vec![0xAB; VALUE_SIZE];
        let mut next = 0u64;

        group.throughput(Throughput::Bytes(VALUE_SIZE as u64));
        group.bench_function("overwrite_hot_set", |b| {
            b.iter_batched(
                || {
                    next += 1;
                    (key(next % 1000), value.clone())
                },
                |(k, v)| f.store.put(black_box(k), black_box(v)).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");
    group.sample_size(20);

    for ops in [1usize, 10, 100, 1000] {
        let f = Fixture::new();
        let value = vec![0xAB; VALUE_SIZE];
        let mut next = 0u64;

        group.throughput(Throughput::Elements(ops as u64));
        group.bench_with_input(BenchmarkId::from_parameter(ops), &ops, |b, &ops| {
            b.iter_batched(
                || {
                    let mut batch = WriteBatch::with_capacity(ops);
                    for _ in 0..ops {
                        next += 1;
                        batch.put(new_key(next), value.as_slice());
                    }
                    batch
                },
                |batch| f.store.write_batch(black_box(batch)).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.sample_size(30);

    group.bench_function("existing_key", |b| {
        b.iter_custom(|iters| {
            let f = Fixture::new();
            fill(&f.store, iters, VALUE_SIZE, 1);
            let keys: Vec<Vec<u8>> = (0..iters).map(key).collect();

            let start = Instant::now();
            for k in keys {
                f.store.delete(black_box(k)).unwrap();
            }
            start.elapsed()
        });
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    for n in [1_000u64, 100_000] {
        let f = Fixture::with_keys(n);
        let hits = random_lookups(n);
        let misses: Vec<Vec<u8>> = (0..LOOKUPS)
            .map(|i| format!("absent_{i}").into_bytes())
            .collect();

        group.throughput(Throughput::Bytes(VALUE_SIZE as u64));
        group.bench_with_input(BenchmarkId::new("hit", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let v = f.store.get(black_box(&hits[i & (LOOKUPS - 1)])).unwrap();
                i += 1;
                black_box(v)
            });
        });

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("miss", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let r = f.store.get(black_box(&misses[i & (LOOKUPS - 1)]));
                i += 1;
                black_box(r.is_err())
            });
        });
    }

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    let f = Fixture::with_keys(10_000);

    for (prefix, count) in [("k00001", 10u64), ("k0000", 100), ("k000", 1000)] {
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(BenchmarkId::new("prefix", count), &prefix, |b, &p| {
            b.iter(|| {
                let mut seen = 0u64;
                for item in f.store.scan_prefix(black_box(p)) {
                    black_box(item.unwrap());
                    seen += 1;
                }
                assert_eq!(seen, count);
            });
        });

        group.bench_with_input(BenchmarkId::new("keys_only", count), &prefix, |b, &p| {
            b.iter(|| {
                let keys = f.store.keys_with_prefix(black_box(p));
                assert_eq!(keys.len() as u64, count);
                black_box(keys)
            });
        });
    }

    group.throughput(Throughput::Elements(100));
    group.bench_function("range/100", |b| {
        b.iter(|| {
            let mut seen = 0u64;
            for item in f
                .store
                .scan_range(black_box("k000500")..black_box("k000600"))
            {
                black_box(item.unwrap());
                seen += 1;
            }
            assert_eq!(seen, 100);
        });
    });

    group.finish();
}

fn bench_concurrent_reads(c: &mut Criterion) {
    const OPS_PER_THREAD: usize = 500;

    let mut group = c.benchmark_group("get_concurrent");
    group.sample_size(30);

    let f = Fixture::with_keys(10_000);
    let lookups = random_lookups(10_000);

    for threads in [1usize, 2, 4] {
        group.throughput(Throughput::Elements((threads * OPS_PER_THREAD) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, &t| {
            b.iter(|| {
                thread::scope(|s| {
                    for tid in 0..t {
                        let store = &f.store;
                        let keys = &lookups;
                        s.spawn(move || {
                            for j in 0..OPS_PER_THREAD {
                                let k = &keys[(tid * OPS_PER_THREAD + j) & (LOOKUPS - 1)];
                                black_box(store.get(k).unwrap());
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

fn bench_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("compact");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    group.throughput(Throughput::Elements(5_000));
    group.bench_function("5k_live_x3_versions", |b| {
        b.iter_batched_ref(
            || {
                let f = Fixture::new();
                fill(&f.store, 5_000, VALUE_SIZE, 3);
                f
            },
            |f| f.store.compact().unwrap(),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("open_recovery");
    group.sample_size(20);

    for n in [10_000u64, 100_000] {
        let f = Fixture::with_keys(n);
        let dir = f._dir.path().to_path_buf();
        drop(f.store);

        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(DbStore::open(black_box(&dir)).unwrap()));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_write_batch,
    bench_delete,
    bench_get,
    bench_scan,
    bench_concurrent_reads,
    bench_compact,
    bench_open
);
criterion_main!(benches);
