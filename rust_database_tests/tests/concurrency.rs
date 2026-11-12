mod common;

use common::*;
use rust_database::{DbError, DbStore, WriteBatch};
use std::collections::HashMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use tempfile::tempdir;

struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn disjoint_key(t: usize, i: usize) -> Vec<u8> {
    format!("w{t}:{i:05}").into_bytes()
}

fn disjoint_value(t: usize, i: usize) -> Vec<u8> {
    format!("{t}-{i}{}", "x".repeat(i % 61)).into_bytes()
}

#[test]
fn disjoint_writers_are_all_durable() {
    const THREADS: usize = 8;
    let per_thread = 100 * scale();
    let total = THREADS * per_thread;

    let dir = tempdir().unwrap();
    let db = open(dir.path());
    let barrier = Barrier::new(THREADS);

    thread::scope(|s| {
        for t in 0..THREADS {
            let (db, barrier) = (&db, &barrier);
            s.spawn(move || {
                barrier.wait();
                for i in 0..per_thread {
                    db.put(disjoint_key(t, i), disjoint_value(t, i)).unwrap();
                }
            });
        }
    });

    let check_all = |db: &DbStore| {
        for t in 0..THREADS {
            for i in 0..per_thread {
                assert_eq!(
                    db.get(&disjoint_key(t, i)).unwrap(),
                    disjoint_value(t, i),
                    "w{t}:{i}"
                );
            }
        }
    };
    check_all(&db);

    let m = db.metrics();
    assert_eq!(
        m.puts as usize, total,
        "no put may be lost or double counted"
    );
    assert_eq!(m.live_keys as usize, total);

    let live = dump(&db);
    assert_eq!(live.len(), total);
    drop(db);

    let reopened = open(dir.path());
    check_all(&reopened);
    assert_same_state("after restart", &live, &dump(&reopened));
}

fn versioned_key(w: usize, j: usize) -> String {
    format!("k{w}:{j}")
}

fn versioned_value(key: &str, generation: usize) -> Vec<u8> {
    format!("{generation:08}|{key}|{}", "p".repeat(generation % 50)).into_bytes()
}

fn parse_versioned(bytes: &[u8]) -> (usize, String) {
    let text = std::str::from_utf8(bytes).expect("torn value: not UTF-8");
    let mut parts = text.splitn(3, '|');
    let generation = parts
        .next()
        .and_then(|g| g.parse().ok())
        .unwrap_or_else(|| panic!("torn value: bad generation in {text:?}"));
    let key = parts
        .next()
        .unwrap_or_else(|| panic!("torn value: missing key in {text:?}"));
    (generation, key.to_string())
}

#[test]
fn readers_see_whole_and_monotonic_values_while_writers_overwrite() {
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const KEYS_PER_WRITER: usize = 6;
    let generations = 25 * scale();

    let dir = tempdir().unwrap();
    let db = open(dir.path());

    for w in 0..WRITERS {
        for j in 0..KEYS_PER_WRITER {
            let key = versioned_key(w, j);
            db.put(key.clone().into_bytes(), versioned_value(&key, 0))
                .unwrap();
        }
    }

    let done = AtomicBool::new(false);
    let completed_reads = AtomicUsize::new(0);

    thread::scope(|s| {
        let _stop = StopOnDrop(&done);

        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let db = &db;
                s.spawn(move || {
                    for generation in 1..=generations {
                        for j in 0..KEYS_PER_WRITER {
                            let key = versioned_key(w, j);
                            db.put(key.clone().into_bytes(), versioned_value(&key, generation))
                                .unwrap();
                        }
                    }
                })
            })
            .collect();

        for r in 0..READERS {
            let (db, done, completed_reads) = (&db, &done, &completed_reads);
            s.spawn(move || {
                let mut rng = Rng::new(100 + r as u64);
                let mut last_seen: HashMap<String, usize> = HashMap::new();

                while !done.load(Ordering::Acquire) {
                    let (w, j) = (rng.below(WRITERS), rng.below(KEYS_PER_WRITER));
                    let key = versioned_key(w, j);

                    let bytes = db
                        .get(key.as_bytes())
                        .unwrap_or_else(|e| panic!("get({key}) failed under load: {e}"));
                    let (generation, embedded_key) = parse_versioned(&bytes);
                    assert_eq!(embedded_key, key, "value belongs to a different key");

                    let previous = last_seen.insert(key.clone(), generation).unwrap_or(0);
                    assert!(
                        generation >= previous,
                        "{key}: generation went backwards ({previous} -> {generation})"
                    );

                    if rng.chance(5) {
                        for item in db.scan_prefix(format!("k{w}:")) {
                            let (k, v) = item.expect("scan entry must be readable");
                            let (_, embedded) = parse_versioned(&v);
                            assert_eq!(
                                embedded.as_bytes(),
                                &k[..],
                                "scan returned a foreign value"
                            );
                        }
                    }
                    completed_reads.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        for writer in writers {
            writer.join().unwrap();
        }
    });

    assert!(
        completed_reads.load(Ordering::Relaxed) > 0,
        "readers never ran concurrently with the writers; the test proves nothing"
    );

    for w in 0..WRITERS {
        for j in 0..KEYS_PER_WRITER {
            let key = versioned_key(w, j);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                versioned_value(&key, generations),
                "{key} must end at its last generation"
            );
        }
    }

    let m = db.metrics();
    assert_eq!(m.get_misses, 0);
    assert_eq!(
        m.reads,
        m.get_hits + m.get_misses,
        "every get is a hit or a miss"
    );
}

#[test]
fn batches_are_atomic_for_concurrent_scans() {
    const KEYS: usize = 6;
    const WRITERS: usize = 3;
    const READERS: usize = 3;
    let rounds = 40 * scale();

    let put_all = |token: usize| {
        let mut batch = WriteBatch::with_capacity(KEYS);
        for k in 0..KEYS {
            batch.put(format!("acct:{k}"), token.to_string());
        }
        batch
    };
    let delete_all = || {
        let mut batch = WriteBatch::with_capacity(KEYS);
        for k in 0..KEYS {
            batch.delete(format!("acct:{k}"));
        }
        batch
    };

    let dir = tempdir().unwrap();
    let db = open(dir.path());
    db.write_batch(put_all(0)).unwrap();

    let done = AtomicBool::new(false);
    let populated_snapshots = AtomicUsize::new(0);
    let empty_snapshots = AtomicUsize::new(0);

    thread::scope(|s| {
        let _stop = StopOnDrop(&done);

        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let db = &db;
                let (put_all, delete_all) = (&put_all, &delete_all);
                s.spawn(move || {
                    for n in 1..=rounds {
                        if n % 4 == 0 {
                            db.write_batch(delete_all()).unwrap();
                        } else {
                            db.write_batch(put_all(w * 1_000_000 + n)).unwrap();
                        }
                    }
                })
            })
            .collect();

        for _ in 0..READERS {
            let (db, done) = (&db, &done);
            let (populated, empty) = (&populated_snapshots, &empty_snapshots);
            s.spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let rows: Vec<_> = db
                        .scan_prefix("acct:")
                        .collect::<Result<_, _>>()
                        .expect("scan entry must be readable");

                    match rows.len() {
                        0 => {
                            empty.fetch_add(1, Ordering::Relaxed);
                        }
                        KEYS => {
                            let first = &rows[0].1;
                            assert!(
                                rows.iter().all(|(_, v)| v == first),
                                "scan oberserved a half-applied batch: {:?}",
                                rows.iter()
                                    .map(|(k, v)| (
                                        String::from_utf8_lossy(k).into_owned(),
                                        String::from_utf8_lossy(v).into_owned()
                                    ))
                                    .collect::<Vec<_>>()
                            );
                            populated.fetch_add(1, Ordering::Relaxed);
                        }
                        n => panic!("scan oberserved a partially applied batch: {n}/{KEYS} keys"),
                    }
                }
            });
        }

        for writer in writers {
            writer.join().unwrap();
        }
    });

    assert!(
        populated_snapshots.load(Ordering::Relaxed) + empty_snapshots.load(Ordering::Relaxed) > 0,
        "readers never ran concurrently with the writers; the test proves nothing"
    );

    let live = dump(&db);
    drop(db);
    assert_same_state("after restart", &live, &dump(&open(dir.path())));
}

#[test]
fn contended_keys_memory_state_matches_disk_state() {
    const THREADS: usize = 8;
    const KEYS: usize = 5;
    let ops = 60 * scale();

    let dir = tempdir().unwrap();
    let db = open(dir.path());
    let barrier = Barrier::new(THREADS);

    thread::scope(|s| {
        for t in 0..THREADS {
            let (db, barrier) = (&db, &barrier);
            s.spawn(move || {
                let mut rng = Rng::new(7_000 + t as u64);
                barrier.wait();
                for i in 0..ops {
                    let key = format!("hot:{}", rng.below(KEYS));
                    match rng.below(10) {
                        0..=5 => db
                            .put(key.into_bytes(), format!("t{t}-op{i}").into_bytes())
                            .unwrap(),
                        6..=7 => match db.delete(key.into_bytes()) {
                            Ok(()) | Err(DbError::KeyNotFound) => {}
                            Err(e) => panic!("unexpected delete error: {e}"),
                        },
                        _ => {
                            let mut batch = WriteBatch::new();
                            for n in 0..3 {
                                let k = format!("hot:{}", rng.below(KEYS));
                                if rng.chance(70) {
                                    batch.put(k, format!("t{t}-op{i}-b{n}"));
                                } else {
                                    batch.delete(k);
                                }
                            }
                            db.write_batch(batch).unwrap();
                        }
                    }
                }
            });
        }
    });

    let live = dump(&db);
    drop(db);
    let recovered = dump(&open(dir.path()));
    assert_same_state(
        "live state vs. state recovered from the log",
        &live,
        &recovered,
    );
}

fn model_worker(db: &DbStore, t: usize, ops: usize, barrier: &Barrier) -> Model {
    const KEYSPACE: usize = 12;
    let prefix = format!("t{t}:");
    let key = |n: usize| format!("{prefix}{n:02}").into_bytes();

    let mut rng = Rng::new(0xC0FFEE ^ t as u64);
    let mut model = Model::new();
    let value = |rng: &mut Rng, i: usize| {
        let mut v = format!("v{t}-{i}-").into_bytes();
        let padding = rng.below(40);
        v.extend(rng.bytes(padding));
        v
    };

    barrier.wait();
    for i in 0..ops {
        match rng.below(100) {
            0..=44 => {
                let k = key(rng.below(KEYSPACE));
                let v = value(&mut rng, i);
                db.put(k.clone(), v.clone()).unwrap();
                model.insert(k, v);
            }
            45..=59 => {
                let k = key(rng.below(KEYSPACE));
                match db.delete(k.clone()) {
                    Ok(()) => assert!(
                        model.remove(&k).is_some(),
                        "t{t} op{i}: delete succeeded on a key the model says is absent"
                    ),
                    Err(DbError::KeyNotFound) => assert!(
                        !model.contains_key(&k),
                        "t{t} op{i}: KeyNotFound for a key the model says is present"
                    ),
                    Err(e) => panic!("t{t} op{i}: unexpected delete error: {e}"),
                }
            }
            60..=74 => {
                let mut batch = WriteBatch::new();
                for _ in 0..2 + rng.below(4) {
                    let k = key(rng.below(KEYSPACE));
                    if rng.chance(70) {
                        let v = value(&mut rng, i);
                        batch.put(k.clone(), v.clone());
                        model.insert(k, v);
                    } else {
                        batch.delete(k.clone());
                        model.remove(&k);
                    }
                }
                db.write_batch(batch).unwrap();
            }
            75..=89 => {
                let k = key(rng.below(KEYSPACE));
                match (db.get(&k), model.get(&k)) {
                    (Ok(got), Some(want)) => assert_eq!(&got, want, "t{t}  op{i}: stale value"),
                    (Err(DbError::KeyNotFound), None) => {}
                    (got, want) => panic!("t{t} op{i}: get = {got:?}, model = {want:?}"),
                }
            }
            _ => {
                let rows: Model = db
                    .scan_prefix(prefix.as_bytes())
                    .map(|r| r.expect("scan entry must be readable"))
                    .collect();
                assert_same_state(&format!("t{t} op{i}: scan{prefix}"), &model, &rows);
            }
        }
    }
    model
}

#[test]
fn mixex_workload_matches_per_thread_models() {
    const THREADS: usize = 6;
    let ops = 120 * scale();

    let dir = tempdir().unwrap();
    let db = open(dir.path());
    let barrier = Barrier::new(THREADS);

    let models: Vec<Model> = thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let (db, barrier) = (&db, &barrier);
                s.spawn(move || model_worker(db, t, ops, barrier))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let expected: Model = models.into_iter().flatten().collect();

    let live = dump(&db);
    assert_same_state("live state vs. union of thread models", &expected, &live);
    assert_eq!(db.metrics().live_keys as usize, expected.len());

    drop(db);
    assert_same_state(
        "recovered state vs. union of thread models",
        &expected,
        &dump(&open(dir.path())),
    );
}

#[test]
fn concurrent_deletes_of_the_same_key_only_one_succeeds() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 10;

    let dir = tempdir().unwrap();
    let db = open(dir.path());

    for round in 0..ROUNDS {
        let key = format!("victim{round}").into_bytes();
        db.put(key.clone(), b"v".to_vec()).unwrap();

        let barrier = Barrier::new(THREADS);
        let successes = AtomicUsize::new(0);
        thread::scope(|s| {
            for _ in 0..THREADS {
                let (db, barrier, successes, key) = (&db, &barrier, &successes, &key);
                s.spawn(move || {
                    barrier.wait();
                    match db.delete(key.clone()) {
                        Ok(()) => {
                            successes.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(DbError::KeyNotFound) => {}
                        Err(e) => panic!("unexpected delete error: {e}"),
                    }
                });
            }
        });
        assert_eq!(
            successes.load(Ordering::SeqCst),
            1,
            "round {round}: exactly one delete of the same key may succeed"
        );
    }
    assert_eq!(db.metrics().deletes as usize, ROUNDS);
}
