mod common;

use common::*;
use rust_database::{DbError, DbStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use tempfile::tempdir;

#[test]
fn writes_after_compaction_survive_a_restart() {
    let dir = tempdir().unwrap();
    {
        let db = open(dir.path());
        db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"a".to_vec(), b"2".to_vec()).unwrap();
        db.put(b"gone".to_vec(), b"x".to_vec()).unwrap();
        db.delete(b"gone".to_vec()).unwrap();

        db.compact().unwrap();

        db.put(b"b".to_vec(), b"after compaction".to_vec()).unwrap();
        assert_eq!(db.get(b"b").unwrap(), b"after compaction".to_vec());
    }

    let db = open(dir.path());
    assert_eq!(db.get(b"a").unwrap(), b"2".to_vec());
    assert!(matches!(db.get(b"gone"), Err(DbError::KeyNotFound)));
    assert_eq!(
        db.get(b"b").unwrap(),
        b"after compaction".to_vec(),
        "a write acknowledged after compact() must be durable"
    );
}

#[test]
fn compaction_can_run_repeatedly() {
    let dir = tempdir().unwrap();
    let db = open(dir.path());

    for round in 0..3 {
        db.put(b"k".to_vec(), format!("round-{round}").into_bytes())
            .unwrap();
        db.compact()
            .unwrap_or_else(|e| panic!("compact #{round} failed: {e}"));
        assert_eq!(
            db.get(b"k").unwrap(),
            format!("round-{round}").into_bytes(),
            "store unreadable after compact #{round}"
        );
    }
}

#[test]
fn compaction_concurrent_with_writers_keeps_latest_values() {
    const WRITERS: usize = 4;
    const KEYS: usize = 8;
    let rounds = 30 * scale();
    let total_puts = WRITERS * KEYS * rounds;

    let dir = tempdir().unwrap();
    let db = open(dir.path());
    let progress = AtomicUsize::new(0);

    let key = |w: usize, j: usize| format!("w{w}:{j}").into_bytes();
    let value = |w: usize, j: usize, round: usize| format!("w{w}-{j}-r{round}").into_bytes();

    thread::scope(|s| {
        for w in 0..WRITERS {
            let (db, progress) = (&db, &progress);
            s.spawn(move || {
                for round in 1..=rounds {
                    for j in 0..KEYS {
                        db.put(key(w, j), value(w, j, round)).unwrap();
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        let (db, progress) = (&db, &progress);
        s.spawn(move || {
            while progress.load(Ordering::Relaxed) < total_puts / 3 {
                thread::yield_now();
            }
            db.compact().expect("compaction under load must succeed");
        });
    });

    let mut wrong = Vec::new();
    for w in 0..WRITERS {
        for j in 0..KEYS {
            let want = value(w, j, rounds);
            match db.get(&key(w, j)) {
                Ok(got) if got == want => {}
                other => wrong.push(format!(
                    "w{w}:{j}: expected {:?}, got {:?}",
                    String::from_utf8_lossy(&want),
                    other.map(|v| String::from_utf8_lossy(&v).into_owned())
                )),
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} keys lost their latest values across a concurrent compaction, e.g.:\n {}",
        wrong.len(),
        WRITERS * KEYS,
        wrong
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n ")
    );

    let live = dump(&db);
    drop(db);
    assert_same_state(
        "after restart",
        &live,
        &dump(&DbStore::open(dir.path()).unwrap()),
    );
}
