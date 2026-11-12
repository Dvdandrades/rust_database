mod common;

use common::*;
use rust_database::{DbStore, Record};
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use tempfile::tempdir;

fn open_with_segment(dir: &Path, bytes: &[u8]) -> rust_database::Result<DbStore> {
    fs::write(segment_path(dir, 0), bytes).unwrap();
    DbStore::open(dir)
}

fn assert_usable_after_recovery(dir: &Path, db: DbStore, expected: &Model, label: &str) {
    const PROBE: &[u8] = b"__probe__";
    db.put(PROBE.to_vec(), b"alive".to_vec()).unwrap();
    drop(db);

    let mut expected = expected.clone();
    expected.insert(PROBE.to_vec(), b"alive".to_vec());
    assert_same_state(
        &format!("{label}: after a write + restart"),
        &expected,
        &dump(&open(dir)),
    );
}

#[test]
fn truncation_at_every_byte_yields_a_consistent_prefix() {
    let reference = build_reference_log();

    for cut in 0..=reference.bytes.len() {
        let dir = tempdir().unwrap();
        let label = format!("truncated at byte {cut}/{}", reference.bytes.len());

        let db = open_with_segment(dir.path(), &reference.bytes[..cut])
            .unwrap_or_else(|e| panic!("{label}: open failed: {e}"));
        let expected = reference.expected_for_prefix(cut);
        assert_same_state(&label, expected, &dump(&db));

        let at_step_boundary = reference.ends.contains(&cut);
        if at_step_boundary || cut % 5 == 0 {
            assert_usable_after_recovery(dir.path(), db, expected, &label);
        }
    }
}

#[test]
fn bit_flips_at_every_byte_yield_a_consistent_prefix() {
    let reference = build_reference_log();

    for mask in [0x01u8, 0xFF] {
        for index in 0..reference.bytes.len() {
            let mut damaged = reference.bytes.clone();
            damaged[index] ^= mask;

            let dir = tempdir().unwrap();
            let label = format!("byte {index} xor {mask:#04x}");

            let db = open_with_segment(dir.path(), &damaged)
                .unwrap_or_else(|e| panic!("{label}: open failed: {e}"));

            let expected = reference.expected_for_prefix(index);
            assert_same_state(&label, expected, &dump(&db));
        }
    }
}

#[test]
fn valid_log_with_garbage_tail_recovers_everything() {
    let reference = build_reference_log();
    let mut rng = Rng::new(0xBAD_7A11);

    for len in 1..=64 {
        let mut bytes = reference.bytes.clone();
        bytes.extend(rng.bytes(len));

        let dir = tempdir().unwrap();
        let db = open_with_segment(dir.path(), &bytes)
            .unwrap_or_else(|e| panic!("tail of {len} garbage bytes: open failed {e}"));
        let expected = reference.states.last().unwrap();
        assert_same_state(
            &format!("tail of {len} garbage bytes"),
            expected,
            &dump(&db),
        );
        assert_usable_after_recovery(dir.path(), db, expected, &format!("tail of {len} bytes"));
    }
}

#[test]
fn arbitrary_garbage_logs_never_panic() {
    let reference = build_reference_log();
    let iterations = 400 * scale();

    for i in 0..iterations {
        let mut rng = Rng::new(0xF00D_0000 + i as u64);
        let bytes = match rng.below(5) {
            0 => rng.bytes_up_to(512),
            1 => {
                let mut b = reference.bytes.clone();
                let start = rng.below(b.len());
                let end = (start + 1 + rng.below(16)).min(b.len());
                for byte in &mut b[start..end] {
                    *byte = rng.next_u64() as u8;
                }
                b
            }
            2 => {
                let mut b = reference.bytes.clone();
                let at = rng.below(b.len());
                if rng.chance(50) {
                    b.insert(at, rng.next_u64() as u8);
                } else {
                    b.remove(at);
                }
                b
            }
            3 => {
                let mut b = rng.bytes(4);
                b.extend(((rng.next_u64()) as u32 | 0x8000_0000).to_le_bytes());
                b.extend(rng.bytes_up_to(32));
                b
            }
            _ => [reference.bytes.clone(), reference.bytes.clone()].concat(),
        };

        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            let dir = tempdir().unwrap();

            if let Ok(db) = open_with_segment(dir.path(), &bytes) {
                let live = dump(&db);
                for (k, v) in &live {
                    assert_eq!(&db.get(k).unwrap(), v);
                }
                db.put(b"__probe__".to_vec(), b"alive".to_vec()).unwrap();
                drop(db);
                let reopened = open(dir.path());
                assert_eq!(reopened.get(b"__probe__").unwrap(), b"alive".to_vec());
            }
        }));

        if let Err(cause) = outcome {
            let repro = std::env::temp_dir().join(format!("db_garbage_repro_{i}.log"));
            fs::write(&repro, &bytes).unwrap();
            let msg = cause
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| cause.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!(
                "iteration {i} panicked: {msg}\nreproducer saved to {}",
                repro.display()
            );
        }
    }
}

#[test]
fn record_deserialize_never_panics_on_garbage() {
    let valid = Record::new_put(b"key".to_vec(), b"value".to_vec())
        .serialize()
        .unwrap();
    let mut rng = Rng::new(0xDEC0DE);
    let mut inputs: Vec<Vec<u8>> = vec![Vec::new(), vec![0], vec![0xFF; 64]];

    for cut in 0..valid.len() {
        inputs.push(valid[..cut].to_vec());
    }

    for i in 0..valid.len() {
        for mask in [0x01, 0x80, 0xFF] {
            let mut b = valid.clone();
            b[i] ^= mask;
            inputs.push(b);
        }
    }

    for len_prefix in [u64::MAX, u64::MAX / 2, 1 << 40, 1 << 32, u32::MAX as u64] {
        let mut b = vec![1u8];
        b.extend(len_prefix.to_le_bytes());
        b.extend(rng.bytes(8));
        inputs.push(b);
    }

    for _ in 0..50_000 {
        inputs.push(rng.bytes_up_to(80));
    }

    for input in inputs {
        let Ok(record) = Record::deserialize(&input) else {
            continue;
        };

        let again = Record::deserialize(&record.serialize().unwrap())
            .expect("re-serialized record must deserialize");
        assert_eq!(record, again, "input {input:02x?}");
    }
}
