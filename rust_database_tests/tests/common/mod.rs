#![allow(dead_code)]

use rust_database::{DbStore, WriteBatch};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub type Model = BTreeMap<Vec<u8>, Vec<u8>>;

pub fn scale() -> usize {
    std::env::var("DB_STRESS_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.next_u64() % 100 < percent
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    pub fn bytes_up_to(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.below(max_len);
        self.bytes(len)
    }
}

pub fn dump(db: &DbStore) -> Model {
    db.scan_prefix(b"")
        .map(|item| item.expect("every key in the index must be readable"))
        .collect()
}

pub fn open(path: &Path) -> DbStore {
    DbStore::open(path).expect("database must open")
}

pub fn segment_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("{id:010}.log"))
}

pub fn assert_same_state(what: &str, expected: &Model, actual: &Model) {
    if expected == actual {
        return;
    }
    let show = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    let mut diffs = Vec::new();
    for (k, v) in expected {
        match actual.get(k) {
            None => diffs.push(format!(" missing {:?} (expected {:?})", show(k), show(v))),
            Some(a) if a != v => diffs.push(format!(
                " changed {:?}: expected {:?}, got {:?}",
                show(k),
                show(v),
                show(a)
            )),
            _ => {}
        }
    }
    for (k, v) in actual {
        if !expected.contains_key(k) {
            diffs.push(format!(" unexpected {:?} = {:?}", show(k), show(v)));
        }
    }
    let total = diffs.len();
    diffs.truncate(10);
    panic!(
        "{what}: state mismatch ({total} differences, first {}):\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

#[derive(Clone, Copy)]
pub enum Op {
    Put(&'static str, &'static str),
    Delete(&'static str),
}

pub enum Step {
    Single(Op),
    Batch(&'static [Op]),
}

pub const REFERENCE_STEPS: &[Step] = &[
    Step::Single(Op::Put("a", "1")),
    Step::Single(Op::Put("b", "22")),
    Step::Batch(&[Op::Put("c", "333"), Op::Put("a", "11"), Op::Delete("b")]),
    Step::Single(Op::Put("d", "4444")),
    Step::Single(Op::Delete("a")),
    Step::Batch(&[Op::Put("e", "5"), Op::Put("e", "55"), Op::Delete("zzz")]),
    Step::Single(Op::Put("b", "back")),
];

fn apply_op(model: &mut Model, op: Op) {
    match op {
        Op::Put(k, v) => {
            model.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        }
        Op::Delete(k) => {
            model.remove(k.as_bytes());
        }
    }
}

pub struct Reference {
    pub bytes: Vec<u8>,
    pub states: Vec<Model>,
    pub ends: Vec<usize>,
}

impl Reference {
    pub fn expected_for_prefix(&self, len: usize) -> &Model {
        let complete_steps = self.ends.iter().filter(|end| **end <= len).count();
        &self.states[complete_steps]
    }
}

pub fn build_reference_log() -> Reference {
    let dir = TempDir::new().unwrap();
    let path = segment_path(dir.path(), 0);
    let db = open(dir.path());

    let mut model = Model::new();
    let mut states = vec![model.clone()];
    let mut ends = Vec::new();

    for step in REFERENCE_STEPS {
        match step {
            Step::Single(op) => {
                match *op {
                    Op::Put(k, v) => db.put(k.into(), v.into()).unwrap(),
                    Op::Delete(k) => db.delete(k.into()).unwrap(),
                }
                apply_op(&mut model, *op);
            }
            Step::Batch(ops) => {
                let mut batch = WriteBatch::new();
                for op in *ops {
                    match *op {
                        Op::Put(k, v) => batch.put(k, v),
                        Op::Delete(k) => batch.delete(k),
                    };
                    apply_op(&mut model, *op);
                }
                db.write_batch(batch).unwrap();
            }
        }
        states.push(model.clone());
        ends.push(fs::metadata(&path).unwrap().len() as usize);
    }
    drop(db);

    Reference {
        bytes: fs::read(&path).unwrap(),
        states,
        ends,
    }
}
