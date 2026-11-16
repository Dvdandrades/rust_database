mod common;

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use common::ScratchDir;
use rust_database::{DbError, DbStore, WriteBatch};

#[derive(Debug)]
enum AppError {
    NoteNotFound(u64),
    InvalidInput(String),
    BadData(String),
    Storage(DbError),
}

impl From<DbError> for AppError {
    fn from(err: DbError) -> Self {
        AppError::Storage(err)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NoteNotFound(id) => write!(f, "note {id} not found"),
            AppError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            AppError::BadData(msg) => write!(f, "corrupt stored data: {msg}"),
            AppError::Storage(err) => write!(f, "storage error: {err}"),
        }
    }
}

impl std::error::Error for AppError {}

#[derive(Debug, Clone, PartialEq)]
struct Note {
    id: u64,
    title: String,
    body: String,
    tags: Vec<String>,
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], AppError> {
        if self.bytes.len() < n {
            return Err(AppError::BadData("value is truncated".into()));
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }

    fn u32(&mut self) -> Result<u32, AppError> {
        let raw: [u8; 4] = self.take(4)?.try_into().expect("take(4) returns 4 bytes");
        Ok(u32::from_le_bytes(raw))
    }

    fn string(&mut self) -> Result<String, AppError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|e| AppError::BadData(format!("invalid UTF-8: {e}")))
    }
}

impl Note {
    fn encode_value(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_str(&mut buf, &self.title);
        put_str(&mut buf, &self.body);
        buf.extend_from_slice(&(self.tags.len() as u32).to_le_bytes());
        for tag in &self.tags {
            put_str(&mut buf, tag);
        }
        buf
    }

    fn decode(id: u64, value: &[u8]) -> Result<Self, AppError> {
        let mut r = Reader { bytes: value };
        let title = r.string()?;
        let body = r.string()?;
        let tag_count = r.u32()?;
        let tags = (0..tag_count)
            .map(|_| r.string())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Note {
            id,
            title,
            body,
            tags,
        })
    }
}

const NOTE_PREFIX: &str = "note:";

fn note_key(id: u64) -> Vec<u8> {
    format!("{NOTE_PREFIX}{id:016x}").into_bytes()
}

fn tag_prefix(tag: &str) -> String {
    format!("tag:{tag}:")
}

fn tag_key(tag: &str, id: u64) -> Vec<u8> {
    format!("{}{id:016x}", tag_prefix(tag)).into_bytes()
}

fn id_from_key(key: &[u8]) -> Result<u64, AppError> {
    let bad = || AppError::BadData(format!("unexpected key {:?}", String::from_utf8_lossy(key)));
    let hex = key.get(key.len().wrapping_sub(16)..).ok_or_else(bad)?;
    let hex = std::str::from_utf8(hex).map_err(|_| bad())?;
    u64::from_str_radix(hex, 16).map_err(|_| bad())
}

struct NoteStore {
    db: DbStore,
    next_id: AtomicU64,
}

impl NoteStore {
    fn open(path: &Path) -> Result<Self, AppError> {
        let db = DbStore::open(path)?;

        let last_id = match db.scan_prefix(NOTE_PREFIX).next_back() {
            Some(entry) => id_from_key(&entry?.0)?,
            None => 0,
        };

        Ok(Self {
            db,
            next_id: AtomicU64::new(last_id + 1),
        })
    }

    fn create(&self, title: &str, body: &str, tags: &[&str]) -> Result<u64, AppError> {
        if let Some(bad) = tags.iter().find(|t| t.is_empty() || t.contains(':')) {
            return Err(AppError::InvalidInput(format!("bad tag {bad:?}")));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let note = Note {
            id,
            title: title.to_owned(),
            body: body.to_owned(),
            tags: tags.iter().map(|t| (*t).to_owned()).collect(),
        };

        let mut batch = WriteBatch::with_capacity(1 + tags.len());
        batch.put(note_key(id), note.encode_value());
        for tag in tags {
            batch.put(tag_key(tag, id), Vec::new());
        }
        self.db.write_batch(batch)?;
        Ok(id)
    }

    fn get(&self, id: u64) -> Result<Note, AppError> {
        match self.db.get(&note_key(id)) {
            Ok(value) => Note::decode(id, &value),
            Err(DbError::KeyNotFound) => Err(AppError::NoteNotFound(id)),
            Err(other) => Err(other.into()),
        }
    }

    fn update_body(&self, id: u64, body: &str) -> Result<(), AppError> {
        let mut note = self.get(id)?;
        note.body = body.to_owned();
        self.db.put(note_key(id), note.encode_value())?;
        Ok(())
    }

    fn delete(&self, id: u64) -> Result<(), AppError> {
        let note = self.get(id)?;
        let mut batch = WriteBatch::with_capacity(1 + note.tags.len());
        batch.delete(note_key(id));
        for tag in &note.tags {
            batch.delete(tag_key(tag, id));
        }
        self.db.write_batch(batch)?;
        Ok(())
    }

    fn count(&self) -> usize {
        self.db.keys_with_prefix(NOTE_PREFIX).len()
    }

    fn count_with_tag(&self, tag: &str) -> usize {
        self.db.keys_with_prefix(tag_prefix(tag)).len()
    }

    fn with_tag(&self, tag: &str) -> Result<Vec<Note>, AppError> {
        let mut notes = Vec::new();
        for id in self.db.keys_with_prefix(tag_prefix(tag)) {
            notes.push(self.get(id_from_key(&id)?)?);
        }
        Ok(notes)
    }

    fn recent(&self, n: usize) -> Result<Vec<Note>, AppError> {
        self.db
            .scan_prefix(NOTE_PREFIX)
            .rev()
            .take(n)
            .map(|entry| {
                let (key, value) = entry?;
                Note::decode(id_from_key(&key)?, &value)
            })
            .collect()
    }

    fn compact_if_larger_than(&self, max_bytes: u64) -> Result<bool, AppError> {
        if self.db.metrics().disk_size_bytes <= max_bytes {
            return Ok(false);
        }
        self.db.compact()?;
        Ok(true)
    }
}

const TAGS: [&str; 4] = ["rust", "db", "ideas", "todo"];
const WORKERS: usize = 4;
const NOTES_PER_WORKER: usize = 25;

fn main() -> Result<(), AppError> {
    let dir = ScratchDir::new("embedded_app");
    let store = Arc::new(NoteStore::open(dir.path())?);

    let workers: Vec<_> = (0..WORKERS)
        .map(|w| {
            let store = Arc::clone(&store);
            thread::spawn(move || -> Result<(), AppError> {
                for n in 0..NOTES_PER_WORKER {
                    let tag = TAGS[(w + n) % TAGS.len()];
                    store.create(&format!("note {w}-{n}"), "some text", &[tag, "all"])?;
                }
                Ok(())
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker thread panicked")?;
    }

    let total = WORKERS * NOTES_PER_WORKER;
    println!("created {} notes from {WORKERS} threads", store.count());
    assert_eq!(store.count(), total);
    assert_eq!(store.count_with_tag("all"), total);

    let expected_rust = (0..WORKERS)
        .flat_map(|w| (0..NOTES_PER_WORKER).map(move |n| (w, n)))
        .filter(|(w, n)| (w + n) % TAGS.len() == 0)
        .count();
    assert_eq!(store.count_with_tag("rust"), expected_rust);
    println!("{expected_rust} of them are tagged #rust");

    let newest = store.recent(3)?;
    println!(
        "newest three ids: {:?}",
        newest.iter().map(|n| n.id).collect::<Vec<_>>()
    );
    assert_eq!(newest[0].id, total as u64);

    let rust_notes = store.with_tag("rust")?;
    assert_eq!(rust_notes.len(), expected_rust);
    assert!(
        rust_notes
            .iter()
            .all(|n| n.tags.iter().any(|t| t == "rust"))
    );

    match store.get(9_999) {
        Err(AppError::NoteNotFound(id)) => println!("note {id}: not found (as expected)"),
        other => panic!("unexpected: {other:?}"),
    }
    match store.create("bad", "tag with a colon", &["a:b"]) {
        Err(AppError::InvalidInput(msg)) => println!("rejected: {msg}"),
        other => panic!("unexpected: {other:?}"),
    }

    for id in 1..=10 {
        store.delete(id)?;
    }
    assert_eq!(store.count(), total - 10);
    assert_eq!(store.count_with_tag("all"), total - 10);
    println!(
        "deleted 10 notes; {} remain, index still consistent",
        store.count()
    );

    let hot = total as u64;
    for revision in 0..200 {
        store.update_body(hot, &format!("revision {revision}, padded with some text"))?;
    }
    let before = store.db.metrics().disk_size_bytes;
    let compacted = store.compact_if_larger_than(10 * 1024)?;
    let after = store.db.metrics().disk_size_bytes;
    println!("log size: {before} bytes -> {after} bytes (compacted: {compacted})");
    assert!(compacted && after < before);
    assert_eq!(store.get(hot)?.body, "revision 199, padded with some text");

    let remaining = store.count();
    drop(
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| unreachable!("all worker threads have been joined")),
    );

    let store = NoteStore::open(dir.path())?;
    assert_eq!(store.count(), remaining);
    let next = store.create("after restart", "still works", &["rust"])?;
    assert_eq!(next, total as u64 + 1);
    println!("reopened: {remaining} notes recovered, next id is {next}");

    let m = store.db.metrics();
    println!(
        "metrics since reopening: {} batch ({} ops), {} live keys, {} bytes on disk",
        m.batches, m.batch_ops, m.live_keys, m.disk_size_bytes
    );

    Ok(())
}
