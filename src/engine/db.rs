use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{DbError, Result};
use crate::model::record::{BatchOp, Record, RecordType, WriteBatch};
use crate::storage::index::{IndexUpdate, KeyDirEntry, KeyIndex};
use crate::storage::log::LogManager;

pub struct DbStore {
    path: PathBuf,
    log: Mutex<LogManager>,
    index: KeyIndex,
    max_file_size: u64,
}

impl DbStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path_ref)?;

        let max_file_size = 256 * 1024 * 1024;
        let mut log = LogManager::open(&path_ref, max_file_size)?;
        let index = KeyIndex::new();

        let records = log.recover()?;
        for (file_id, pos, record) in records {
            let serialized = record.serialize()?;
            let size = (8 + serialized.len()) as u32;
            let rec_type = RecordType::try_from(record.recid)?;

            match rec_type {
                RecordType::Put => {
                    index.insert(record.key, file_id, pos, size);
                }
                RecordType::Delete => {
                    index.remove(&record.key);
                }
                RecordType::BatchBegin => {
                    return Err(DbError::Corruption(
                        "Batch header found among recovered records".to_string(),
                    ));
                }
            }
        }

        Ok(Self {
            path: path_ref,
            log: Mutex::new(log),
            index,
            max_file_size,
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let record = Record::new_put(key.clone(), value);
        let serialized = record.serialize()?;
        let size = (8 + serialized.len()) as u32;

        let mut log = self.log.lock().unwrap();
        let (file_id, pos) = log.append(&record)?;
        drop(log);

        self.index.insert(key, file_id, pos, size);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let entry = self.index.get(key).ok_or(DbError::KeyNotFound)?;

        let log = self.log.lock().unwrap();
        let record = log.read_at(entry.file_id, entry.offset)?;
        drop(log);

        let rec_type = RecordType::try_from(record.recid)?;

        match rec_type {
            RecordType::Put => Ok(record.value),
            RecordType::Delete => Err(DbError::KeyNotFound),
            RecordType::BatchBegin => Err(DbError::Corruption(
                "Index entry points to a batch header".to_string(),
            )),
        }
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        if self.index.get(&key).is_none() {
            return Err(DbError::KeyNotFound);
        }

        let record = Record::new_delete(key.clone());
        let mut log = self.log.lock().unwrap();
        log.append(&record)?;
        drop(log);

        self.index.remove(&key);
        Ok(())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut records = Vec::with_capacity(batch.len());
        let mut targets = Vec::with_capacity(batch.len());
        for op in batch.into_ops() {
            match op {
                BatchOp::Put { key, value } => {
                    records.push(Record::new_put(key.clone(), value));
                    targets.push((key, true));
                }
                BatchOp::Delete { key } => {
                    records.push(Record::new_delete(key.clone()));
                    targets.push((key, false));
                }
            }
        }

        let mut log = self.log.lock().unwrap();
        let locations = log.append_batch(&records)?;

        let updates = targets
            .into_iter()
            .zip(locations)
            .map(|((key, is_put), loc)| {
                if is_put {
                    IndexUpdate::Put {
                        key,
                        entry: KeyDirEntry {
                            file_id: loc.file_id,
                            offset: loc.offset,
                            size: loc.size,
                        },
                    }
                } else {
                    IndexUpdate::Remove { key }
                }
            });
        self.index.apply_batch(updates);
        drop(log);

        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        let compacted_dir = self.path.join("compacted_temp");
        if compacted_dir.exists() {
            std::fs::remove_dir_all(&compacted_dir)?;
        }

        let mut new_log = LogManager::open(&compacted_dir, self.max_file_size)?;

        let snapshot = self.index.get_all();
        let mut new_index_map = Vec::new();

        let log_guard = self.log.lock().unwrap();
        for (key, entry) in snapshot {
            let record = log_guard.read_at(entry.file_id, entry.offset)?;
            let serialized = record.serialize()?;
            let size = (8 + serialized.len()) as u32;

            let (new_file_id, new_pos) = new_log.append(&record)?;
            new_index_map.push((key, new_file_id, new_pos, size));
        }
        drop(log_guard);

        let _ = {
            let log_guard = self.log.lock().unwrap();
            log_guard.active_file_id()
        };

        let mut log_guard = self.log.lock().unwrap();
        *log_guard = LogManager::open(&compacted_dir, self.max_file_size)?;
        drop(log_guard);

        for (key, file_id, pos, size) in new_index_map {
            self.index.insert(key, file_id, pos, size);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_put_and_get() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let key = b"hello".to_vec();
        let value = b"world".to_vec();

        db.put(key.clone(), value.clone())?;
        let retrieved = db.get(&key)?;
        assert_eq!(retrieved, value);

        Ok(())
    }

    #[test]
    fn test_get_non_existent() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let result = db.get(b"missing_key");
        assert!(matches!(result, Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let key = b"session_id".to_vec();
        let value = b"12345".to_vec();

        db.put(key.clone(), value)?;
        db.delete(key.clone())?;

        let result = db.get(&key);
        assert!(matches!(result, Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_delete_non_existent() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let result = db.delete(b"phantom_key".to_vec());
        assert!(matches!(result, Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_persistence_and_recovery() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        {
            let db = DbStore::open(&path)?;
            db.put(b"user_1".to_vec(), b"David".to_vec())?;
            db.put(b"user_2".to_vec(), b"Ana".to_vec())?;
            db.delete(b"user_1".to_vec())?;
        }

        {
            let db = DbStore::open(&path)?;

            assert!(matches!(db.get(b"user_1"), Err(DbError::KeyNotFound)));

            let val = db.get(b"user_2")?;
            assert_eq!(val, b"Ana".to_vec());
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_reads_and_writes() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = Arc::new(DbStore::open(dir.path())?);

        db.put(b"shared_key".to_vec(), b"initial_value".to_vec())?;

        let mut handles = vec![];

        for _ in 0..10 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let val = db_clone.get(b"shared_key");
                    assert!(val.is_ok());
                }
            }));
        }

        let db_writer = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..50 {
                let key = format!("key_{}", i).into_bytes();
                let val = format!("val_{}", i).into_bytes();
                let _ = db_writer.put(key, val);
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        let val = db.get(b"shared_key")?;
        assert_eq!(val, b"initial_value".to_vec());

        Ok(())
    }

    fn active_segment(path: &std::path::Path) -> std::path::PathBuf {
        let mut segments: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect();
        segments.sort();
        segments.pop().unwrap()
    }

    #[test]
    fn test_write_batch_applies_all_operations() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        db.put(b"to_delete".to_vec(), b"old".to_vec())?;
        db.put(b"to_update".to_vec(), b"old".to_vec())?;

        let mut batch = WriteBatch::new();
        batch
            .put("new_1", "v1")
            .put("new_2", "v2")
            .put("to_update", "new")
            .delete("to_delete");
        db.write_batch(batch)?;

        assert_eq!(db.get(b"new_1")?, b"v1".to_vec());
        assert_eq!(db.get(b"new_2")?, b"v2".to_vec());
        assert_eq!(db.get(b"to_update")?, b"new".to_vec());
        assert!(matches!(db.get(b"to_delete"), Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_write_batch_last_operation_wins() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let mut batch = WriteBatch::new();
        batch
            .put("k", "first")
            .put("k", "second")
            .put("gone", "temp")
            .delete("gone")
            .delete("revived")
            .put("revived", "back");
        db.write_batch(batch)?;

        assert_eq!(db.get(b"k")?, b"second".to_vec());
        assert!(matches!(db.get(b"gone"), Err(DbError::KeyNotFound)));
        assert_eq!(db.get(b"revived")?, b"back".to_vec());

        drop(db);
        let db = DbStore::open(dir.path())?;
        assert_eq!(db.get(b"k")?, b"second".to_vec());
        assert!(matches!(db.get(b"gone"), Err(DbError::KeyNotFound)));
        assert_eq!(db.get(b"revived")?, b"back".to_vec());

        Ok(())
    }

    #[test]
    fn test_write_batch_empty_and_delete_missing_are_ok() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        db.write_batch(WriteBatch::new())?;

        let mut batch = WriteBatch::new();
        batch.delete("never_existed");
        db.write_batch(batch)?;

        assert!(matches!(
            db.get(b"never_existed"),
            Err(DbError::KeyNotFound)
        ));
        Ok(())
    }

    #[test]
    fn test_write_batch_persistence_and_mixed_with_single_ops() -> Result<()> {
        let dir = tempdir().unwrap();

        {
            let db = DbStore::open(dir.path())?;
            db.put(b"before".to_vec(), b"1".to_vec())?;

            let mut batch = WriteBatch::new();
            batch.put("a", "A").put("b", "B").delete("before");
            db.write_batch(batch)?;

            db.put(b"after".to_vec(), b"2".to_vec())?;
        }

        let db = DbStore::open(dir.path())?;
        assert_eq!(db.get(b"a")?, b"A".to_vec());
        assert_eq!(db.get(b"b")?, b"B".to_vec());
        assert_eq!(db.get(b"after")?, b"2".to_vec());
        assert!(matches!(db.get(b"before"), Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_crash_during_batch_leaves_no_trace() -> Result<()> {
        let dir = tempdir().unwrap();

        {
            let db = DbStore::open(dir.path())?;
            db.put(b"base".to_vec(), b"safe".to_vec())?;

            let mut batch = WriteBatch::new();
            batch
                .put("x", "1")
                .put("y", "2")
                .put("base", "overwritten")
                .delete("z");
            db.write_batch(batch)?;
        }

        let segment = active_segment(dir.path());
        let len = std::fs::metadata(&segment).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&segment)
            .unwrap()
            .set_len(len - 4)
            .unwrap();

        {
            let db = DbStore::open(dir.path())?;

            assert!(matches!(db.get(b"x"), Err(DbError::KeyNotFound)));
            assert!(matches!(db.get(b"y"), Err(DbError::KeyNotFound)));
            assert_eq!(db.get(b"base")?, b"safe".to_vec());

            db.put(b"post_crash".to_vec(), b"still here".to_vec())?;
        }

        let db = DbStore::open(dir.path())?;
        assert_eq!(db.get(b"post_crash")?, b"still here".to_vec());
        assert_eq!(db.get(b"base")?, b"safe".to_vec());
        assert!(matches!(db.get(b"x"), Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_concurrent_batches() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = Arc::new(DbStore::open(dir.path())?);

        let handles: Vec<_> = (0..4)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for n in 0..25 {
                        let mut batch = WriteBatch::new();
                        for k in 0..4 {
                            batch.put(format!("t{t}_n{n}_k{k}"), format!("{t}-{n}-{k}"));
                        }
                        db.write_batch(batch).unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let check = |db: &DbStore| {
            for t in 0..4 {
                for n in 0..25 {
                    for k in 0..4 {
                        let key = format!("t{t}_n{n}_k{k}");
                        assert_eq!(
                            db.get(key.as_bytes()).unwrap(),
                            format!("{t}-{n}-{k}").into_bytes()
                        );
                    }
                }
            }
        };
        check(&db);

        drop(db);
        check(&DbStore::open(dir.path())?);

        Ok(())
    }

    #[test]
    fn test_compact_keeps_batch_written_data() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        let mut batch = WriteBatch::new();
        batch.put("a", "1").put("b", "2").delete("a");
        db.write_batch(batch)?;

        db.compact()?;

        assert_eq!(db.get(b"b")?, b"2".to_vec());
        assert!(matches!(db.get(b"a"), Err(DbError::KeyNotFound)));
        Ok(())
    }
}
