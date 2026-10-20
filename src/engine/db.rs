use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{DbError, Result};
use crate::model::record::{Record, RecordType};
use crate::storage::index::KeyIndex;
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
        let log = LogManager::open(&path_ref, max_file_size)?;
        let index = KeyIndex::new();

        let records = log.read_all_segments()?;
        for (file_id, pos, record) in records {
            let serialized = record.serialize()?;
            let size = (4 + serialized.len()) as u32;
            let rec_type = RecordType::try_from(record.recid)?;

            match rec_type {
                RecordType::Put => {
                    index.insert(record.key, file_id, pos, size);
                }
                RecordType::Delete => {
                    index.remove(&record.key);
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
        let size = (4 + serialized.len()) as u32;

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
            let size = (4 + serialized.len()) as u32;

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
}
