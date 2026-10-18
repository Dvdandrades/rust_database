use std::path::Path;

use crate::error::{DbError, Result};
use crate::model::record::{Record, RecordType};
use crate::storage::index::KeyIndex;
use crate::storage::log::LogManager;

pub struct DbStore {
    log: LogManager,
    index: KeyIndex,
}

impl DbStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();

        std::fs::create_dir_all(path_ref)?;

        let log_path = path_ref.join("db.log");
        let log = LogManager::open(log_path)?;
        let index = KeyIndex::new();

        let records = log.read_all()?;
        for (pos, record) in records {
            let serialized = record.serialize()?;

            let size = (4 + serialized.len()) as u32;
            let rec_type = RecordType::try_from(record.recid)?;

            match rec_type {
                RecordType::Put => {
                    index.insert(record.key, pos, size);
                }
                RecordType::Delete => {
                    index.remove(&record.key);
                }
            }
        }

        Ok(Self { log, index })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let record = Record::new_put(key.clone(), value);
        let serialized = record.serialize()?;
        let size = (4 + serialized.len()) as u32;

        let pos = self.log.append(&record)?;
        self.index.insert(key, pos, size);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let entry = self.index.get(key).ok_or(DbError::KeyNotFound)?;

        let record = self.log.read_at(entry.offset)?;
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
        self.log.append(&record)?;
        self.index.remove(&key);

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
