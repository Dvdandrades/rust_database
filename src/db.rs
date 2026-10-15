use std::path::Path;

use crate::error::{DbError, Result};
use crate::index::KeyIndex;
use crate::log::LogManager;
use crate::record::{Record, RecordType};

pub struct DbStore {
    log: LogManager,
    index: KeyIndex,
}

impl DbStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let log_path = path.as_ref().join("db.log");
        let mut log = LogManager::open(log_path)?;
        let mut index = KeyIndex::new();

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

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let record = Record::new_put(key.clone(), value);
        let serialized = record.serialize()?;
        let size = (4 + serialized.len()) as u32;

        let pos = self.log.append(&record)?;

        self.index.insert(key, pos, size);

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        let entry = self.index.get(key).ok_or(DbError::KeyNotFound)?;

        let record = self.log.read_at(entry.offset)?;
        let rec_type = RecordType::try_from(record.recid)?;

        match rec_type {
            RecordType::Put => Ok(record.value),
            RecordType::Delete => Err(DbError::KeyNotFound),
        }
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
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
    use tempfile::tempdir;

    #[test]
    fn test_put_and_get() -> Result<()> {
        let dir = tempdir().unwrap();
        let mut db = DbStore::open(dir.path())?;

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
        let mut db = DbStore::open(dir.path())?;

        let result = db.get(b"missing_key");
        assert!(matches!(result, Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let dir = tempdir().unwrap();
        let mut db = DbStore::open(dir.path())?;

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
        let mut db = DbStore::open(dir.path())?;

        let result = db.delete(b"phantom_key".to_vec());
        assert!(matches!(result, Err(DbError::KeyNotFound)));

        Ok(())
    }

    #[test]
    fn test_persistence_and_recovery() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        {
            let mut db = DbStore::open(&path)?;
            db.put(b"user_1".to_vec(), b"David".to_vec())?;
            db.put(b"user_2".to_vec(), b"Ana".to_vec())?;
            db.delete(b"user_1".to_vec())?;
        }

        {
            let mut db = DbStore::open(&path)?;

            assert!(matches!(db.get(b"user_1"), Err(DbError::KeyNotFound)));

            let val = db.get(b"user_2")?;
            assert_eq!(val, b"Ana".to_vec());
        }

        Ok(())
    }
}
