use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::Result;
use crate::model::record::Record;

pub struct LogManager {
    file: File,
}

impl LogManager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self { file })
    }

    pub fn append(&mut self, record: &Record) -> Result<u64> {
        let bytes = record.serialize()?;

        let pos = self.file.seek(SeekFrom::End(0))?;

        let len = bytes.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&bytes)?;
        self.file.flush()?;

        Ok(pos)
    }

    pub fn read_at(&mut self, offset: u64) -> Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut len_bytes = [0u8; 4];
        self.file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buffer = vec![0u8; len];
        self.file.read_exact(&mut buffer)?;

        Record::deserialize(&buffer)
    }

    pub fn read_all(&mut self) -> Result<Vec<(u64, Record)>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        let mut pos = 0u64;

        loop {
            let mut len_bytes = [0u8; 4];
            match self.file.read_exact(&mut len_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            self.file.read_exact(&mut buffer)?;

            let record = Record::deserialize(&buffer)?;
            records.push((pos, record));

            pos += 4 + len as u64;
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::record::Record;
    use std::env;

    #[test]
    fn test_log_manager_append_and_read() {
        let mut path = env::temp_dir();
        path.push(format!("test_log_{}.wal", uuid_or_random()));

        let _ = std::fs::remove_file(&path);

        {
            let mut log_manager = LogManager::open(&path).unwrap();

            let record1 = Record::new_put(b"key1".to_vec(), b"value1".to_vec());
            let record2 = Record::new_delete(b"key2".to_vec());

            let pos1 = log_manager.append(&record1).unwrap();
            let pos2 = log_manager.append(&record2).unwrap();

            let fetched1 = log_manager.read_at(pos1).unwrap();
            assert_eq!(fetched1, record1);

            let fetched2 = log_manager.read_at(pos2).unwrap();
            assert_eq!(fetched2, record2);

            let all_records = log_manager.read_all().unwrap();
            assert_eq!(all_records.len(), 2);
            assert_eq!(all_records[0], (pos1, record1));
            assert_eq!(all_records[1], (pos2, record2));
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_empty_log_read_all() {
        let mut path = env::temp_dir();
        path.push("empty_test_log.wal");
        let _ = std::fs::remove_file(&path);

        {
            let mut log_manager = LogManager::open(&path).unwrap();
            let all_records = log_manager.read_all().unwrap();
            assert!(all_records.is_empty());
        }

        let _ = std::fs::remove_file(&path);
    }

    fn uuid_or_random() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
