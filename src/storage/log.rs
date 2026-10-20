use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::model::record::Record;

pub struct LogManager {
    dir_path: PathBuf,
    max_file_size: u64,
    active_file_id: u32,
    active_file: File,
    active_file_size: u64,
    read_only_files: BTreeMap<u32, PathBuf>,
}

impl LogManager {
    pub fn open<P: AsRef<Path>>(path: P, max_file_size: u64) -> Result<Self> {
        let dir_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        let mut segments = BTreeMap::new();
        let mut highest_id = 0;

        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".log")
                && let Some(id_str) = name_str.strip_suffix(".log")
                && let Ok(id) = id_str.parse::<u32>()
            {
                segments.insert(id, entry.path());
                if id > highest_id {
                    highest_id = id;
                }
            }
        }

        let active_file_id = if segments.is_empty() { 0 } else { highest_id };
        let active_path = dir_path.join(format!("{:010}.log", active_file_id));

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&active_path)?;

        let active_file_size = file.seek(SeekFrom::End(0))?;

        segments.remove(&active_file_id);

        Ok(Self {
            dir_path,
            max_file_size,
            active_file_id,
            active_file: file,
            active_file_size,
            read_only_files: segments,
        })
    }

    pub fn append(&mut self, record: &Record) -> Result<(u32, u64)> {
        let bytes = record.serialize()?;
        let len = bytes.len() as u32;
        let total_len = 4 + bytes.len() as u64;

        if self.active_file_size + total_len > self.max_file_size {
            self.rotate()?;
        }

        let pos = self.active_file_size;
        self.active_file.write_all(&len.to_le_bytes())?;
        self.active_file.write_all(&bytes)?;

        self.active_file.sync_all()?;

        self.active_file_size += total_len;

        Ok((self.active_file_id, pos))
    }

    fn rotate(&mut self) -> Result<()> {
        self.active_file.sync_all()?;

        let old_id = self.active_file_id;
        let old_path = self.dir_path.join(format!("{:010}.log", old_id));
        self.read_only_files.insert(old_id, old_path);

        self.active_file_id += 1;
        let new_path = self
            .dir_path
            .join(format!("{:010}.log", self.active_file_id));

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&new_path)?;

        self.active_file = file;
        self.active_file_size = 0;

        Ok(())
    }

    pub fn read_at(&self, file_id: u32, offset: u64) -> Result<Record> {
        let mut file = if file_id == self.active_file_id {
            let path = self.dir_path.join(format!("{:010}.log", file_id));
            File::open(path)?
        } else {
            let path = self.read_only_files.get(&file_id).ok_or_else(|| {
                crate::error::DbError::Corruption(format!("Log segment not found: {}", file_id))
            })?;
            File::open(path)?
        };

        file.seek(SeekFrom::Start(offset))?;

        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buffer = vec![0u8; len];
        file.read_exact(&mut buffer)?;

        Record::deserialize(&buffer)
    }

    pub fn read_all_segments(&self) -> Result<Vec<(u32, u64, Record)>> {
        let mut all_records = Vec::new();

        let mut all_ids: Vec<u32> = self.read_only_files.keys().cloned().collect();
        all_ids.push(self.active_file_id);
        all_ids.sort();

        for file_id in all_ids {
            let path = if file_id == self.active_file_id {
                self.dir_path.join(format!("{:010}.log", file_id))
            } else {
                self.read_only_files.get(&file_id).unwrap().clone()
            };

            if !path.exists() {
                continue;
            }

            let mut file = File::open(&path)?;
            let mut pos = 0u64;

            loop {
                let mut len_bytes = [0u8; 4];
                match file.read_exact(&mut len_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }

                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut buffer = vec![0u8; len];
                file.read_exact(&mut buffer)?;

                let record = Record::deserialize(&buffer)?;
                all_records.push((file_id, pos, record));

                pos += 4 + len as u64;
            }
        }

        Ok(all_records)
    }

    pub fn active_file_id(&self) -> u32 {
        self.active_file_id
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
        path.push(format!("test_log_{}", uuid_or_random()));

        let _ = std::fs::remove_dir_all(&path);

        {
            let mut log_manager = LogManager::open(&path, 1024 * 1024).unwrap();

            let record1 = Record::new_put(b"key1".to_vec(), b"value1".to_vec());
            let record2 = Record::new_delete(b"key2".to_vec());

            let (file_id1, pos1) = log_manager.append(&record1).unwrap();
            let (file_id2, pos2) = log_manager.append(&record2).unwrap();

            let fetched1 = log_manager.read_at(file_id1, pos1).unwrap();
            assert_eq!(fetched1, record1);

            let fetched2 = log_manager.read_at(file_id2, pos2).unwrap();
            assert_eq!(fetched2, record2);

            let all_records = log_manager.read_all_segments().unwrap();
            assert_eq!(all_records.len(), 2);
            assert_eq!(all_records[0], (file_id1, pos1, record1));
            assert_eq!(all_records[1], (file_id2, pos2, record2));
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn test_empty_log_read_all() {
        let mut path = env::temp_dir();
        path.push("empty_test_log");
        let _ = std::fs::remove_dir_all(&path);

        {
            let log_manager = LogManager::open(&path, 1024 * 1024).unwrap();
            let all_records = log_manager.read_all_segments().unwrap();
            assert!(all_records.is_empty());
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    fn uuid_or_random() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
