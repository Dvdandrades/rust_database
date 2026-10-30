use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{DbError, Result};
use crate::model::record::{Record, RecordType};

const FRAME_HEADER_LEN: u64 = 8;
const BATCH_META_LEN: usize = 16;

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

fn enconde_frame(payload: &[u8], out: &mut Vec<u8>) -> Result<usize> {
    let len = u32::try_from(payload.len())
        .map_err(|_| DbError::Serialization("record is too large for a log frame".to_string()))?;
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(FRAME_HEADER_LEN as usize + payload.len())
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<bool> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn read_frame<R: Read>(reader: &mut R, remaining: u64) -> Result<Option<(Record, u64)>> {
    if remaining < FRAME_HEADER_LEN {
        return Ok(None);
    }

    let mut header = [0u8; FRAME_HEADER_LEN as usize];
    if !read_exact_or_eof(reader, &mut header)? {
        return Ok(None);
    }
    let expected_checksum = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let len = u64::from(u32::from_le_bytes([
        header[4], header[5], header[6], header[7],
    ]));

    if len > remaining - FRAME_HEADER_LEN {
        return Ok(None);
    }

    let mut payload = vec![0u8; len as usize];
    if !read_exact_or_eof(reader, &mut payload)? {
        return Ok(None);
    }
    if crc32(&payload) != expected_checksum {
        return Ok(None);
    }

    match Record::deserialize(&payload) {
        Ok(record) => Ok(Some((record, FRAME_HEADER_LEN + len))),
        Err(_) => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchMeta {
    count: u32,
    body_len: u64,
    body_crc: u32,
}

impl BatchMeta {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(BATCH_META_LEN);
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&self.body_len.to_le_bytes());
        bytes.extend_from_slice(&self.body_crc.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != BATCH_META_LEN {
            return None;
        }
        Some(Self {
            count: u32::from_le_bytes(bytes[0..4].try_into().ok()?),
            body_len: u64::from_le_bytes(bytes[4..12].try_into().ok()?),
            body_crc: u32::from_le_bytes(bytes[12..16].try_into().ok()?),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLocation {
    pub file_id: u32,
    pub offset: u64,
    pub size: u32,
}

type ScannedRecord = (u32, u64, Record);

fn read_batch_body<R: Read>(
    reader: &mut R,
    header: &Record,
    file_id: u32,
    body_start: u64,
    remaining: u64,
) -> Result<Option<(Vec<ScannedRecord>, u64)>> {
    let Some(meta) = BatchMeta::decode(&header.value) else {
        return Ok(None);
    };
    if meta.body_len > remaining {
        return Ok(None);
    }

    let mut body = vec![0u8; meta.body_len as usize];
    if !read_exact_or_eof(reader, &mut body)? {
        return Ok(None);
    }
    if crc32(&body) != meta.body_crc {
        return Ok(None);
    }

    let capacity = (meta.count as u64).min(meta.body_len / FRAME_HEADER_LEN) as usize;
    let mut records = Vec::with_capacity(capacity);
    let mut cursor = Cursor::new(&body[..]);
    let mut offset = 0u64;

    for _ in 0..meta.count {
        let Some((record, frame_len)) = read_frame(&mut cursor, meta.body_len - offset)? else {
            return Ok(None);
        };
        if !matches!(
            RecordType::try_from(record.recid),
            Ok(RecordType::Put | RecordType::Delete)
        ) {
            return Ok(None);
        }
        records.push((file_id, body_start + offset, record));
        offset += frame_len;
    }

    if offset != meta.body_len {
        return Ok(None);
    }
    Ok(Some((records, meta.body_len)))
}

fn scan_segment(path: &Path, file_id: u32) -> Result<(Vec<ScannedRecord>, u64, u64)> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mut records = Vec::new();
    let mut pos = 0u64;

    while pos < file_len {
        let Some((record, frame_len)) = read_frame(&mut reader, file_len - pos)? else {
            break;
        };

        match RecordType::try_from(record.recid) {
            Ok(RecordType::Put | RecordType::Delete) => {
                records.push((file_id, pos, record));
                pos += frame_len;
            }
            Ok(RecordType::BatchBegin) => {
                let body_start = pos + frame_len;
                let Some((batch_records, body_len)) = read_batch_body(
                    &mut reader,
                    &record,
                    file_id,
                    body_start,
                    file_len - body_start,
                )?
                else {
                    break;
                };
                records.extend(batch_records);
                pos = body_start + body_len;
            }
            Err(_) => break,
        }
    }
    Ok((records, pos, file_len))
}

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

    fn write_and_sync(&mut self, buf: &[u8]) -> Result<()> {
        let result = self
            .active_file
            .write_all(buf)
            .and_then(|_| self.active_file.sync_all());

        if let Err(e) = result {
            let _ = self.active_file.set_len(self.active_file_size);
            return Err(e.into());
        }

        self.active_file_size += buf.len() as u64;
        Ok(())
    }

    pub fn append(&mut self, record: &Record) -> Result<(u32, u64)> {
        let mut frame = Vec::new();
        enconde_frame(&record.serialize()?, &mut frame)?;

        if self.active_file_size + frame.len() as u64 > self.max_file_size {
            self.rotate()?;
        }

        let pos = self.active_file_size;
        self.write_and_sync(&frame)?;

        Ok((self.active_file_id, pos))
    }

    pub fn append_batch(&mut self, records: &[Record]) -> Result<Vec<RecordLocation>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let count = u32::try_from(records.len())
            .map_err(|_| DbError::Serialization("batch has too many operations".to_string()))?;

        let mut body = Vec::new();
        let mut relative = Vec::with_capacity(records.len());
        for record in records {
            let start = body.len() as u64;
            let frame_len = enconde_frame(&record.serialize()?, &mut body)?;
            let size = u32::try_from(frame_len).map_err(|_| {
                DbError::Serialization("record is too large for a log frame".to_string())
            })?;
            relative.push((start, size));
        }

        let meta = BatchMeta {
            count,
            body_len: body.len() as u64,
            body_crc: crc32(&body),
        };
        let header = Record {
            recid: RecordType::BatchBegin as u8,
            key: Vec::new(),
            value: meta.encode(),
        };

        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN as usize + BATCH_META_LEN + body.len());
        let header_len = enconde_frame(&header.serialize()?, &mut buf)? as u64;
        buf.extend_from_slice(&body);
        drop(body);

        if self.active_file_size > 0
            && self.active_file_size + buf.len() as u64 > self.max_file_size
        {
            self.rotate()?;
        }

        let base = self.active_file_size;
        self.write_and_sync(&buf)?;

        let file_id = self.active_file_id;
        Ok(relative
            .into_iter()
            .map(|(start, size)| RecordLocation {
                file_id,
                offset: base + header_len + start,
                size,
            })
            .collect())
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

        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buffer = vec![0u8; len];
        file.read_exact(&mut buffer)?;

        let actual_checksum = crc32(&buffer);
        if actual_checksum != expected_checksum {
            return Err(DbError::Corruption(format!(
                "CRC32 checksum mismatch: expected {:x}, got {:x}",
                expected_checksum, actual_checksum
            )));
        }

        Record::deserialize(&buffer)
    }

    fn scan_all(&self) -> Result<(Vec<ScannedRecord>, bool)> {
        let mut all_records = Vec::new();
        let mut active_damaged = false;

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

            let (records, valid_len, file_len) = scan_segment(&path, file_id)?;
            all_records.extend(records);

            if file_id == self.active_file_id && valid_len < file_len {
                active_damaged = true;
            }
        }

        Ok((all_records, active_damaged))
    }

    pub fn read_all_segments(&self) -> Result<Vec<(u32, u64, Record)>> {
        Ok(self.scan_all()?.0)
    }

    pub fn recover(&mut self) -> Result<Vec<(u32, u64, Record)>> {
        let (records, active_damaged) = self.scan_all()?;
        if active_damaged {
            self.rotate()?;
        }
        Ok(records)
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
    use tempfile::tempdir;

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

    fn segment_path(dir: &Path, id: u32) -> PathBuf {
        dir.join(format!("{:010}.log", id))
    }

    fn sample_batch() -> Vec<Record> {
        vec![
            Record::new_put(b"a".to_vec(), b"1".to_vec()),
            Record::new_put(b"b".to_vec(), b"22".to_vec()),
            Record::new_delete(b"a".to_vec()),
        ]
    }

    #[test]
    fn test_batch_meta_roundtrip() {
        let meta = BatchMeta {
            count: 7,
            body_len: 123_456_789_012,
            body_crc: 0xDEADBEEF,
        };
        assert_eq!(BatchMeta::decode(&meta.encode()), Some(meta));
        assert_eq!(BatchMeta::decode(&[0u8; 3]), None);
    }

    #[test]
    fn test_append_batch_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = LogManager::open(dir.path(), 1024 * 1024).unwrap();

        let before = Record::new_put(b"before".to_vec(), b"x".to_vec());
        let after = Record::new_put(b"after".to_vec(), b"y".to_vec());
        let batch = sample_batch();

        let (before_id, before_pos) = log.append(&before).unwrap();
        let locations = log.append_batch(&batch).unwrap();
        let (after_id, after_pos) = log.append(&after).unwrap();

        assert_eq!(locations.len(), batch.len());
        for (record, loc) in batch.iter().zip(&locations) {
            assert_eq!(&log.read_at(loc.file_id, loc.offset).unwrap(), record);
            assert_eq!(
                loc.size as usize,
                8 + record.serialize().unwrap().len(),
                "size must be the full frame size, like for single appends"
            );
        }

        let all = log.read_all_segments().unwrap();
        let mut expected = vec![(before_id, before_pos, before)];
        expected.extend(
            batch
                .into_iter()
                .zip(&locations)
                .map(|(r, loc)| (loc.file_id, loc.offset, r)),
        );
        expected.push((after_id, after_pos, after));
        assert_eq!(all, expected);
    }

    #[test]
    fn test_append_empty_batch_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = LogManager::open(dir.path(), 1024).unwrap();

        assert!(log.append_batch(&[]).unwrap().is_empty());
        assert_eq!(fs::metadata(segment_path(dir.path(), 0)).unwrap().len(), 0);
    }

    #[test]
    fn test_torn_batch_is_discarded_at_every_cut_point() {
        let source = tempdir().unwrap();
        let first = Record::new_put(b"first".to_vec(), b"ok".to_vec());

        let (first_end, full) = {
            let mut log = LogManager::open(source.path(), 1024 * 1024).unwrap();
            log.append(&first).unwrap();
            let first_end = fs::metadata(segment_path(source.path(), 0)).unwrap().len();
            log.append_batch(&sample_batch()).unwrap();
            let full = fs::read(segment_path(source.path(), 0)).unwrap();
            (first_end as usize, full)
        };

        for cut in first_end..full.len() {
            let dir = tempdir().unwrap();
            fs::write(segment_path(dir.path(), 0), &full[..cut]).unwrap();

            let log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
            let records: Vec<Record> = log
                .read_all_segments()
                .unwrap()
                .into_iter()
                .map(|(_, _, r)| r)
                .collect();
            assert_eq!(records, vec![first.clone()], "cut at byte {cut}");
        }

        let dir = tempdir().unwrap();
        fs::write(segment_path(dir.path(), 0), &full).unwrap();
        let log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
        assert_eq!(log.read_all_segments().unwrap().len(), 1 + 3);
    }

    #[test]
    fn test_corrupted_batch_is_discarded_entirely() {
        let source = tempdir().unwrap();
        let first = Record::new_put(b"first".to_vec(), b"ok".to_vec());

        let (first_end, full) = {
            let mut log = LogManager::open(source.path(), 1024 * 1024).unwrap();
            log.append(&first).unwrap();
            let first_end = fs::metadata(segment_path(source.path(), 0)).unwrap().len();
            log.append_batch(&sample_batch()).unwrap();
            (
                first_end as usize,
                fs::read(segment_path(source.path(), 0)).unwrap(),
            )
        };

        for index in first_end..full.len() {
            let mut damaged = full.clone();
            damaged[index] ^= 0xFF;

            let dir = tempdir().unwrap();
            fs::write(segment_path(dir.path(), 0), &damaged).unwrap();

            let log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
            let records = log.read_all_segments().unwrap();
            assert_eq!(records.len(), 1, "flipped byte {index}");
            assert_eq!(records[0].2, first);
        }
    }

    #[test]
    fn test_batch_never_spans_segments() {
        let dir = tempdir().unwrap();
        let mut log = LogManager::open(dir.path(), 300).unwrap();

        let filler = Record::new_put(b"filler".to_vec(), vec![7u8; 100]);
        let (filler_id, _) = log.append(&filler).unwrap();

        let batch: Vec<Record> = (0..4)
            .map(|i| Record::new_put(format!("k{i}").into_bytes(), vec![i as u8; 60]))
            .collect();
        let locations = log.append_batch(&batch).unwrap();

        let batch_segment = locations[0].file_id;
        assert_ne!(batch_segment, filler_id);
        assert!(locations.iter().all(|l| l.file_id == batch_segment));
        for (record, loc) in batch.iter().zip(&locations) {
            assert_eq!(&log.read_at(loc.file_id, loc.offset).unwrap(), record);
        }
        assert_eq!(log.read_all_segments().unwrap().len(), 1 + batch.len());
    }

    #[test]
    fn tests_oversized_batch_in_empty_segment_does_not_rotate() {
        let dir = tempdir().unwrap();
        let mut log = LogManager::open(dir.path(), 64).unwrap();

        let batch: Vec<Record> = (0..5)
            .map(|i| Record::new_put(format!("k{i}").into_bytes(), vec![0u8; 50]))
            .collect();
        let locations = log.append_batch(&batch).unwrap();

        assert!(locations.iter().all(|l| l.file_id == 0));
        assert!(!segment_path(dir.path(), 1).exists());
    }

    #[test]
    fn test_recover_seals_damaged_active_segment() {
        let dir = tempdir().unwrap();
        let first = Record::new_put(b"first".to_vec(), b"ok".to_vec());
        let later = Record::new_put(b"later".to_vec(), b"after crash".to_vec());

        {
            let mut log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
            log.append(&first).unwrap();
            log.append_batch(&sample_batch()).unwrap();
        }

        let path = segment_path(dir.path(), 0);
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 3)
            .unwrap();

        {
            let mut log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
            let records = log.recover().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].2, first);
            assert_eq!(log.active_file_id(), 1, "damaged segment must be sealed");

            log.append(&later).unwrap();
        }

        let mut log = LogManager::open(dir.path(), 1024 * 1024).unwrap();
        let records: Vec<Record> = log
            .recover()
            .unwrap()
            .into_iter()
            .map(|(_, _, r)| r)
            .collect();
        assert_eq!(records, vec![first, later]);
        assert_eq!(log.active_file_id(), 1, "healthy log must not rotate again");
    }
}
