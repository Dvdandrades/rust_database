//! The storage engine's public entry point.
//! 
//! This module contains [`DbStore`], the handle applicattions use to read and
//! write data, and [`Scan`], the iterator returned by prefix and range scans.
//! Everything else in the engine (the segmented log, the in-memory index, the
//! metrics counters) is an implementation detail reachable only through
//! `DbStore`.

use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::metrics::{Metrics, MetricsSnapshot};
use crate::error::{DbError, Result};
use crate::model::record::{BatchOp, Record, RecordType, WriteBatch};
use crate::storage::index::{IndexUpdate, KeyDirEntry, KeyIndex};
use crate::storage::log::LogManager;

/// An embedded, persistent, thread-sage key-value store.
/// 
/// `DbStore` is the main type of this crate. Keys and values are arbitrary byte
/// strings. Every write is appended to a segmented, checksummed log on disk, and
/// a sorted in-memory index maps each live key to the position of its latest
/// value (a design inspired by [Bitcask]). Reads therefore cost one index lookup
/// plus one positioned disk read.
/// 
/// [Bitcask]: https://riak.com/assets/bitcask-intro.pdf
/// 
/// # Quick start
/// 
/// ```
/// use rust_database::{DbError, DbStore};
/// # let dir = tempfile::tempdir()?;
/// 
/// let db = DbStore::open(dir.path())?;
/// 
/// db.put(b"greeting".to_vec(), b"hello".to_vec())?;
/// assert_eq!(db.get(b"greeting")?, b"hello".to_vec());
/// 
/// db.delete(b"greeting".to_vec())?;
/// assert!(matches!(db.get(b"greeting"), Err(DbError::KeyNotFound)));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// # Guarantees
/// 
/// * **Durability.** [`put`](Self::put), [`delete`](Self::delete) and
/// [`write_batch`](Self::write_batch) flush with `fsync` before returning, so
/// an `Ok` means the operation has been handed to stable storage. There is no
/// separate "flush" or "close" step: dropping the store loses nothing.
/// * **Crash safety.** Every record is framed with its length and a CRC-32. After
/// a crash, [`open`](Self::open) discards a torn write at the end of the newest
/// segment and keeps everything before it.
/// * **Atomic batches.** A [`WriteBatch`] is written as a single checksummed unit:
/// after a crash either all of its operations are visible or none are. While
/// the process is running, readers never observe half of a batch either.
/// * **Ordered keys.** Keys are ordered by their bytes (lexicographically), which
/// is what [`scan_prefix`](Self::scan_prefix) and
/// [`scan_range`](Self::scan_range) rely on.
/// 
/// # Costs and limits
/// 
/// * **All live keys are held in memory** (plus a small fixed-size entry per
/// key). Values stay on disk and are read on demand. Size your key space
/// accordingly.
/// * **Overwrites and deletes leave dead space.** Old versions stay in the log
/// until you call [`compact`](Self::compact); nothing compacts automatically.
/// * **Segments rotate at 256 MiB.** The directory holds one `NNNNNNNNNN.log`
/// file per segment.
/// * **One process per directory.** The directory is not protected by a lock
/// file. Opening the same directory twice (from two `DbStore's` or two
/// processes) at the same time is not supported and will corrupt the log.
/// 
/// # Concurrency
/// 
/// `DbStore` is `Send + Sync` and every method takes `&self`, so a single store
/// can be shared between threads, typically behind an [`Arc`][std::sync::Arc]:
/// 
/// ```
/// use std::{sync::Arc, thread};
/// use rust_database::DbStore;
/// # let dir = tempfile::tempdir()?;
/// 
/// let db = Arc::new(DbStore::open(dir.path())?);
/// 
/// let workers: Vec<_> = (0..4).map(|i| {
///     let db = Arc::clone(&db);
///     thread::spawn(move || {
///         db.put(format!("worker:{i}").into_bytes(), b"done".to_vec()).unwrap();
///     })
/// }).collect();
/// for worker in workers {
///     worker.join().unwrap();
/// }
/// 
/// assert_eq!(db.scan_prefix("worker:").count(), 4);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// Operations that touch the log file (`put`, `get`, `delete`, `write_batch`,
/// `compact` and the reads performed while iterating a [`Scan`]) are serialized
/// by a single internal mutex, so they run one at a time, reads included. This
/// keeps the design simple and consistent, but it means throughput does not grow
/// with the number of threads. [`keys_with_prefix`](Self::keys_with_prefix) and the
/// moment a scan is created only consult the in-memory index and do not wait for
/// that mutex.
pub struct DbStore {
    log: Mutex<LogManager>,
    index: KeyIndex,
    metrics: Metrics,
    generation: AtomicU64,
}

impl DbStore {
    /// Opens the store rooted at `path`, creating it if it does not exist.
    /// 
    /// `path` is a *directory*, not a file. It is created (together with any missing
    /// parent directories) if absent, and it holds the log segments
    /// (`0000000000.log`, `0000000001.log`, ...).
    /// 
    /// # Recovery
    /// 
    /// Opening replays every segment in order to rebuild the in-memory index, so
    /// start-up time grows with the total size of the log (compacting keeps it
    /// small). Within each segment, replay stops at the first frame that is
    /// truncated, fails its checksum or cannot be decoded, and everything after it
    /// in that segment is ignored. When the damage is in the newest segment (the
    /// usual result of a crash in the middel of a write) a fresh segment is started,
    /// so new writes never land after garbage. A batch that was only partially
    /// written is discarded as a whole.
    /// 
    /// The operation counters reported by [`metrics`](Self::metrics) start from zero
    /// on every open, while `live_keys` and `disk_size_bytes` reflect the recovered
    /// state.
    /// 
    /// # Errors
    /// 
    /// * [`DbError::Io`] or [`DbError::PermissionDenied`] if the directory cannot be
    ///   created or its files cannot be read or opened.
    /// * [`DbError::Corruption`] if the recovered log contains a record that cannot be
    ///   interpreted.
    /// 
    /// # Examples
    /// 
    /// Data survives closing and reopening the store:
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// 
    /// {
    ///     let db = DbStore::open(dir.path())?;
    ///     db.put(b"k".to_vec(), b"v".to_vec())?;
    /// } // dropped: nothing to flush, every write was already fsynced
    /// 
    /// let db = DbStore::open(dir.path())?;
    /// assert_eq!(db.get(b"k")?, b"v".to_vec());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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

        let metrics = Metrics::default();
        metrics.set_disk_size(log.total_size());

        Ok(Self {
            log: Mutex::new(log),
            index,
            metrics,
            generation: AtomicU64::new(0),
        })
    }

    /// Stores `value` under `key`, replacing any previous value.
    /// 
    /// The record is appended to the log and flushed with `fsync` before this method
    /// returns. The replaced value stays on disk as dead space until the next
    /// [`compact`](Self::compact). To write many entries at once, prefer
    /// [`write_batch`](Self::write_batch): it pays for a single `fsync` and is atomic.
    /// 
    /// Empty keys and empty values are allowed.
    /// 
    /// # Errors
    /// 
    /// * [`DbError::Io`] or [`DbError::PermissionDenied`] if the write or the `fsync`
    ///   fails. The partial write is rolled back on a best-effort basis and the index
    ///   is left untouched, so the previous value (if any) is still the current one.
    /// * [`DbError::Serialization`] if the record is too large to be framed (its
    ///   encoded size must fit in 32 bits).
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// 
    /// db.put(b"counter".to_vec(), b"1".to_vec())?;
    /// db.put(b"counter".to_vec(), b"2".to_vec())?; // overwrite
    /// 
    /// assert_eq!(db.get(b"counter")?, b"2".to_vec());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let record = Record::new_put(key.clone(), value);
        let serialized = record.serialize()?;
        let size = (8 + serialized.len()) as u32;

        let mut log = self.log.lock().unwrap();
        let (file_id, pos) = log.append(&record)?;
        self.index.insert(key, file_id, pos, size);
        self.metrics.set_disk_size(log.total_size());
        drop(log);
        self.metrics.record_put();
        Ok(())
    }

    /// Returns the current value stored under `key`.
    /// 
    /// Each call reads the record from disk and verifies its checksum; there is no
    /// value cache. The call counts towards the `reads`, `get_hits` and `get_misses`
    /// metrics.
    /// 
    /// # Errors
    /// 
    /// * [`DbError::KeyNotFound`] if the key does not exist (or was deleted). This is
    ///   an ordinary outcome rather than a failure, so match on it explicitly.
    /// * [`DbError::Io`] or [`DbError::UnexpectedEof`] if the record cannot be read.
    /// * [`DbError::Corruption`] if the record fails its checksum or cannot be
    ///   decoded.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::{DbError, DbStore};
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// db.put(b"user:42".to_vec(), b"ana".to_vec())?;
    /// 
    /// assert_eq!(db.get(b"user:42")?, b"ana".to_vec());
    /// 
    /// // A missing key is reported as `KeyNotFound`; other errors are real failures.
    /// match db.get(b"user:43") {
    ///     Ok(_) => unreachable!(),
    ///     Err(DbError::KeyNotFound) => println!("no such user"),
    ///     Err(other) => return Err(other.into()),
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.metrics.record_read();

        let result = {
            let log = self.log.lock().unwrap();
            match self.index.get(key) {
                Some(entry) => Self::value_at(&log, entry),
                None => Err(DbError::KeyNotFound),
            }
        };

        match &result {
            Ok(_) => self.metrics.record_get_hit(),
            Err(DbError::KeyNotFound) => self.metrics.record_get_miss(),
            Err(_) => {}
        }
        result
    }

    fn value_at(log: &LogManager, entry: KeyDirEntry) -> Result<Vec<u8>> {
        let record = log.read_at(entry.file_id, entry.offset)?;

        let rec_type = RecordType::try_from(record.recid)?;

        match rec_type {
            RecordType::Put => Ok(record.value),
            RecordType::Delete => Err(DbError::KeyNotFound),
            RecordType::BatchBegin => Err(DbError::Corruption(
                "Index entry points to a batch header".to_string(),
            )),
        }
    }

    fn read_scanned(&self, key: &[u8], entry: KeyDirEntry, generation: u64) -> Result<Vec<u8>> {
        let log = self.log.lock().unwrap();
        if self.generation.load(Ordering::Acquire) == generation {
            return Self::value_at(&log, entry);
        }
        match self.index.get(key) {
            Some(current) => Self::value_at(&log, current),
            None => Err(DbError::KeyNotFound),
        }
    }

    /// Iterates over every entry whose key starts with `prefix`, in ascending key
    /// order.
    /// 
    /// `prefix` can be anything that implements `AsRef<[u8]>`: a `&str`, a `String`,
    /// a byte-string literal such as `b"user:"`, a `&[u8]` or a `Vec<u8>`. An empty
    /// prefix matches every key.
    /// 
    /// The set of keys is captured when this method is called, and values are read
    /// lazily as the returned [`Scan`] is advanced. No lock is held between items, so
    /// a scan never blocks writers. See [`Scan`] for the precise snapshot semantics.
    /// Each call counts as one scan in the metrics.
    /// 
    /// Choose your key layout so related entries share a prefix (for instance
    /// `user:<id>` or `order:<customer>:<id>`); that is what makes this method cheap.
    /// Note that `"user"` also matches `"users"`, so include the separator in the
    /// prefix when you mean `"user:"`.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// db.put(b"user:2".to_vec(), b"david".to_vec())?;
    /// db.put(b"user:1".to_vec(), b"ana".to_vec())?;
    /// db.put(b"order:1".to_vec(), b"book".to_vec())?;
    /// 
    /// for entry in db.scan_prefix("user:") {
    ///     let (key, value) = entry?;
    ///     println!(
    ///         "{} = {}",
    ///         String::from_utf8_lossy(&key),
    ///         String::from_utf8_lossy(&value)
    ///     );
    /// }
    /// 
    /// // Collect just the keys; the first I/O error aborts the collection.
    /// let keys = db.scan_prefix("user:").map(|entry| entry.map(|(key, _value)| key))
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// assert_eq!(keys, [b"user:1".to_vec(), b"user:2".to_vec()]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> Scan<'_> {
        self.metrics.record_scan();
        let generation = self.generation.load(Ordering::Acquire);
        Scan::new(self, generation, self.index.scan_prefix(prefix.as_ref()))
    }

    /// Iterates over every entry whose key lies inside `range`, in ascending key
    /// order.
    /// 
    /// `range` can be any of the standard range types over byte-like keys
    /// (`"a".."m"`, `"a"..="m"`, `"m"..`, `.."m"`) or an explicit
    /// `(Bound, Bound)` pair, which is the way to express an exclusive start bound.
    /// Keys are compared bytewise. An inverted range (start after end) simply yields
    /// nothing; ir does not panic.
    /// 
    /// For a completely unbounded range (`..`) and for `(Bound, Bound)` pairs the
    /// compiler cannot infer the key type, so spell it out:
    /// `scan_range::<&str, _>(..)` (or use `scan_prefix("")` to scan everything).
    /// 
    /// Like [`scan_prefix`](Self::scan_prefix), the key set is captured when the
    /// method is called, values are loaded lazily and the call counst as one scan.
    /// See [`Scan`] for the snapshot semantics.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use std::ops::Bound;
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// for key in ["a", "b", "c", "d", "e"] {
    ///     db.put(key.as_bytes().to_vec(), key.to_uppercase().into_bytes())?;
    /// }
    /// 
    /// assert_eq!(db.scan_range("b".."d").count(), 2); // b, c
    /// assert_eq!(db.scan_range("b"..="d").count(), 3); // b, c, d
    /// assert_eq!(db.scan_range("d"..).count(), 2); // d, e
    /// assert_eq!(db.scan_range::<&str, _>(..).count(), 5); // everything
    /// assert_eq!(db.scan_range("z".."a").count(), 0); // inverted: empty 
    /// 
    /// // Exclusive start bound: everything after "b".
    /// let after_b = db.scan_range::<&str, _>((Bound::Excluded("b"), Bound::Unbounded));
    /// assert_eq!(after_b.count(), 3); // c, d, e
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn scan_range<K, R>(&self, range: R) -> Scan<'_>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        self.metrics.record_scan();
        let start = range.start_bound().map(|k| AsRef::<[u8]>::as_ref(k));
        let end = range.end_bound().map(|k| AsRef::<[u8]>::as_ref(k));
        let generation = self.generation.load(Ordering::Acquire);
        Scan::new(self, generation, self.index.range(start, end))
    }

    /// Returns the keys (without their values) that start with `prefix`, in ascending
    /// order.
    /// 
    /// This is the cheapest way to list or count entries: it only consults the
    /// in-memory index, performs no disk I/O, does not wait for the log mutex and is
    /// not counted as a scan in the metrics. The whole result is materialized in a
    /// `Vec`; use [`scan_prefix`](Self::scan_prefix) when you also need the values or
    /// want to process entries one at a time.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// db.put(b"tag:b".to_vec(), vec![])?;
    /// db.put(b"tag:a".to_vec(), vec![])?;
    /// db.put(b"other".to_vec(), vec![])?;
    /// 
    /// assert_eq!(db.keys_with_prefix("tag:"), vec![b"tag:a".to_vec(), b"tag:b".to_vec()]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn keys_with_prefix(&self, prefix: impl AsRef<[u8]>) -> Vec<Vec<u8>> {
        self.index
            .scan_prefix(prefix.as_ref())
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    /// Returns a point-in-time snapshot of the store's counters and gauges.
    /// 
    /// * **Counters** (`reads`, `puts`, `batches`, ...) count operations since this
    ///   `DbStore` was opened and restart from zero on every [`open`](Self::open).
    /// * **Gauges** (`live_keys`, `disk_size_bytes`) describe the current state and
    ///   are rebuilt from the log when the store is opened. `disk_size_bytes` is the
    ///   total size of all log segments, dead space included.
    /// 
    /// Taking a snapshot is cheap and never blocks. The fields are read one by one
    /// without a lock, so a snapshot taken while other threads are writing can be
    /// very slightly inconsistent between fields.
    /// 
    /// [`MetricsSnapshot::to_prometheus`] renders a snapshot in the Prometheus text
    /// exposition format.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// 
    /// db.put(b"a".to_vec(), b"1".to_vec())?;
    /// db.get(b"a")?;
    /// let _ = db.get(b"missing");
    /// 
    /// let m = db.metrics();
    /// assert_eq!((m.puts, m.reads), (1, 2));
    /// assert_eq!((m.get_hits, m.get_misses), (1, 1));
    /// assert_eq!(m.get_hit_ratio(), Some(0.5));
    /// assert_eq!(m.live_keys, 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot(self.index.len() as u64)
    }


    /// Removes a `key` from the store.
    /// 
    /// A tombstone record is appended to the log and flushed with `fsync` before this
    /// method returns, so the deletion is durable. The disk space of the removed
    /// value is only reclaimed by [`compact`](Self::compact).
    /// 
    /// # Errors
    /// 
    /// * [`DbError::KeyNotFound`] if the key does not exist. Nothing is written in
    ///   that case. If you want "delete if present" semantics, either ignore this
    ///   error or use a [`WriteBatch`], where deleting a missing key is not an error.
    /// * [`DbError::Io`] or [`DbError::PermissionDenied`] if the write or the `fsync`
    ///   fails; the key then remains in place.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::{DbError, DbStore};
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// db.put(b"session".to_vec(), b"abc".to_vec())?;
    /// 
    /// db.delete(b"session".to_vec())?;
    /// assert!(matches!(db.get(b"session"), Err(DbError::KeyNotFound)));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let mut log = self.log.lock().unwrap();
        if self.index.get(&key).is_none() {
            return Err(DbError::KeyNotFound);
        }

        let record = Record::new_delete(key.clone());
        log.append(&record)?;
        self.index.remove(&key);
        self.metrics.set_disk_size(log.total_size());
        drop(log);

        self.metrics.record_delete();
        Ok(())
    }

    /// Applies every operation in `batch` atomically.
    /// 
    /// The operations take effect in the order they were added to the batch, so a
    /// later operation on a key wins over an earlier one (`put` then `delete` leaves
    /// the key deleted; `delete` then `put` leaves it present).
    /// 
    /// * **Atomic on disk.** The whole batch is written as one checksummed unit. I
    ///   the process crashes mid-write, recovery discards the batch entirely.
    /// * **Atomic in memory.** The index is updated in a single step while the log
    ///   mutex is held, so concurrent `get`s, scans and
    ///   [`keys_with_prefix`](Self::keys_with_prefix) calls see either none or all of
    ///   the batch.
    /// * **Fast.** The batch costs one `fsync` regardless of its size, far fewer than
    ///   the same number of individual [`put`](Self::put) calls.
    /// 
    /// An empty batch is a successful no-op and is not counted in the metrics.
    /// Deleting a key that does not exist is *not* an error inside a batch. Batches
    /// count as one `batches` plus `n` `batch_ops` in the metrics (they do not
    /// increment `puts` or `deletes`).
    /// 
    /// # Errors
    /// 
    /// * [`DbError::Io`] or [`DbError::PermissionDenied`] if the write or the `fsync`
    ///   fails.
    /// * [`DbError::Serialization`] if a record is too large to be framed or the batch
    ///   holds more operations than fit in 32 bits.
    /// 
    /// On error nothing is applied to the index.
    /// 
    /// # Examples
    /// 
    /// Move a balance between two accounts so that no reader (and no crash) can ever
    /// see the money in both places or in neither:
    /// 
    /// ```
    /// use rust_database::{DbStore, WriteBatch};
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// db.put(b"account:david".to_vec(), b"100".to_vec())?;
    /// db.put(b"account:ana".to_vec(), b"50".to_vec())?;
    /// db.put(b"pending:transfer-7".to_vec(), b"david->ana:10".to_vec())?;
    /// 
    /// let mut batch = WriteBatch::new();
    /// batch.put("account:david", "90").put("account:ana", "60").delete("pending:transfer-7");
    /// db.write_batch(batch)?;
    /// 
    /// assert_eq!(db.get(b"account:david")?, b"90".to_vec());
    /// assert_eq!(db.get(b"account:ana")?, b"60".to_vec());
    /// assert!(db.keys_with_prefix("pending:").is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let ops_count = batch.len() as u64;

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
        self.metrics.set_disk_size(log.total_size());

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

        self.metrics.record_batch(ops_count);
        Ok(())
    }

    /// Reclaims the disk space used by overwritten and deleted entries.
    /// 
    /// Compaction copies every live record into fresh segments, switches the index
    /// over to the copies and then deletes the old segments. Nothing runs
    /// automatically, so call this yourself when
    /// [`metrics`](Self::metrics)`().disk_size_bytes` has grown well beyond the size
    /// of your live data, typically after many overwrites or deletes. It also bounds
    /// start-up time, since [`open`](Self::open) replays the whole log.
    /// 
    /// # Cost
    /// 
    /// Compaction holds the log mutex for its whole duration, so **every other
    /// operation that touches the log (including `get`) waits for it to finish**.
    /// The work is proportional to the amount of *live* data, not to the log size.
    /// Schedule it for quiet moments, or run it from a dedicated maintenance thread.
    /// 
    /// # Crash safety
    /// 
    /// The new segments are flushed (and the directory synced) before any old
    /// segment is removed. A crash in between leaves both generations on disk;
    /// recovery replays them in order and arrives at the same state, and the next
    /// call to `compact` removes the leftovers.
    /// 
    /// # Interaction with scans
    /// 
    /// A [`Scan`] created before a compaction keeps working, but its remaining
    /// items are then resolved against the current index instead of the snapshot;
    /// see [`Scan`] for the details.
    /// 
    /// # Errors
    /// 
    /// * [`DbError::Io`] or [`DbError::PermissionDenied`] if copying or syncing fails.
    ///   The store then keeps serving its previous state.
    /// * The same errors if deleting the old segments fails. In that case the
    ///   compaction itself has already taken effect and no data is at risk; calling
    ///   `compact` again removes the leftovers.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use rust_database::DbStore;
    /// # let dir = tempfile::tempdir()?;
    /// let db = DbStore::open(dir.path())?;
    /// 
    /// // Rewriting one key 50 times leaves 49 dead versions in the log.
    /// for i in 0..50 {
    ///     db.put(b"hot".to_vec(), format!("v{i}").into_bytes())?;
    /// }
    /// let before = db.metrics().disk_size_bytes;
    /// 
    /// db.compact()?;
    /// 
    /// let after = db.metrics();
    /// assert!(after.disk_size_bytes < before);
    /// assert_eq!(after.compactions, 1);
    /// assert_eq!(db.get(b"hot")?, b"v49".to_vec());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn compact(&self) -> Result<()> {
        let mut log = self.log.lock().unwrap();

        let snapshot = self.index.get_all();
        let live: Vec<(u32, u64)> = snapshot
            .iter()
            .map(|(_, entry)| (entry.file_id, entry.offset))
            .collect();

        let (locations, obsolete) = log.rewrite_live(&live)?;

        let updates = snapshot
            .into_iter()
            .zip(locations)
            .map(|((key, _), loc)| IndexUpdate::Put {
                key,
                entry: KeyDirEntry {
                    file_id: loc.file_id,
                    offset: loc.offset,
                    size: loc.size,
                },
            });
        self.index.apply_batch(updates);
        self.generation.fetch_add(1, Ordering::Release);

        let removed = log.remove_segments(&obsolete);
        self.metrics.set_disk_size(log.total_size());
        drop(log);

        self.metrics.record_compaction();
        removed
    }
}

/// A lazy iterator over the entries selected by
/// [`DbStore::scan_prefix`] or [`DbStore::scan_range`].
/// 
/// Each item is a `Result<(key, value)>` with both parts as owned `Vec<u8>`.
/// Entries come in ascending key order, and the iterator is also a
/// [`DoubleEndedIterator`] (use `.rev()` or `next_back()` to walk from the
/// largest key down) and an [`ExactSizeIterator`] (`len()` is the number of
/// entries not yet yielded).
/// 
/// # Snapshot semantics
/// 
/// * **The set of keys is fixed when the scan is created.** Keys inserted
///   afterwards never appear. The snapshot is consistent with respect to
///   [`write_batch`](DbStore::write_batch): a batch is either entirely inside it
///   or entirely outside.
/// * **Values are read lazily**, one per call to `next`, so scanning a huge range
///   and stopping early only pays for the entries you actually consume (for
///   example `scan.rev().take(10)` reads ten values).
/// * **Overwrites and deletes after creation are not seen.** An entry yields the
///   value it had when the scan was created, even if the key has since been
///   overwriten or deleted.
/// * **Compaction weakens that guarantee.** Compaction deletes the old log
///   segments the snapshot points into, so if [`compact`](DbStore::compact) runs
///   while a scan is still being consumed, the scan re-resolves each remaining
///   key against the current idex: it then yields the *current* value, and a key
///   that was deleted in the meantime yields `Err(`[`DbError::KeyNotFound`]`)`.
///   Items yielded before the compaction are unaffected.
/// * **Errors are per item.** An I/O error or a corrupted record is reported as
///   an `Err` for that entry only; the iterator is not fused and later entries can
///   still succeed. Use `collect::<Result<Vec_>, _>>()` to stop at the first
///   error.
/// 
/// A `Scan` borrows its [`DbStore`] and holds no lock between items, so it never
/// blocks writers (and you cannot drop the store while a scan is alive).
/// 
/// # Examples
/// 
/// ```
/// use rust_database::DbStore;
/// # let dir = tempfile::tempdir()?;
/// let db = DbStore::open(dir.path())?;
/// db.put(b"k1".to_vec(), b"old".to_vec())?;
/// db.put(b"k2".to_vec(), b"old".to_vec())?;
/// 
/// let scan = db.scan_prefix("k");
/// 
/// // None of these writes is visible to the scan created above.
/// db.put(b"k1".to_vec(), b"new".to_vec())?;
/// db.delete(b"k2".to_vec())?;
/// db.put(b"k3".to_vec(), b"late".to_vec())?;
/// 
/// let seen = scan.collect::<Result<Vec<_>, _>>()?;
/// assert_eq!(seen, [(b"k1".to_vec(), b"old".to_vec()), (b"k2".to_vec(), b"old".to_vec())]);
/// 
/// // A fresh scan sees the current state; `.rev()` walks from the largest key.
/// let newest_first = db.scan_prefix("k").rev().map(|entry| entry.map(|(key, _)| key)).collect::<Result<Vec<_>, _>>()?;
/// assert_eq!(newest_first, [b"k3".to_vec(), b"k1".to_vec()]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Scan<'a> {
    db: &'a DbStore,
    generation: u64,
    entries: std::vec::IntoIter<(Vec<u8>, KeyDirEntry)>,
}

impl<'a> Scan<'a> {
    fn new(db: &'a DbStore, generation: u64, entries: Vec<(Vec<u8>, KeyDirEntry)>) -> Self {
        Self {
            db,
            generation,
            entries: entries.into_iter(),
        }
    }

    fn load(&self, key: Vec<u8>, entry: KeyDirEntry) -> Result<(Vec<u8>, Vec<u8>)> {
        let value = self.db.read_scanned(&key, entry, self.generation)?;
        Ok((key, value))
    }
}

impl Iterator for Scan<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, entry) = self.entries.next()?;
        Some(self.load(key, entry))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl DoubleEndedIterator for Scan<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (key, entry) = self.entries.next_back()?;
        Some(self.load(key, entry))
    }
}

impl ExactSizeIterator for Scan<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;
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

    fn collect(scan: Scan<'_>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        scan.collect()
    }

    fn kv(k: &str, v: &str) -> (Vec<u8>, Vec<u8>) {
        (k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    #[test]
    fn test_scan_prefix_is_sorted_and_filtered() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        db.put(b"user:3".to_vec(), b"david".to_vec())?;
        db.put(b"order:1".to_vec(), b"x".to_vec())?;
        db.put(b"user:1".to_vec(), b"ana".to_vec())?;
        db.put(b"user:2".to_vec(), b"pablo".to_vec())?;
        db.put(b"users".to_vec(), b"not a match for 'user:'".to_vec())?;
        db.put(b"user:2".to_vec(), b"paco".to_vec())?;
        db.put(b"user:9".to_vec(), b"juan".to_vec())?;
        db.delete(b"user:9".to_vec())?;

        assert_eq!(
            collect(db.scan_prefix("user:"))?,
            vec![
                kv("user:1", "ana"),
                kv("user:2", "paco"),
                kv("user:3", "david")
            ]
        );
        assert_eq!(db.scan_prefix("nope").count(), 0);
        assert_eq!(db.scan_prefix("").count(), 5, "empty prefix = everything");
        assert_eq!(db.scan_prefix(b"user:").len(), 3, "ExactSizeIterator");

        Ok(())
    }

    #[test]
    fn test_scan_range_variants() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        for k in ["a", "b", "c", "d", "e"] {
            db.put(k.as_bytes().to_vec(), k.to_uppercase().into_bytes())?;
        }
        let keys = |scan: Scan<'_>| -> Vec<String> {
            scan.map(|r| String::from_utf8(r.unwrap().0).unwrap())
                .collect()
        };

        assert_eq!(keys(db.scan_range("b".."d")), ["b", "c"]);
        assert_eq!(keys(db.scan_range("b"..="d")), ["b", "c", "d"]);
        assert_eq!(keys(db.scan_range("d"..)), ["d", "e"]);
        assert_eq!(keys(db.scan_range(.."c")), ["a", "b"]);
        assert_eq!(
            keys(db.scan_range::<&str, _>(..)),
            ["a", "b", "c", "d", "e"]
        );
        assert_eq!(
            keys(db.scan_range::<&str, _>((Bound::Excluded("b"), Bound::Unbounded))),
            ["c", "d", "e"]
        );
        assert_eq!(
            keys(db.scan_range(b"b".to_vec()..b"d".to_vec())),
            ["b", "c"],
            "owned bytes keys work too"
        );
        assert_eq!(
            db.scan_range("z".."a").count(),
            0,
            "inverted: empty, no panic"
        );

        Ok(())
    }

    #[test]
    fn test_scan_can_run_in_reverse() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        db.put(b"k1".to_vec(), b"1".to_vec())?;
        db.put(b"k2".to_vec(), b"2".to_vec())?;
        db.put(b"k3".to_vec(), b"3".to_vec())?;

        let forward = collect(db.scan_prefix("k"))?;
        let mut backward = collect_rev(db.scan_prefix("k"))?;
        backward.reverse();
        assert_eq!(forward, backward);
        assert_eq!(
            db.scan_prefix("k").next_back().unwrap()?,
            kv("k3", "3"),
            "last key first when walking backwards"
        );

        Ok(())
    }

    fn collect_rev(scan: Scan<'_>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        scan.rev().collect()
    }

    #[test]
    fn test_scan_is_a_snapshot() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        db.put(b"k1".to_vec(), b"old1".to_vec())?;
        db.put(b"k2".to_vec(), b"old2".to_vec())?;

        let scan = db.scan_prefix("k");

        db.put(b"k1".to_vec(), b"new1".to_vec())?;
        db.delete(b"k2".to_vec())?;
        db.put(b"k3".to_vec(), b"late".to_vec())?;

        assert_eq!(collect(scan)?, vec![kv("k1", "old1"), kv("k2", "old2")]);
        assert_eq!(
            collect(db.scan_prefix("k"))?,
            vec![kv("k1", "new1"), kv("k3", "late")]
        );

        Ok(())
    }

    #[test]
    fn test_scan_after_reopen_and_with_batches() -> Result<()> {
        let dir = tempdir().unwrap();

        {
            let db = DbStore::open(dir.path())?;
            db.put(b"p:a".to_vec(), b"1".to_vec())?;
            let mut batch = WriteBatch::new();
            batch
                .put("p:b", "2")
                .put("p:c", "3")
                .put("q:x", "9")
                .delete("p:a");
            db.write_batch(batch)?;
        }

        let db = DbStore::open(dir.path())?;
        assert_eq!(
            collect(db.scan_prefix("p:"))?,
            vec![kv("p:b", "2"), kv("p:c", "3")]
        );
        assert_eq!(collect(db.scan_prefix("q:"))?, vec![kv("q:x", "9")]);

        Ok(())
    }

    #[test]
    fn test_scan_does_not_block_writers() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        for i in 0..50 {
            db.put(format!("k{i:02}").into_bytes(), vec![b'v'])?;
        }

        let mut scan = db.scan_prefix("k");
        scan.next().unwrap()?;
        db.put(b"k99".to_vec(), b"w".to_vec())?;
        assert_eq!(scan.count(), 49);

        Ok(())
    }

    fn dir_size(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().metadata().unwrap().len())
            .sum()
    }

    #[test]
    fn test_metrics_start_at_zero() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        assert_eq!(db.metrics(), MetricsSnapshot::default());
        Ok(())
    }

    #[test]
    fn test_metrics_count_operations() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        db.put(b"a".to_vec(), b"1".to_vec())?;
        db.put(b"b".to_vec(), b"2".to_vec())?;

        assert_eq!(db.get(b"a")?, b"1".to_vec());
        assert!(db.get(b"zzz").is_err());

        db.delete(b"a".to_vec())?;
        assert!(db.delete(b"nope".to_vec()).is_err());

        let mut batch = WriteBatch::new();
        batch.put("c", "3").put("d", "4").delete("b");
        db.write_batch(batch)?;
        db.write_batch(WriteBatch::new())?;

        assert_eq!(db.scan_prefix("c").count(), 1);

        let m = db.metrics();
        assert_eq!(m.reads, 2);
        assert_eq!(m.get_hits, 1);
        assert_eq!(m.get_misses, 1);
        assert_eq!(m.puts, 2);
        assert_eq!(m.deletes, 1, "deleting a missing key is not counted");
        assert_eq!(m.batches, 1, "empty batches are not counted");
        assert_eq!(m.batch_ops, 3);
        assert_eq!(m.scans, 1);
        assert_eq!(m.compactions, 0);
        assert_eq!(m.live_keys, 2);
        assert_eq!(m.total_writes(), 6);
        assert_eq!(m.get_hit_ratio(), Some(0.5));

        Ok(())
    }

    #[test]
    fn test_metrics_disk_size_tracks_the_log() -> Result<()> {
        let dir = tempdir().unwrap();

        {
            let db = DbStore::open(dir.path())?;
            assert_eq!(db.metrics().disk_size_bytes, 0);

            db.put(b"k1".to_vec(), vec![7; 100])?;
            let after_put = db.metrics().disk_size_bytes;
            assert!(after_put > 100);
            assert_eq!(after_put, dir_size(dir.path()));

            db.delete(b"k1".to_vec())?;
            let after_delete = db.metrics().disk_size_bytes;
            assert!(after_delete > after_put, "a tombstone is appended");

            let mut batch = WriteBatch::new();
            batch.put("k2", "v").put("k3", "v");
            db.write_batch(batch)?;
            assert!(db.metrics().disk_size_bytes > after_delete);
            assert_eq!(db.metrics().disk_size_bytes, dir_size(dir.path()));
        }

        let db = DbStore::open(dir.path())?;
        let m = db.metrics();
        assert_eq!(m.disk_size_bytes, dir_size(dir.path()));
        assert_eq!(m.live_keys, 2, "gauges survive a restart");
        assert_eq!(m.puts, 0, "counters restart from zero");

        Ok(())
    }

    fn segment_files(path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".log"))
            .collect();
        names.sort();
        names
    }

    #[test]
    fn test_compact_replaces_segments_in_place() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        for i in 0..20 {
            db.put(b"hot".to_vec(), format!("value_{i}").into_bytes())?;
        }
        assert_eq!(segment_files(dir.path()), ["0000000000.log"]);

        db.compact()?;

        assert_eq!(
            segment_files(dir.path()),
            ["0000000001.log"],
            "old segmente removed, compacted one lives next to where the old one was"
        );
        assert!(!dir.path().join("compacted_temp").exists());
        assert_eq!(db.metrics().disk_size_bytes, dir_size(dir.path()));
        Ok(())
    }

    #[test]
    fn test_metric_after_compaction() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;

        for i in 0..20 {
            db.put(b"hot".to_vec(), format!("value_{i}").into_bytes())?;
        }
        let before = db.metrics().disk_size_bytes;

        db.compact()?;

        let m = db.metrics();
        assert_eq!(m.compactions, 1);
        assert_eq!(m.live_keys, 1);
        assert!(m.disk_size_bytes < before, "stale versions are dropped");

        Ok(())
    }

    #[test]
    fn test_scan_created_before_compaction_still_reads() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = DbStore::open(dir.path())?;
        db.put(b"k1".to_vec(), b"a".to_vec())?;
        db.put(b"k1".to_vec(), b"b".to_vec())?;
        db.put(b"k2".to_vec(), b"c".to_vec())?;

        let scan = db.scan_prefix("k");
        db.compact()?;

        assert_eq!(collect(scan)?, vec![kv("k1", "b"), kv("k2", "c")]);
        Ok(())
    }

    #[test]
    fn test_crash_before_old_segments_are_removed_is_harmless() -> Result<()> {
        let dir = tempdir().unwrap();
        let backup = tempdir().unwrap();

        {
            let db = DbStore::open(dir.path())?;
            db.put(b"a".to_vec(), b"1".to_vec())?;
            db.put(b"a".to_vec(), b"2".to_vec())?;
            db.put(b"gone".to_vec(), b"x".to_vec())?;
            db.delete(b"gone".to_vec())?;
            for entry in std::fs::read_dir(dir.path()).unwrap() {
                let entry = entry.unwrap();
                std::fs::copy(entry.path(), backup.path().join(entry.file_name())).unwrap();
            }
            db.compact()?;
        }

        for entry in std::fs::read_dir(backup.path()).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), dir.path().join(entry.file_name())).unwrap();
        }
        assert_eq!(segment_files(dir.path()).len(), 2);

        let db = DbStore::open(dir.path())?;
        assert_eq!(db.get(b"a")?, b"2".to_vec());
        assert!(matches!(db.get(b"gone"), Err(DbError::KeyNotFound)));

        db.compact()?;
        assert_eq!(
            segment_files(dir.path()).len(),
            1,
            "leftovers are collected"
        );
        assert_eq!(db.get(b"a")?, b"2".to_vec());
        Ok(())
    }
}
