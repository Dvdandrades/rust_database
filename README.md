# Rust Database

`rust_database` is a small embedded, persistent key/value store written in Rust. It stores arbitrary byte strings, appends writes to a checksummed log, and keeps an ordered in-memory index from keys to their latest log records. The design is inspired by Bitcask.

This project is useful for learning about log-structured storage and for small embedded workloads. It is not a network database or a production database server.

## Features

- Durable `put`, `delete`, and batch writes: each successful write is synced before the call returns.
- Recovery by replaying the log when the store opens; checksummed records allow recovery to stop at a damaged or incomplete frame.
- Atomic batches, including after a crash.
- Ordered prefix and range scans, including reverse iteration.
- Manual compaction to reclaim space from overwrites and deletes.
- Metrics snapshots and Prometheus text output.
- A small command-line frontend and runnable examples.
- Thread-safe sharing of one open `DbStore` between threads.

## Requirements

- Rust and Cargo with support for the Rust 2024 edition.
- A filesystem that supports file synchronization (`fsync`/`sync_all`).

## Build and run

From the repository root:

```sh
cargo build
```

Run the CLI (it creates or opens `data.db` in the current working directory):

```sh
cargo run --bin cli -- put greeting hello
cargo run --bin cli -- get greeting
cargo run --bin cli -- scan ""
```

The examples use temporary directories and clean them up when they exit:

```sh
cargo run --example basic
cargo run --example batch_and_scan
cargo run --example metrics_and_compactions
cargo run --example embedded_app
```

To run the Criterion benchmark:

```sh
cargo bench --bench operations
```

The benchmark measures individual puts and gets; puts include a durable sync, so results depend strongly on the storage device and filesystem.

## Library quick start

Add the crate as a dependency (for a local checkout):

```toml
[dependencies]
rust_database = { path = "." }
```

Open a directory and use byte-vector keys and values:

```rust
use rust_database::{DbError, DbStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbStore::open("my-data")?;

    db.put(b"greeting".to_vec(), b"hello".to_vec())?;
    assert_eq!(db.get(b"greeting")?, b"hello".to_vec());

    db.delete(b"greeting".to_vec())?;
    assert!(matches!(db.get(b"greeting"), Err(DbError::KeyNotFound)));
    Ok(())
}
```

`DbStore::open` accepts a directory path, creates it and its missing parents, and replays existing segment files. Dropping the handle needs no explicit flush or close call: successful writes are already synced. Reopening the same directory after dropping the previous handle rebuilds its in-memory index from disk.

## API guide

### Keys and values

The library accepts arbitrary bytes, including empty keys and values. The CLI accepts text arguments and displays values as text (or identifies non-UTF-8 values); use the Rust API for binary data. Keys are ordered lexicographically by bytes.

### Put and get

`put(key: Vec<u8>, value: Vec<u8>)` inserts or replaces a value. Each call appends a record and syncs it, so frequent individual puts can be expensive. Replaced records remain in the log until compaction.

`get(key: &[u8])` reads the latest value from disk and checks the record checksum. Values are not cached in memory. A missing key returns `DbError::KeyNotFound`.

### Delete

`delete(key: Vec<u8>)` appends and syncs a tombstone. Deleting a missing key returns `DbError::KeyNotFound` and does not change the store. Deleted values occupy log space until compaction.

### Atomic batches

Use `WriteBatch` to group operations into one durable, atomic write:

```rust
use rust_database::{DbStore, WriteBatch};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = tempfile::tempdir()?;
let db = DbStore::open(dir.path())?;
let mut batch = WriteBatch::new();
batch.put("account:alice", "90")
    .put("account:bob", "60")
    .delete("pending:transfer-7");
db.write_batch(batch)?;
# Ok(())
# }
```

`WriteBatch::with_capacity(n)` can reserve space up front. `put` and `delete` are chainable; `len`, `is_empty`, and `ops` inspect a batch before it is consumed by `write_batch`. Operations apply in insertion order, so the last operation for a key determines its final state. Deleting a missing key within a batch is allowed. An empty batch succeeds as a no-op.

A non-empty batch is written and synced as one checksummed unit, then applied to the index together. Recovery discards an incomplete batch in its entirety. Batches increment `batches` and `batch_ops` metrics; their operations do not increment the individual `puts` or `deletes` counters.

### Prefix and range scans

`scan_prefix(prefix)` iterates matching `(key, value)` pairs in ascending key order. An empty prefix selects every key. The key set is selected when the scan is created and values are loaded lazily as you iterate. A scan is a double-ended, exact-size iterator, so methods such as `.rev()`, `.next_back()`, `.take(n)`, and `.len()` are available. Each item is a `Result<(Vec<u8>, Vec<u8>)>`; collect with `collect::<Result<Vec<_>, _>>()?` to stop at the first error.

`scan_range(range)` uses bytewise ordering and standard Rust range bounds. For example, `"a".."m"` includes `a` and excludes `m`; `"a"..="m"` includes both. Use `std::ops::Bound` for explicit inclusive or exclusive bounds. An inverted range is empty. For an unbounded range, specify the key type, for example `scan_range::<&str, _>(..)`. An empty-prefix scan is a simpler way to select all entries.

`keys_with_prefix(prefix)` returns the matching keys only, in order, materialized in a `Vec`. It reads just the in-memory index, does no value I/O, and does not increment the scan metric.

Scans hold no lock between yielded items, so writers can proceed. Inserts after scan creation are not included, and overwrites or deletes normally do not change values already selected by that scan. Compaction is the exception: since it removes old segments, a scan still being consumed after compaction resolves its remaining keys against the current index. Those items can therefore reflect newer values or return `KeyNotFound` if deleted.

### Compaction

Compaction is manual; the store never runs it automatically. `compact()` copies live records into fresh segments and removes obsolete segments, reclaiming space left by overwrites and deletes. Call it when `metrics().disk_size_bytes` is much larger than the live data or to reduce the amount of log replay needed at startup.

Compaction holds the log mutex for its full duration. Calls that read or write log records, including `get`, wait until it finishes. It is crash-safe: new segments are synced before old segments are removed. If removal fails after the new data is installed, the operation returns an error but the current data remains safe; calling compaction again can remove leftovers.

### Metrics

`metrics()` returns a `MetricsSnapshot`:

| Field | Meaning |
| --- | --- |
| `reads` | Calls to `get` |
| `get_hits` / `get_misses` | Successful and missing-key gets |
| `scans` | Calls to `scan_prefix` or `scan_range` |
| `puts`, `deletes` | Successful individual operations |
| `batches`, `batch_ops` | Successful non-empty batches and their operation count |
| `compactions` | Completed compactions |
| `live_keys` | Current number of indexed keys |
| `disk_size_bytes` | Total bytes in log segments, including obsolete records |

`total_writes()` sums the write counters. `get_hit_ratio()` returns `None` before any gets, otherwise the hit fraction. `to_prometheus(prefix)` renders Prometheus exposition text with the supplied metric prefix. Counters restart at zero each time the database is opened; `live_keys` and `disk_size_bytes` are reconstructed from disk. A snapshot does not lock the database, so fields may differ slightly in time when writes are concurrent.

### Errors

Fallible operations return the crate alias `rust_database::Result<T>`, equivalent to `std::result::Result<T, DbError>`. `DbError` variants are:

- `KeyNotFound`: a get or individual delete references no live key.
- `Io` and `PermissionDenied`: filesystem operations failed.
- `UnexpectedEof`: a record could not be read completely.
- `Corruption`: a record or recovered log cannot be interpreted safely.
- `Serialization`: data cannot be encoded in the on-disk frame limits.
- `UnsupportedVersion`: a format version is not supported.

Match `KeyNotFound` separately from other errors when it is an expected condition.

## Storage layout and recovery

The database path is a directory containing zero-padded segment files such as `0000000000.log` and `0000000001.log`. Segments rotate at 256 MiB. Each record has framing and a CRC-32 checksum. Startup scans segments in order and rebuilds the live-key index; consequently, opening time grows with the log size. Compaction can reduce that replay work.

If recovery finds a truncated, invalid-checksum, or undecodable frame, it ignores the remainder of that segment. Damage at the newest segment's tail is treated as a torn write and a fresh segment is started before further appends. An incomplete batch is discarded whole. Keep regular backups if the data matters: checksums detect damaged records but cannot restore them.

**Only one process or open store may use a database directory at a time.** The directory has no inter-process lock. Concurrently opening it from multiple `DbStore` handles or processes is unsupported and can corrupt data.

## Concurrency

One `DbStore` is `Send + Sync` and may be shared across threads, commonly with `Arc<DbStore>`. Methods take `&self`. Operations that access log records are serialized by a single internal mutex, including reads, writes, batches, compaction, and scan item reads. Thus the handle is safe to share, but operations do not gain parallel log throughput as threads are added. The in-memory key listing operations do not need the log mutex.

The store does not provide a read-modify-write transaction. If multiple threads update a key based on its current value, coordinate that sequence in the application. Batches make a known group of writes atomic, but do not calculate values from a locked read.

## CLI

The CLI stores data in `data.db` relative to the current working directory. Run `cargo run --bin cli --` without a command to print usage. Commands:

```text
cargo run --bin cli -- put <key> <value>
cargo run --bin cli -- get <key>
cargo run --bin cli -- delete <key>
cargo run --bin cli -- batch (put <key> <value> | delete <key>)...
cargo run --bin cli -- scan <prefix>       # use "" to scan all keys
cargo run --bin cli -- range <start> <end> # start inclusive, end exclusive
```

For example:

```sh
cargo run --bin cli -- batch put color blue put language Rust delete obsolete
cargo run --bin cli -- scan ""
cargo run --bin cli -- range a z
```

CLI arguments are UTF-8 command-line text and are converted to bytes. The range command is half-open. Since the database path is fixed, run the CLI from a disposable working directory if you want to try it without creating `data.db` in the repository.

## Examples

- `basic`: open, put, get, delete, error handling, and persistence after reopening.
- `batch_and_scan`: batches, prefix/range scans, reverse iteration, and scan behavior during updates.
- `metrics_and_compactions`: counters, log growth, compaction, reopening, and Prometheus output.
- `embedded_app`: a miniature typed note repository with a secondary tag index, worker threads, atomic updates, and maintenance.

## Tests and doctests

Run the complete Cargo workspace suite, including unit tests, the integration tests in `rust_database_tests`, and Rust documentation tests:

```sh
cargo test --workspace
cargo test --doc --workspace
```

The workspace tests cover storage/index behavior, batches, scans, persistence and recovery, crash-tail truncation and corruption, concurrent readers and writers, and compaction. Stress test iteration counts can be scaled with `DB_STRESS_SCALE` (a positive integer; defaults to `1`):

```sh
DB_STRESS_SCALE=3 cargo test --workspace
```

The standalone fuzzing crate lives in `rust_database_tests/fuzz` and is not part of the workspace test command. It uses `cargo-fuzz`/libFuzzer targets for record decoding and log recovery. With `cargo-fuzz` installed, run from that directory:

```sh
cd rust_database_tests/fuzz
cargo fuzz run record_deserialize
cargo fuzz run log_recovery
```
