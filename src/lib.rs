//! read-modify-write primitive: if two threads must update the same key
//! based on its current value, coordinate them yourself.
//! *Compaction is manual and blocks other operations while it runs.
//!
//! ## Examples
//!
//! The `examples/` directory shows how to embed the store in a Rust
//! application. Run any of them with `cargo run --example <name>`:
//!
//! * `basic`: open, put, get, delete, error handling and persistence.
//! * `batch_and_scan`: atomic batches, prefix and range scans, reverse scans.
//! * `metrics_and_compactions`: dead space, `compact`, counters and the
//!   Prometheus output.
//! * `embedded_app`: a complete miniature service (typed repository, secondary
//!   index kept consistent with batches, worker threads, maintenance).
//!
//! There is also a small command-line front end, `cargo run --bin cli`, which
//! prints its usage when run without arguments.

/// The storage engine: the [`DbStore`] handle, its [`Scan`] iterator and the
/// metrics counters behind [`MetricsSnapshot`].
pub mod engine;

/// The [`DbError`] type and the [`Result`] alias returned by every fallible
/// operation.
pub mod error;

/// The data model: the on-disk [`Record`] and the [`WriteBatch`] builder used
/// with [`DbStore::write_batch`].
pub mod model;

/// Low-level building blocks used by [`DbStore`]: the segmented, checksummed
/// log and the in-memory key index. Applications normally never need these;
/// use [`DbStore`] instead.
pub mod storage;

pub use engine::db::{DbStore, Scan};
pub use engine::metrics::MetricsSnapshot;
pub use error::{DbError, Result};
pub use model::record::{BatchOp, Record, WriteBatch};
