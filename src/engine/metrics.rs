use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct Metrics {
    reads: AtomicU64,
    get_hits: AtomicU64,
    get_misses: AtomicU64,
    scans: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    batches: AtomicU64,
    batch_ops: AtomicU64,
    compactions: AtomicU64,
    disk_size_bytes: AtomicU64,
}

impl Metrics {
    pub(crate) fn record_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get_hit(&self) {
        self.get_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get_miss(&self) {
        self.get_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_scan(&self) {
        self.scans.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_put(&self) {
        self.puts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_batch(&self, ops: u64) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.batch_ops.fetch_add(ops, Ordering::Relaxed);
    }

    pub(crate) fn record_compaction(&self) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_disk_size(&self, bytes: u64) {
        self.disk_size_bytes.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, live_keys: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            get_hits: self.get_hits.load(Ordering::Relaxed),
            get_misses: self.get_misses.load(Ordering::Relaxed),
            scans: self.scans.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            batch_ops: self.batch_ops.load(Ordering::Relaxed),
            compactions: self.compactions.load(Ordering::Relaxed),
            live_keys,
            disk_size_bytes: self.disk_size_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub reads: u64,
    pub get_hits: u64,
    pub get_misses: u64,
    pub scans: u64,
    pub puts: u64,
    pub deletes: u64,
    pub batches: u64,
    pub batch_ops: u64,
    pub compactions: u64,
    pub live_keys: u64,
    pub disk_size_bytes: u64,
}

impl MetricsSnapshot {
    pub fn total_writes(&self) -> u64 {
        self.puts + self.deletes + self.batch_ops
    }

    pub fn get_hit_ratio(&self) -> Option<f64> {
        let lookups = self.get_hits + self.get_misses;
        (lookups > 0).then(|| self.get_hits as f64 / lookups as f64)
    }

    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut out = String::new();
        let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP {prefix}_{name} {help}");
            let _ = writeln!(out, "# TYPE {prefix}_{name} {kind}");
            let _ = writeln!(out, "{prefix}_{name} {value}");
        };

        metric("reads_total", "counter", "Calss to get.", self.reads);
        metric(
            "get_hits_total",
            "counter",
            "Gets that found the key.",
            self.get_hits,
        );
        metric(
            "scans_total",
            "counter",
            "Prefix and range scans.",
            self.scans,
        );
        metric("puts_total", "counter", "Successful puts.", self.puts);
        metric(
            "deletes_total",
            "counter",
            "Successful deletes.",
            self.deletes,
        );
        metric(
            "batches_total",
            "counter",
            "Successful write batches.",
            self.batches,
        );
        metric(
            "batch_ops_total",
            "counter",
            "Operations contained in write batches.",
            self.batch_ops,
        );
        metric(
            "compactions total",
            "counter",
            "Completed compactions.",
            self.compactions,
        );
        metric(
            "live_keys",
            "gauge",
            "Keys currently in the index.",
            self.live_keys,
        );
        metric(
            "disk_size_bytes",
            "gauge",
            "Current size of the log segments in bytes.",
            self.disk_size_bytes,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_reflects_counters() {
        let m = Metrics::default();
        assert_eq!(m.snapshot(0), MetricsSnapshot::default());

        m.record_read();
        m.record_read();
        m.record_get_hit();
        m.record_get_miss();
        m.record_scan();
        m.record_put();
        m.record_delete();
        m.record_batch(4);
        m.record_compaction();
        m.set_disk_size(123);
        m.set_disk_size(456);

        let s = m.snapshot(7);
        assert_eq!(s.reads, 2);
        assert_eq!(s.get_hits, 1);
        assert_eq!(s.get_misses, 1);
        assert_eq!(s.scans, 1);
        assert_eq!(s.puts, 1);
        assert_eq!(s.deletes, 1);
        assert_eq!(s.batches, 1);
        assert_eq!(s.batch_ops, 4);
        assert_eq!(s.compactions, 1);
        assert_eq!(s.live_keys, 7);
        assert_eq!(s.disk_size_bytes, 456, "gauge keeps the last value");
        assert_eq!(s.total_writes(), 6);
    }

    #[test]
    fn test_hit_ratio() {
        assert_eq!(MetricsSnapshot::default().get_hit_ratio(), None);

        let s = MetricsSnapshot {
            get_hits: 3,
            get_misses: 1,
            ..Default::default()
        };
        assert_eq!(s.get_hit_ratio(), Some(0.75));
    }

    #[test]
    fn test_prometheus_output() {
        let s = MetricsSnapshot {
            reads: 42,
            disk_size_bytes: 1024,
            ..Default::default()
        };
        let text = s.to_prometheus("db");

        assert!(text.contains("# TYPE db_reads_total counter\ndb_reads_total 42\n"));
        assert!(text.contains("# TYPE db_disk_size_bytes gauge\ndb_disk_size_bytes 1024\n"));
        assert!(text.contains("db_live_keys 0\n"));
    }
}
