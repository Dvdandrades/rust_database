mod common;

use common::ScratchDir;
use rust_database::{DbError, DbStore, MetricsSnapshot};

fn report(title: &str, m: &MetricsSnapshot) {
    println!("--- {title}");
    println!("  live keys:       {}", m.live_keys);
    println!("  disk size:       {} bytes", m.disk_size_bytes);
    println!(
        "  writes:          {} (puts {}, deletes {}, batch ops {})",
        m.total_writes(),
        m.puts,
        m.deletes,
        m.batch_ops
    );
    println!(
        "  reads:           {} (hits {}, misses {})",
        m.reads, m.get_hits, m.get_misses
    );
    match m.get_hit_ratio() {
        Some(ratio) => println!("  get hit ratio:   {:.0}%", ratio * 100.0),
        None => println!("  get hit ratio:   n/a (no gets yet)"),
    }
    println!("  scans:           {}", m.scans);
    println!("  compactions:     {}", m.compactions);
}

fn main() -> Result<(), DbError> {
    let dir = ScratchDir::new("metrics");
    let db = DbStore::open(dir.path())?;

    report("fresh store", &db.metrics());

    for round in 0..10 {
        for sensor in 0..10 {
            db.put(
                format!("sensor:{sensor:02}").into_bytes(),
                format!("reading-{round}").into_bytes(),
            )?;
        }
    }

    for sensor in 0..12 {
        let _ = db.get(format!("sensor:{sensor:02}").as_bytes());
    }
    let _ = db.scan_prefix("sensor:").count();

    let before = db.metrics();
    report("after 100 overwrites", &before);

    db.compact()?;

    let after = db.metrics();
    report("after compaction", &after);
    println!(
        "\ncompaction shrank the log from {} to {} bytes ({:.1}x smaller)",
        before.disk_size_bytes,
        after.disk_size_bytes,
        before.disk_size_bytes as f64 / after.disk_size_bytes as f64
    );
    assert!(after.disk_size_bytes < before.disk_size_bytes);
    assert_eq!(after.live_keys, 10);
    assert_eq!(after.compactions, 1);

    assert_eq!(db.get(b"sensor:00")?, b"reading-9".to_vec());

    drop(db);
    let db = DbStore::open(dir.path())?;
    let reopened = db.metrics();
    assert_eq!(reopened.puts, 0);
    assert_eq!(reopened.live_keys, 10);
    assert_eq!(reopened.disk_size_bytes, after.disk_size_bytes);
    println!(
        "after reopening: puts = {}, live keys = {}",
        reopened.puts, reopened.live_keys
    );

    println!(
        "\n--- Prometheus output\n{}",
        reopened.to_prometheus("example_db")
    );

    Ok(())
}
