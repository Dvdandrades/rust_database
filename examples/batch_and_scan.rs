mod common;

use std::ops::Bound;

use common::{ScratchDir, text};
use rust_database::{DbError, DbStore, WriteBatch};

fn balance(db: &DbStore, account: &str) -> Result<i64, DbError> {
    let bytes = db.get(format!("account:{account}").as_bytes())?;
    Ok(text(&bytes)
        .parse()
        .expect("balances are stored as numbers"))
}

fn main() -> Result<(), DbError> {
    let dir = ScratchDir::new("batch_and_scan");
    let db = DbStore::open(dir.path())?;

    let mut load = WriteBatch::with_capacity(8);
    load.put("account:david", "100")
        .put("account:ana", "50")
        .put("order:ana:1", "book")
        .put("order:ana:2", "lamp")
        .put("order:ana:3", "desk")
        .put("order:ana:1", "not ana's order")
        .put("order:david:1", "chair");
    println!("loading {} operations in one batch", load.len());
    db.write_batch(load)?;

    let amount = 30;
    let (ana, david) = (balance(&db, "ana")?, balance(&db, "david")?);
    let mut transfer = WriteBatch::new();
    transfer
        .put("account:ana", (ana - amount).to_string())
        .put("account:david", (david + amount).to_string())
        .put("ledger:0001", format!("ana -> david: {amount}"));
    db.write_batch(transfer)?;
    println!(
        "after transfer: ana = {}, david = {}",
        balance(&db, "ana")?,
        balance(&db, "david")?
    );

    for (id, what) in [("0002", "david -> ana: 5"), ("0003", "ana -> david: 1")] {
        db.put(
            format!("ledger:{id}").into_bytes(),
            what.as_bytes().to_vec(),
        )?;
    }

    println!("\nana's orders:");
    for entry in db.scan_prefix("order:ana:") {
        let (key, value) = entry?;
        println!("  {} => {}", text(&key), text(&value));
    }

    println!("orders in total: {}", db.keys_with_prefix("order:").len());

    println!("ledger entries: {}", db.scan_prefix("ledger:").len());

    println!("\nledger 0001 and 0002 (half-open range):");
    for entry in db.scan_range("ledger:0001".."ledger:0003") {
        let (key, value) = entry?;
        println!("  {} => {}", text(&key), text(&value));
    }

    let after_first = db.scan_range::<&str, _>((
        Bound::Excluded("ledger:0001"),
        Bound::Included("ledger:0003"),
    ));
    println!(
        "ledger entries after 0001, up to 0003: {}",
        after_first.count()
    );

    println!("\nlatest two ledger entries:");
    for entry in db.scan_prefix("ledger:").rev().take(2) {
        let (key, value) = entry?;
        println!("  {} => {}", text(&key), text(&value));
    }
    let (latest_key, _) = db
        .scan_prefix("ledger:")
        .next_back()
        .expect("ledger is not empty")?;
    println!("latest ledger key: {}", text(&latest_key));

    let mut tidy = WriteBatch::new();
    tidy.put("tmp", "first")
        .put("tmp", "second")
        .delete("never-existed")
        .put("scratch", "x")
        .delete("scratch");
    db.write_batch(tidy)?;
    assert_eq!(db.get(b"tmp")?, b"second".to_vec());
    assert!(matches!(db.get(b"scratch"), Err(DbError::KeyNotFound)));

    let scan = db.scan_prefix("order:david:");
    db.put(b"order:david:2".to_vec(), b"added after the scan".to_vec())?;
    db.delete(b"order:david:1".to_vec())?;
    let seen: Vec<_> = scan.collect::<Result<_, _>>()?;
    assert_eq!(seen, [(b"order:david:1".to_vec(), b"chair".to_vec())]);

    let fresh = db.keys_with_prefix("order:david:");
    assert_eq!(fresh, [b"order:david:2".to_vec()]);
    println!(
        "\nold scan saw {:?}; a new scan sees {:?}",
        text(&seen[0].0),
        text(&fresh[0])
    );

    Ok(())
}
