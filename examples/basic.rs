mod common;

use common::{ScratchDir, text};
use rust_database::{DbError, DbStore};

fn main() -> Result<(), DbError> {
    let dir = ScratchDir::new("basic");
    let db = DbStore::open(dir.path())?;
    println!("opened a store in {}", dir.path().display());

    db.put(b"language".to_vec(), b"Rust".to_vec())?;
    db.put(b"edition".to_vec(), b"2024".to_vec())?;

    let value = db.get(b"language")?;
    println!("language = {}", text(&value));

    db.put(b"edition".to_vec(), b"2021".to_vec())?;
    println!("edition = {}", text(&db.get(b"edition")?));

    match db.get(b"doest-not-exist") {
        Ok(value) => println!("found {}", text(&value)),
        Err(DbError::KeyNotFound) => println!("doest-not-exist: no such key"),
        Err(other) => return Err(other),
    }

    let maybe = match db.get(b"does-not-exist") {
        Ok(value) => Some(value),
        Err(DbError::KeyNotFound) => None,
        Err(other) => return Err(other),
    };
    assert!(maybe.is_none());

    db.delete(b"edition".to_vec())?;
    assert!(matches!(db.get(b"edition"), Err(DbError::KeyNotFound)));

    match db.delete(b"edition".to_vec()) {
        Err(DbError::KeyNotFound) => println!("edition: already deleted"),
        other => panic!("unexpected result: {other:?}"),
    }

    drop(db);
    let db = DbStore::open(dir.path())?;
    assert_eq!(db.get(b"language")?, b"Rust".to_vec());
    assert!(matches!(db.get(b"edition"), Err(DbError::KeyNotFound)));
    println!(
        "after reopening: language = {}",
        text(&db.get(b"language")?)
    );

    Ok(())
}
