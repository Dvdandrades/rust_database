use rust_database::{DbStore, Scan, WriteBatch};
use std::env;
use std::process;

fn print_usage() {
    println!("Database usage:");
    println!("  cargo run --bin cli put <key> <value>");
    println!("  cargo run --bin cli get <key>");
    println!("  cargo run --bin cli delete <key>");
    println!("  cargo run --bin cli batch (put <key> <value> | delete <key>)...");
    println!("    e.g. cargo run --bin cli batch put a 1 put b 2 delete c");
    println!("  cargo run --bin cli scan <prefix>        (use \"\" for every key)");
    println!("  cargo run --bin cli range <start> <end>  (start inclusive, end exclusive)");
}

fn print_scan(scan: Scan<'_>) {
    let mut count = 0usize;
    for item in scan {
        match item {
            Ok((key, value)) => {
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
                count += 1;
            }
            Err(e) => {
                eprintln!("Error reading entry: {}", e);
                process::exit(1);
            }
        }
    }
    eprintln!("{} entries", count);
}

fn parse_batch(args: &[String]) -> Result<WriteBatch, String> {
    let mut batch = WriteBatch::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "put" => {
                if i + 2 >= args.len() {
                    return Err("'put' needs a key and a value".to_string());
                }
                batch.put(args[i + 1].as_bytes(), args[i + 2].as_bytes());
                i += 3;
            }
            "delete" => {
                if i + 1 >= args.len() {
                    return Err("'delete' needs a key".to_string());
                }
                batch.delete(args[i + 1].as_bytes());
                i += 2;
            }
            other => return Err(format!("Unknown batch operation: {}", other)),
        }
    }
    Ok(batch)
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        print_usage();
        process::exit(1);
    }

    let command = &args[1];
    let db_path = "data.db";

    let store = match DbStore::open(db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            process::exit(1);
        }
    };

    match command.as_str() {
        "put" => {
            if args.len() < 4 {
                eprintln!("Incorrect use. Example: cargo run --bin cli put my_key my_value");
                process::exit(1);
            }
            let key = args[2].as_bytes().to_vec();
            let value = args[3].as_bytes().to_vec();

            match store.put(key, value) {
                Ok(_) => println!("OK"),
                Err(e) => {
                    eprintln!("Error inserting: {}", e);
                    process::exit(1);
                }
            }
        }
        "get" => {
            let key = args[2].as_bytes().to_vec();

            match store.get(&key) {
                Ok(value) => match String::from_utf8(value) {
                    Ok(v_str) => println!("{}", v_str),
                    Err(err) => println!("Binary value (non UTF-8): {:?}", err),
                },
                Err(e) => {
                    eprintln!("Error: {}", e);
                    process::exit(1);
                }
            }
        }
        "delete" => {
            let key = args[2].as_bytes().to_vec();

            match store.delete(key) {
                Ok(_) => println!("OK"),
                Err(e) => {
                    eprintln!("Error deleting: {}", e);
                    process::exit(1);
                }
            }
        }
        "batch" => {
            let batch = match parse_batch(&args[2..]) {
                Ok(b) => b,
                Err(msg) => {
                    eprintln!("Error: {}", msg);
                    print_usage();
                    process::exit(1);
                }
            };
            let count = batch.len();

            match store.write_batch(batch) {
                Ok(_) => println!("OK {} operations", count),
                Err(e) => {
                    eprintln!("Error writing batch: {}", e);
                    process::exit(1);
                }
            }
        }
        "scan" => print_scan(store.scan_prefix(args[2].as_bytes())),
        "range" => {
            if args.len() < 4 {
                eprintln!("Incorrect use. Example: cargo run --bin cli range a m");
                process::exit(1);
            }
            print_scan(store.scan_range(args[2].as_str()..args[3].as_str()));
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            print_usage();
            process::exit(1);
        }
    }
}
