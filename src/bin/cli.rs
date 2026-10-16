use rust_database::DbStore;
use std::env;
use std::process;

fn print_usage() {
    println!("Database usage:");
    println!("  cargo run --bin cli put <key> <value>");
    println!("  cargo run --bin cli get <key>");
    println!("  cargo run --bin cli delete <key>");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        print_usage();
        process::exit(1);
    }

    let command = &args[1];
    let db_path = "data.db";

    let mut store = match DbStore::open(db_path) {
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
        _ => {
            eprintln!("Unknown command: {}", command);
            print_usage();
            process::exit(1);
        }
    }
}
