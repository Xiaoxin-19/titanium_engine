mod error;
mod index;
mod kv;
mod log_entry;
mod storage;
mod writer;

use std::io::{self, Write};

use crate::error::TitaniumError;
use crate::kv::KVStore;

const DATA_DIR: &str = "./data";

fn main() -> Result<(), TitaniumError> {
    let mut kv_store = KVStore::new(DATA_DIR)?;
    // restore or initialize the KV store as needed
    kv_store.restore()?;

    println!("Welcome to Titanium KV Store!");
    println!("Commands: SET <key> <value> | GET <key> | EXIT");

    let mut input = String::new();
    loop {
        print!("> ");
        io::stdout().flush()?;
        input.clear();

        match io::stdin().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let trimmed = input.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let mut parts = trimmed.split_whitespace();
                let command = parts.next().unwrap_or("").to_uppercase();

                match command.as_str() {
                    "SET" => {
                        if let Some(key) = parts.next() {
                            let value_parts: Vec<&str> = parts.collect();
                            if value_parts.is_empty() {
                                println!("Usage: SET <key> <value>");
                            } else {
                                let value = value_parts.join(" ");
                                match kv_store.set(key.to_string(), value.into_bytes()) {
                                    Ok(_) => println!("OK"),
                                    Err(e) => eprintln!("Error: {}", e),
                                }
                            }
                        } else {
                            println!("Usage: SET <key> <value>");
                        }
                    }
                    "GET" => {
                        if let Some(key) = parts.next() {
                            match kv_store.get(key.to_string()) {
                                Ok(Some(entry)) => match String::from_utf8(entry.value) {
                                    Ok(s) => println!("{}", s),
                                    Err(e) => println!("{:?}", e.into_bytes()),
                                },
                                Ok(None) => println!("(nil)"),
                                Err(e) => eprintln!("Error: {}", e),
                            }
                        } else {
                            println!("Usage: GET <key>");
                        }
                    }
                    "EXIT" => break,
                    _ => println!("Unknown command: {}", command),
                }
            }
            Err(e) => return Err(TitaniumError::Io(e)),
        }
    }
    Ok(())
}
