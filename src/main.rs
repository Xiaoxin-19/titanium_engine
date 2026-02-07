mod compaction;
mod config;
mod error;
mod index;
mod kv;
mod log_entry;
mod storage;
mod utils;
mod writer;

use std::io::{self, Write};
use std::sync::Arc;

use crate::config::{ConfigWatcher, DEFAULT_CONFIG_FILE};
use crate::error::TitaniumError;
use crate::kv::KVStore;
use crate::storage::OsFileSystem;

fn main() -> Result<(), TitaniumError> {
    ConfigWatcher::init(DEFAULT_CONFIG_FILE)?;
    let watcher = ConfigWatcher::global().clone();
    let fs = Arc::new(OsFileSystem);
    let mut kv_store = KVStore::new(watcher, fs)?;
    // restore or initialize the KV store as needed
    kv_store.restore()?;

    println!("Welcome to Titanium KV Store!");
    println!("Commands: SET <key> <value> | GET <key> | RM <key> | EXIT");

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
                    "RM" => {
                        if let Some(key) = parts.next() {
                            match kv_store.remove(key) {
                                Ok(_) => println!("OK"),
                                Err(e) => eprintln!("Error: {}", e),
                            }
                        } else {
                            println!("Usage: RM <key>");
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
