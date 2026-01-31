mod error;
mod log_entry;
mod writer;

use std::fs::File;

use crate::error::TitaniumError;
use crate::writer::Writer;
fn main() -> Result<(), TitaniumError> {
    let file = File::options()
        .create(true)
        .write(true)
        .open("output.log")?;

    // use the Writer to write log entries to the file
    let mut writer = Writer::new(file);

    for i in 0..10 {
        let key = format!("key{}", i);
        let value = vec![i as u8; 10];
        let entry = log_entry::LogEntry {
            key: key,
            value: value, // value is a vector of ten bytes, each set to i
        };
        let offset = writer.write_entry(&entry)?;
        writer.flush()?;
        println!("Wrote entry at offset {}", offset);
    }
    Ok(())
}
