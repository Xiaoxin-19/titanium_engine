mod error;
mod index;
mod kv;
mod log_entry;
mod storage;
mod writer;
use std::fs::File;
use std::io::{Seek, SeekFrom};

use crate::error::TitaniumError;
use crate::log_entry::{Decoder, LogEntry};
use crate::writer::Writer;

fn main() -> Result<(), TitaniumError> {
    let file = File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open("output.log")?;

    // use the Writer to write log entries to the file
    let mut writer = Writer::new(file);

    for i in 0..1000 {
        let key = format!("key{}", i);
        let value = vec![i as u8; 10];
        let entry = LogEntry {
            key,
            value, // value is a vector of ten bytes, each set to i
        };
        let _ = writer.write_entry(&entry)?;
    }
    writer.sync()?;

    // --- validate  ---
    let mut reader_file = File::options().read(true).open("output.log")?;
    reader_file.seek(SeekFrom::Start(0))?; // 回到文件开头
    let mut decoder = Decoder::new();

    for i in 0..1000 {
        let entry = decoder.decode_from(&mut reader_file)?;
        assert_eq!(entry.key, format!("key{}", i));
    }

    Ok(())
}
