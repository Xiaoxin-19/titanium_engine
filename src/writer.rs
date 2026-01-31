use std::io::{self, Seek, Write};

use crate::{error::TitaniumError, log_entry::LogEntry};

pub struct Writer<W: Write + Seek> {
    writer: io::BufWriter<W>,
    current_offset: u64, // tracks the current write offset
}

impl<W: Write + Seek> Writer<W> {
    pub fn new(inner: W) -> Self {
        Writer {
            writer: io::BufWriter::new(inner),
            current_offset: 0,
        }
    }

    // Write a log entry, returning the offset at which it was written
    pub fn write_entry(&mut self, entry: &LogEntry) -> Result<u64, TitaniumError> {
        let offset = self.current_offset;
        let bytes_written = LogEntry::encode_to(&entry.key, &entry.value, &mut self.writer)?;
        self.current_offset += bytes_written;
        Ok(offset)
    }

    // Flush the buffer to ensure all data is written to the underlying storage
    pub fn flush(&mut self) -> Result<(), TitaniumError> {
        self.writer.flush()?;
        Ok(())
    }
}
