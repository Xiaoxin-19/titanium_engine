use crate::error::TitaniumError;
use crate::log_entry::LogEntry;
use crate::storage::Storage;
use std::io;
use std::io::Write;
// 修改泛型约束，使用我们新的 Storage trait
pub struct Writer<W: Storage> {
    writer: io::BufWriter<W>,
    current_offset: u64,
}

impl<W: Storage> Writer<W> {
    pub fn new(inner: W, offset: u64) -> Self {
        Self {
            // TODO: [File Header] Write a fixed-length header at the beginning of new files (offset == 0).
            // Layout: MagicNumber(4B) + Version(1B) + EncryptionSalt(Optional).
            // Example: b"TITN" + 0x01
            // This helps in identifying valid data files and handling format migrations.
            writer: io::BufWriter::new(inner),
            current_offset: offset,
            // 💡 思考：如果是追加模式，这里应该 seek 到文件末尾获取初始 offset
            // TODO: Ensure the inner writer is actually at the correct offset if appending to an existing file.
            // 但目前 Day 2 假设新文件，0 是可以的。
        }
    }

    pub fn write_entry(&mut self, entry: &LogEntry) -> Result<u64, TitaniumError> {
        let offset = self.current_offset;
        let bytes_written = entry.encode_to(&mut self.writer)?;
        self.current_offset += bytes_written;
        Ok(offset)
    }

    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    // 获取内部 writer 的引用，用于读取
    pub fn get_ref(&self) -> &W {
        self.writer.get_ref()
    }

    // ⚡️ 真正的落盘 (慢，安全)
    // 通常仅在事务提交或关键数据写入时调用
    pub fn sync(&mut self) -> Result<(), TitaniumError> {
        // 1. 先把 BufWriter 的数据推给内核
        self.writer.flush()?;
        // 2. 再命令内核推给磁盘
        self.writer.get_mut().sync()?;
        Ok(())
    }

    // 仅刷新到操作系统缓存 (快，保证 Read-Your-Writes 可见性)
    pub fn flush_to_os(&mut self) -> Result<(), TitaniumError> {
        self.writer.flush()?;
        Ok(())
    }

    // 供 KVStore::restore 使用：当发现 active file 数据损坏并截断后，需要修正内存中的 offset
    pub(crate) fn set_offset(&mut self, offset: u64) {
        self.current_offset = offset;
    }
}
