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
    pub fn new(inner: W) -> Self {
        Self {
            writer: io::BufWriter::new(inner),
            current_offset: 0,
            // 💡 思考：如果是追加模式，这里应该 seek 到文件末尾获取初始 offset
            // 但目前 Day 2 假设新文件，0 是可以的。
        }
    }

    pub fn write_entry(&mut self, entry: &LogEntry) -> Result<u64, TitaniumError> {
        let offset = self.current_offset;
        let bytes_written = LogEntry::encode_to(&entry.key, &entry.value, &mut self.writer)?;
        self.current_offset += bytes_written;
        Ok(offset)
    }

    // 普通的 flush，仅推送到系统缓存 (快，不安全)
    pub fn flush(&mut self) -> Result<(), TitaniumError> {
        self.writer.flush()?;
        Ok(())
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
}
