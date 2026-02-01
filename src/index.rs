pub struct LogIndex {
    pub file_id: u32,
    pub val_len: u32,
    pub offset: u64,
}

impl LogIndex {
    pub fn new(file_id: u32, offset: u64, val_len: u32) -> Self {
        LogIndex {
            file_id,
            offset,
            val_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_size() {
        // 打印 LogIndex 的内存大小
        // Rust 默认会对字段重排以减少 padding：offset(8) + file_id(4) + val_len(4) = 16 bytes
        println!(
            "Size of LogIndex: {} bytes",
            std::mem::size_of::<LogIndex>()
        );
    }
}
