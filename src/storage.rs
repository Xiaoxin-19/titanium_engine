use std::fs::File;
use std::io::{self, Seek, Write};

// 定义一个新 Trait，集合了我们需要的三个能力
pub trait Storage: Write + Seek {
    fn sync(&mut self) -> io::Result<()>;
}

// 为 File 实现这个 Trait
impl Storage for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all() // <--- 这才是真正的物理落盘指令
    }
}

// 同时也为 Cursor<Vec<u8>> 实现，方便单元测试
impl Storage for io::Cursor<Vec<u8>> {
    fn sync(&mut self) -> io::Result<()> {
        Ok(()) // 内存操作不需要 sync
    }
}
