use std::io;
use std::path::{Path, PathBuf};

/// [Capability Trait] 随机读取能力
///
/// 对应场景：读取不可变的归档文件 (SSTable) 或活跃文件的读路径。
/// 核心特性：
/// 1. `read_at` 是无状态的（不改变文件游标），支持多线程并发读取。
/// 2. 类似于 Unix 的 `pread` 或 Windows 的 `ReadFile` (Overlapped)。
pub trait RandomAccessFile: Send + Sync {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    fn len(&self) -> io::Result<u64>;
}

/// [Capability Trait] 顺序写入能力
///
/// 对应场景：写入活跃日志文件 (WAL)。
/// 核心特性：
/// 1. 继承 `std::io::Write`，支持 `write`, `flush`。
/// 2. 提供 `sync` (fsync) 确保数据落盘。
pub trait WritableFile: std::io::Write + Send + Sync {
    fn sync(&mut self) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
}

/// [Composite Trait] 全能存储对象
///
/// 对应场景：活跃文件 (Active File)，既需要追加写，也需要随机读，还需要 Seek (截断/恢复)。
/// 这是一个 "Super Trait"，它组合了所有能力。
pub trait Storage: RandomAccessFile + WritableFile + std::io::Seek + std::io::Read {}

// --- Boilerplate: 动态分发 (Dynamic Dispatch) 适配 ---
//
// Rust 的 Trait Object (如 Box<dyn Storage>) 不会自动继承 Trait 的方法。
// 我们必须手动为 Box<dyn Storage> 实现这些 Trait，将调用转发给内部的具体对象 (**self)。
// 这样 Writer<Box<dyn Storage>> 才能正常工作。

impl RandomAccessFile for Box<dyn Storage> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }
}

impl WritableFile for Box<dyn Storage> {
    fn sync(&mut self) -> io::Result<()> {
        (**self).sync()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        (**self).set_len(len)
    }
}

impl Storage for Box<dyn Storage> {}

// 为 Box<dyn RandomAccessFile> 实现 RandomAccessFile (方便 file_map 使用)
impl RandomAccessFile for Box<dyn RandomAccessFile> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }
}

/// [Factory Trait] 文件系统抽象
///
/// 职责：负责文件的生命周期管理 (CRUD) 和 路径解析。
/// 核心价值：
/// 1. **解耦**: KVStore 不再依赖 `std::fs`，而是依赖这个接口。
/// 2. **测试**: 可以注入 `MemFileSystem` 进行纯内存测试。
/// 3. **扩展**: 未来可以实现 `S3FileSystem` 直接读写对象存储。
pub trait FileSystem: Send + Sync {
    fn open_reader(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>>;
    fn open_file(&self, path: &Path) -> io::Result<Box<dyn Storage>>;
    fn create_file(&self, path: &Path) -> io::Result<Box<dyn Storage>>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    fn metadata(&self, path: &Path) -> io::Result<FileMetadata>;
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub len: u64,
    pub is_file: bool,
}
