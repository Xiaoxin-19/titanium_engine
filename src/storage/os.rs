use super::traits::{FileMetadata, FileSystem, RandomAccessFile, Storage, WritableFile};
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        use std::os::unix::fs::FileExt;
        fn read_at_impl(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            FileExt::read_at(file, buf, offset)
        }
    } else if #[cfg(windows)] {
        use std::os::windows::fs::FileExt;
        fn read_at_impl(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            FileExt::seek_read(file, buf, offset)
        }
    } else {
        // 兜底逻辑：在不支持的平台上也能编译通过，但运行时返回错误
        fn read_at_impl(_file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "Platform not supported"))
        }
    }
}

// 封装 std::fs::File，提供更好的扩展性（如未来添加 Metrics 或 Path 记录）
pub struct OsFile {
    inner: File,
}

// 为 OsFile 实现 RandomAccessFile
impl RandomAccessFile for OsFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        read_at_impl(&self.inner, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.inner.metadata()?.len())
    }
}

impl Write for OsFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// 为 OsFile 实现 WritableFile
impl WritableFile for OsFile {
    fn sync(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}

// OsFile 满足 Storage (需要实现 Seek)
impl Seek for OsFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Read for OsFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Storage for OsFile {}

/// 默认的 OS 文件系统实现
pub struct OsFileSystem;

impl FileSystem for OsFileSystem {
    fn open_reader(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>> {
        let file = std::fs::File::open(path)?;
        Ok(Box::new(OsFile { inner: file }))
    }

    fn open_file(&self, path: &Path) -> io::Result<Box<dyn Storage>> {
        // OpenOptions 可以根据需要封装更细致的配置
        let file = std::fs::File::options().read(true).write(true).open(path)?;
        Ok(Box::new(OsFile { inner: file }))
    }

    fn create_file(&self, path: &Path) -> io::Result<Box<dyn Storage>> {
        let file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;

        // [FIX] 工业级实践：创建文件后 sync 父目录，防止断电导致文件丢失 (Dentry loss)
        if let Some(parent) = path.parent() {
            // 忽略目录 sync 错误，因为某些环境（如只读挂载）可能不允许，但不应阻断流程
            let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
        }

        Ok(Box::new(OsFile { inner: file }))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            files.push(entry.path());
        }
        Ok(files)
    }

    fn metadata(&self, path: &Path) -> io::Result<FileMetadata> {
        let meta = std::fs::metadata(path)?;
        Ok(FileMetadata {
            len: meta.len(),
            is_file: meta.is_file(),
        })
    }
}
