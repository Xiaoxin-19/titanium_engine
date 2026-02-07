use super::traits::{FileMetadata, FileSystem, RandomAccessFile, Storage, WritableFile};
use parking_lot::RwLock;
use std::cmp;
use std::collections::HashMap;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// 为 Cursor 实现 RandomAccessFile
impl RandomAccessFile for io::Cursor<Vec<u8>> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let start = offset as usize;
        let end = start + buf.len();
        let inner = self.get_ref();
        if start >= inner.len() {
            return Ok(0);
        }
        let end = std::cmp::min(end, inner.len());
        let n = end - start;
        buf[..n].copy_from_slice(&inner[start..end]);
        Ok(n)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.get_ref().len() as u64)
    }
}

// 为 Cursor 实现 WritableFile
impl WritableFile for io::Cursor<Vec<u8>> {
    fn sync(&mut self) -> io::Result<()> {
        Ok(()) // 内存操作不需要 sync
    }

    fn set_len(&self, _len: u64) -> io::Result<()> {
        Ok(()) // 内存 Cursor 暂不支持截断，或者需要 RefCell 内部可变性
    }
}

impl Storage for io::Cursor<Vec<u8>> {}

// --- In-Memory File System (For Testing) ---

#[derive(Clone)]
pub struct MemFileSystem {
    // Path -> File Content
    files: Arc<RwLock<HashMap<PathBuf, Arc<RwLock<Vec<u8>>>>>>,
}

impl MemFileSystem {
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

struct MemFile {
    data: Arc<RwLock<Vec<u8>>>,
    pos: u64,
}

impl RandomAccessFile for MemFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let guard = self.data.read();
        let start = offset as usize;
        if start >= guard.len() {
            return Ok(0);
        }
        let end = cmp::min(start + buf.len(), guard.len());
        let n = end - start;
        buf[..n].copy_from_slice(&guard[start..end]);
        Ok(n)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.data.read().len() as u64)
    }
}

impl WritableFile for MemFile {
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.data.write().resize(len as usize, 0);
        Ok(())
    }
}

impl Seek for MemFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let len = self.data.read().len() as u64;
        match pos {
            io::SeekFrom::Start(p) => self.pos = p,
            io::SeekFrom::End(p) => self.pos = (len as i64 + p) as u64,
            io::SeekFrom::Current(p) => self.pos = (self.pos as i64 + p) as u64,
        }
        Ok(self.pos)
    }
}

impl Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.data.write();
        let pos = self.pos as usize;
        let end = pos + buf.len();
        if end > guard.len() {
            guard.resize(end, 0);
        }
        guard[pos..end].copy_from_slice(buf);
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.data.read();
        let pos = self.pos as usize;
        if pos >= guard.len() {
            return Ok(0);
        }
        let end = cmp::min(pos + buf.len(), guard.len());
        let n = end - pos;
        buf[..n].copy_from_slice(&guard[pos..end]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Storage for MemFile {}

impl FileSystem for MemFileSystem {
    fn open_reader(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>> {
        let guard = self.files.read();
        let data = guard
            .get(path)
            .ok_or(io::Error::new(io::ErrorKind::NotFound, "File not found"))?;
        Ok(Box::new(MemFile {
            data: data.clone(),
            pos: 0,
        }))
    }
    fn open_file(&self, path: &Path) -> io::Result<Box<dyn Storage>> {
        let guard = self.files.read();
        let data = guard
            .get(path)
            .ok_or(io::Error::new(io::ErrorKind::NotFound, "File not found"))?;
        Ok(Box::new(MemFile {
            data: data.clone(),
            pos: 0,
        }))
    }
    fn create_file(&self, path: &Path) -> io::Result<Box<dyn Storage>> {
        let mut guard = self.files.write();
        let data = Arc::new(RwLock::new(Vec::new()));
        guard.insert(path.to_path_buf(), data.clone());
        Ok(Box::new(MemFile { data, pos: 0 }))
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.files.write().remove(path);
        Ok(())
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut guard = self.files.write();
        if let Some(data) = guard.remove(from) {
            guard.insert(to.to_path_buf(), data);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "File not found"))
        }
    }
    fn exists(&self, path: &Path) -> bool {
        self.files.read().contains_key(path)
    }
    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let guard = self.files.read();
        Ok(guard
            .keys()
            .filter(|p| p.parent() == Some(path))
            .cloned()
            .collect())
    }
    fn metadata(&self, path: &Path) -> io::Result<FileMetadata> {
        let guard = self.files.read();
        let data = guard
            .get(path)
            .ok_or(io::Error::new(io::ErrorKind::NotFound, "File not found"))?;
        Ok(FileMetadata {
            len: data.read().len() as u64,
            is_file: true,
        })
    }
}
