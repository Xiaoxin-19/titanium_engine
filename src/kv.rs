use crate::config;
use crate::error::TitaniumError;
use crate::index::{HashIndexer, Indexer, LogIndex};
use crate::log_entry::{Decoder, LogEntry};
use crate::storage::{FileSystem, RandomAccessFile, Storage};
use crate::writer::Writer;
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 一个辅助结构体，用于将 read_at 适配为 Read trait
/// 这样 Decoder 就可以在不改变文件游标的情况下读取数据
pub struct FileAtReader<'a> {
    pub reader: &'a dyn RandomAccessFile,
    pub offset: u64,
}

impl<'a> Read for FileAtReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl<'a> Seek for FileAtReader<'a> {
    fn rewind(&mut self) -> io::Result<()> {
        self.offset = 0;
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset)
    }

    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        self.seek(SeekFrom::Current(offset))?;
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_offset = match pos {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(offset) => self.offset.checked_add_signed(offset),
            SeekFrom::End(offset) => {
                let len = self.reader.len()?;
                len.checked_add_signed(offset)
            }
        };

        if let Some(n) = new_offset {
            self.offset = n;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            ));
        }
        Ok(self.offset)
    }
}

pub struct KVStore {
    pub(crate) indexer: Box<dyn Indexer>,
    writer: Writer<Box<dyn Storage>>,
    pub(crate) fs: Arc<dyn FileSystem>,
    pub(crate) file_map: HashMap<u32, (Box<dyn RandomAccessFile>, PathBuf)>,
    data_path: PathBuf,
    active_file_id: u32,
    pub(crate) config: config::ConfigWatcher,
    current_seq_no: u64,
}

impl KVStore {
    pub fn new(
        config: config::ConfigWatcher,
        fs: Arc<dyn FileSystem>,
    ) -> Result<Self, TitaniumError> {
        // 扫描目录，查找数据文件，如果没有目录，则创建对应目录，并初始化bs文件
        let data_path = config.get().data_dir;
        let root_path = Path::new(&data_path);
        if !fs.exists(root_path) {
            fs.create_dir_all(root_path)?;
        }

        // 1. 扫描所有 .bs 文件并提取 ID
        let mut file_ids: Vec<u32> = fs
            .list_files(root_path)?
            .into_iter()
            .filter(|path| path.is_file() && path.extension().map_or(false, |ext| ext == "bs"))
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u32>().ok())
            })
            .collect();

        file_ids.sort();

        // 2. 确定 active_file_id
        // 检查最后一个文件是否写满，没写满则追加（复用），写满则轮转。
        let mut active_file_id = 1;

        if let Some(&last_id) = file_ids.last() {
            let last_path = root_path.join(format!("{:04}.bs", last_id));
            let len = fs.metadata(&last_path).map(|m| m.len).unwrap_or(0);

            if len < config.max_file_size() as u64 {
                active_file_id = last_id;
                file_ids.pop(); // 从归档列表中移除，因为它将作为 active file
            } else {
                active_file_id = last_id + 1;
            }
        }

        let active_file_name = format!("{:04}.bs", active_file_id);
        let active_path = root_path.join(active_file_name);

        // 3. 打开活跃文件 (Append 模式)
        // FileSystem 抽象通常不区分 append/create 选项，而是由实现决定
        // 这里我们假设 create_file 或 open_file 能满足需求。
        // 对于 MemFileSystem，create_file 会截断，open_file 会保留。
        // 我们需要一个能 "Open or Create" 的语义。
        // 简化起见，如果存在则 open，不存在则 create。
        let mut active_file = if fs.exists(&active_path) {
            fs.open_file(&active_path)?
        } else {
            fs.create_file(&active_path)?
        };

        // TODO: [File Header] If active_file is new (len == 0), write the File Header immediately.
        // If it's an existing file, read and validate the Magic Number and Version.

        let file_len = active_file.len()?;

        // 打开现有文件进行追加写时，必须将游标移动到文件末尾
        active_file.seek(io::SeekFrom::Start(file_len))?;

        let writer = Writer::new(active_file, file_len);

        // 4. 加载旧文件到 file_map (只读)
        let mut file_map = HashMap::new();
        for &id in &file_ids {
            let path = root_path.join(format!("{:04}.bs", id));
            let file = fs.open_reader(&path)?;
            // TODO: [File Header] Validate Magic Number and Version for each immutable file.
            // 注意：open_reader 返回 Box<dyn RandomAccessFile>
            file_map.insert(id, (file, path));
        }

        Ok(KVStore {
            indexer: Box::new(HashIndexer::new()),
            writer,
            fs,
            file_map: file_map,
            data_path: root_path.to_path_buf(),
            active_file_id,
            config,
            current_seq_no: 0,
        })
    }

    /// 获取下一个序列号，如果溢出则返回错误
    fn next_seq_no(&mut self) -> Result<u64, TitaniumError> {
        self.current_seq_no = self.current_seq_no.checked_add(1).ok_or_else(|| {
            TitaniumError::Io(io::Error::new(
                io::ErrorKind::Other,
                "Sequence number overflow: database limit reached",
            ))
        })?;
        Ok(self.current_seq_no)
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) -> Result<(), TitaniumError> {
        // 0. 检查是否需要轮转文件
        if self.writer.current_offset() >= self.config.max_file_size() as u64 {
            self.rotate()?;
        }

        // TODO: Support WriteBatch (Atomic updates for multiple keys).
        // This would involve writing a batch header or using a transaction marker in EntryType.
        let seq_no = self.next_seq_no()?;

        // 1. write to log file
        let entry = LogEntry::new(key, value, seq_no).build();
        let offset = self.writer.write_entry(&entry)?;
        // use config to decide when to sync
        match self.config.write_mod() {
            config::WriteMod::Sync => self.writer.sync()?,
            config::WriteMod::Buffer => self.writer.flush_to_os()?,
        }
        // 2. update indexer
        self.indexer.put(
            entry.key,
            LogIndex::new(self.active_file_id, offset, entry.value.len() as u32),
        );
        Ok(())
    }

    /// 支持 TTL (过期时间) 的写入接口
    ///
    /// # 参数
    /// - `ttl`: 数据存活时长。例如 `Duration::from_secs(60)` 表示 60 秒后过期。
    pub fn set_with_ttl(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl: std::time::Duration,
    ) -> Result<(), TitaniumError> {
        if self.writer.current_offset() >= self.config.max_file_size() as u64 {
            self.rotate()?;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let expire_at = now + ttl.as_millis() as u64;

        let seq_no = self.next_seq_no()?;

        let entry = LogEntry::new(key.to_string(), value, seq_no)
            .with_ttl(expire_at)
            .build();
        let offset = self.writer.write_entry(&entry)?;
        match self.config.write_mod() {
            config::WriteMod::Sync => self.writer.sync()?,
            config::WriteMod::Buffer => self.writer.flush_to_os()?,
        }
        self.indexer.put(
            entry.key,
            LogIndex::new(self.active_file_id, offset, entry.value.len() as u32),
        );
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> Result<(), TitaniumError> {
        // 1. 如果 Key 存在，则写入 Tombstone
        if self.indexer.get(&key).is_some() {
            let seq_no = self.next_seq_no()?;

            let entry = LogEntry::new_tombstone(key.to_string(), seq_no);
            self.writer.write_entry(&entry)?;

            match self.config.write_mod() {
                config::WriteMod::Sync => self.writer.sync()?,
                config::WriteMod::Buffer => self.writer.flush_to_os()?,
            }
            // 2. 从内存索引中移除
            self.indexer.remove(&key);
        }
        Ok(())
    }

    /// 轮转活跃文件：将当前文件转为只读归档，并创建新的活跃文件
    fn rotate(&mut self) -> Result<(), TitaniumError> {
        // 1. 强制刷盘，确保旧数据落盘
        self.writer.sync()?;

        // 2. 将当前的 active_file 加入到 file_map 中 (作为只读)
        // 注意：我们需要重新以只读模式打开它，或者复用路径
        let old_id = self.active_file_id;
        let old_path = self.data_path.join(format!("{:04}.bs", old_id));

        // 重新打开为只读句柄放入 map，供 get 使用
        let old_file = self.fs.open_reader(&old_path)?;
        self.file_map.insert(old_id, (old_file, old_path));

        // 3. 更新 active_file_id 并创建新文件
        self.active_file_id += 1;
        let new_path = self
            .data_path
            .join(format!("{:04}.bs", self.active_file_id));

        let new_file = self.fs.create_file(&new_path)?;

        // 4. 替换 Writer
        // Writer::new 会初始化 offset，如果是新文件则为 0
        self.writer = Writer::new(new_file, 0);

        Ok(())
    }

    /// 手动触发刷盘，将缓冲区数据写入磁盘
    pub fn sync(&mut self) -> Result<(), TitaniumError> {
        self.writer.sync()
    }

    pub fn get(&self, key: String) -> Result<Option<LogEntry>, TitaniumError> {
        let log_index = match self.indexer.get(&key) {
            Some(index) => index,
            None => return Ok(None),
        };

        // 区分读取的是归档文件还是当前的活跃文件
        // 如果是活跃文件，我们需要从 writer 中获取（或者如果 writer 的文件句柄支持 read，也可以直接用）
        // 但为了简化，我们在 new/rotate 时确保 active_file 也是可读的，
        // 并且我们不把 active_file 放入 file_map，所以这里需要特殊处理
        let reader: &dyn RandomAccessFile = if log_index.file_id == self.active_file_id {
            self.writer.get_ref().as_ref()
        } else {
            self.file_map[&log_index.file_id].0.as_ref()
        };

        // 使用 FileAtReader 替代 seek，实现无锁并发读取
        let mut reader = FileAtReader {
            reader,
            offset: log_index.offset,
        };

        // 使用 Thread-local Decoder 减少高频读取时的内存分配开销
        thread_local! {
            static DECODER: std::cell::RefCell<Decoder> = std::cell::RefCell::new(Decoder::new(0, 0));
        }

        let entry = DECODER.with(|cell| {
            let mut decoder = cell.borrow_mut();
            let (max_key, max_val) = self.config.max_sizes();
            decoder.set_limits(max_key, max_val);
            decoder.decode_from(&mut reader)?.ok_or_else(|| {
                TitaniumError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF at indexed offset",
                ))
            })
        })?;

        // [TTL Check] 检查数据是否过期
        if let Some(expire_at) = entry.expire_at() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if now > expire_at {
                // 数据已过期，返回 None (惰性删除：索引中还在，但用户读不到)
                return Ok(None);
            }
        }

        Ok(Some(entry))
    }

    // 程序重启后，恢复 KVStore 状态
    pub fn restore(&mut self) -> Result<(), TitaniumError> {
        let (max_key, max_val) = self.config.max_sizes();
        let mut decoder = Decoder::new(max_key, max_val);

        // TODO: Implement Hint file loading for faster startup.
        // 1. Check if a valid .hbs file exists for each .bs file.
        // 2. If yes, load index directly from .hbs (avoids reading large values).
        // 3. If no (or checksum mismatch), fall back to the current full scan recovery.
        // 1. 获取所有 file_id 并排序，确保按时间顺序恢复数据 (旧 -> 新)
        let mut file_ids: Vec<u32> = self.file_map.keys().cloned().collect();
        file_ids.sort();

        // 别忘了加上当前的 active_file_id，因为它不在 file_map 中
        file_ids.push(self.active_file_id);

        for file_id in &file_ids {
            let is_active = *file_id == self.active_file_id;

            let active_path_buf; // 声明变量以延长生命周期
            let (reader, file_path): (&dyn RandomAccessFile, &PathBuf) = if is_active {
                active_path_buf = self.data_path.join(format!("{:04}.bs", file_id));
                (self.writer.get_ref().as_ref(), &active_path_buf)
            } else {
                let (f, p) = &self.file_map[file_id];
                (f.as_ref(), p)
            };

            let mut reader = std::io::BufReader::new(FileAtReader { reader, offset: 0 });
            // [Optimization] 提前获取文件长度，避免在循环中对每个 Entry 调用 syscall (stat)
            let file_len = reader.get_ref().reader.len()?;

            // TODO: [File Header] Skip the fixed-length file header before reading entries.
            // reader.seek_relative(HEADER_SIZE)?;

            loop {
                let offset = reader.stream_position()?; // 记录起始位置
                match decoder.decode_header_and_key(&mut reader) {
                    // 1. 完美读取
                    Ok(Some(header)) => {
                        // [FIX] 预检查：确保文件剩余内容足够容纳 Body (BodyCRC + Value)
                        // 使用循环外获取的 file_len，纯内存比较，零开销。
                        let current_pos = reader.stream_position()?;
                        let body_len = 4 + header.val_len as u64;

                        if current_pos + body_len > file_len {
                            eprintln!(
                                "Recover: Incomplete entry body at file {} offset {}. Truncating.",
                                file_id, offset
                            );
                            if is_active {
                                self.writer.get_ref().set_len(offset)?;
                                self.writer.set_offset(offset);
                            } else {
                                let write_file = self.fs.open_file(file_path)?;
                                write_file.set_len(offset)?;
                            }
                            break;
                        }

                        // 恢复最大的序列号
                        // 使用 max 而不是直接赋值，是为了防止：
                        // 1. Compaction 产生的归档文件可能包含较旧的序列号，但文件 ID 较新。
                        // 2. 确保 next_seq_no 生成的序号永远大于数据库中已存在的任何序号。
                        self.current_seq_no = self.current_seq_no.max(header.sequence_number);

                        if header.is_tombstone() {
                            self.indexer.remove(&header.key);
                        } else {
                            self.indexer.put(
                                header.key,
                                LogIndex::new(*file_id as u32, offset, header.val_len),
                            );
                        }

                        // 关键优化：跳过 Value 部分 (BodyCRC 4 bytes + Value)
                        // 这样我们就不需要从磁盘读取 Value，大大加速启动
                        reader.seek_relative(body_len as i64)?;
                    }

                    // 2. 完美结束 (EOF)
                    Ok(None) => break,

                    // 3. 数据损坏 (CRC 错, 意外EOF, Varint错, 数据超长) -> 执行截断
                    Err(e) => {
                        let is_corruption = match &e {
                            TitaniumError::CrcMismatch { .. }
                            | TitaniumError::VarintDecodeError => true,
                            TitaniumError::Io(io_e) => matches!(
                                io_e.kind(),
                                io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                            ),
                            _ => false,
                        };

                        if !is_corruption {
                            return Err(e);
                        }

                        eprintln!(
                            "Recover: Corrupted data at file {} offset {}. Truncating.",
                            file_id, offset
                        );

                        if is_active {
                            // 只有 active file (Storage) 才有 set_len 能力
                            self.writer.get_ref().set_len(offset)?;
                            // 关键修复：如果复用了 active file 且发生了截断，必须更新 writer 的 offset
                            self.writer.set_offset(offset);
                        } else {
                            // 对于只读的归档文件，需要重新以写模式打开才能截断
                            let write_file = self.fs.open_file(file_path)?;
                            write_file.set_len(offset)?;
                        }

                        break; // 停止处理当前文件
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemFileSystem;
    use std::thread;
    use std::time::Duration;

    // 辅助函数：创建基于内存文件系统的 KVStore
    fn create_kv_store(data_dir: &str) -> (KVStore, Arc<MemFileSystem>, config::ConfigWatcher) {
        let watcher = config::ConfigWatcher::new("non_existent.conf").unwrap();
        let mut cfg = watcher.get();
        cfg.data_dir = data_dir.to_string();
        watcher.override_config(cfg);

        let fs = Arc::new(MemFileSystem::new());
        let kv = KVStore::new(watcher.clone(), fs.clone()).unwrap();
        (kv, fs, watcher)
    }

    #[test]
    fn test_basic_operations() {
        let (mut kv, _, _) = create_kv_store("test_basic");

        // 1. Set & Get
        kv.set("key1".to_string(), b"value1".to_vec()).unwrap();
        let entry = kv.get("key1".to_string()).unwrap().unwrap();
        assert_eq!(entry.value, b"value1");
        assert_eq!(entry.key, "key1");

        // 2. Update (Overwrite)
        kv.set("key1".to_string(), b"value2".to_vec()).unwrap();
        let entry = kv.get("key1".to_string()).unwrap().unwrap();
        assert_eq!(entry.value, b"value2");

        // 3. Remove
        kv.remove("key1").unwrap();
        assert!(kv.get("key1".to_string()).unwrap().is_none());

        // 4. Get non-existent
        assert!(kv.get("key_not_found".to_string()).unwrap().is_none());

        // 5. Empty Key/Value
        kv.set("".to_string(), vec![]).unwrap();
        let entry = kv.get("".to_string()).unwrap().unwrap();
        assert_eq!(entry.value.len(), 0);
    }

    #[test]
    fn test_ttl_expiration() {
        let (mut kv, _, _) = create_kv_store("test_ttl");

        // 设置 100ms 过期
        kv.set_with_ttl("key_ttl", b"val".to_vec(), Duration::from_millis(100))
            .unwrap();

        // 立即读取 (未过期)
        assert!(kv.get("key_ttl".to_string()).unwrap().is_some());

        // 等待过期
        thread::sleep(Duration::from_millis(200));

        // 再次读取 (已过期)
        // 注意：底层数据还在磁盘上，但 get 接口会过滤掉
        assert!(kv.get("key_ttl".to_string()).unwrap().is_none());
    }

    #[test]
    fn test_restore_normal() {
        let path = "test_restore_normal";
        let fs = Arc::new(MemFileSystem::new());
        let watcher = config::ConfigWatcher::new("non_existent.conf").unwrap();
        let mut cfg = watcher.get();
        cfg.data_dir = path.to_string();
        watcher.override_config(cfg);

        // 1. 写入数据并关闭
        {
            let mut kv = KVStore::new(watcher.clone(), fs.clone()).unwrap();
            kv.set("k1".to_string(), b"v1".to_vec()).unwrap();
            kv.set("k2".to_string(), b"v2".to_vec()).unwrap();
            kv.remove("k1").unwrap(); // k1 被删除
        }

        // 2. 重新打开并恢复
        let mut kv = KVStore::new(watcher, fs).unwrap();
        kv.restore().unwrap();

        // 3. 验证状态
        assert!(kv.get("k1".to_string()).unwrap().is_none()); // k1 应该是墓碑
        let entry = kv.get("k2".to_string()).unwrap().unwrap();
        assert_eq!(entry.value, b"v2");
    }

    #[test]
    fn test_restore_corrupted_data() {
        let path = "test_corrupt";
        let fs = Arc::new(MemFileSystem::new());
        let watcher = config::ConfigWatcher::new("non_existent.conf").unwrap();
        let mut cfg = watcher.get();
        cfg.data_dir = path.to_string();
        watcher.override_config(cfg);

        // 1. 写入数据
        {
            let mut kv = KVStore::new(watcher.clone(), fs.clone()).unwrap();
            kv.set("k1".to_string(), b"v1".to_vec()).unwrap(); // Valid
            kv.set("k2".to_string(), b"v2".to_vec()).unwrap(); // Will be corrupted
        }

        // 2. 手动破坏文件：修改 k2 的数据
        let file_path = Path::new(path).join("0001.bs");
        let mut file = fs.open_file(&file_path).unwrap();

        // 我们破坏文件末尾的数据，模拟 k2 损坏。
        // 注意：必须破坏 Header 部分，因为 restore 为了性能会跳过 Value 校验。
        file.seek(io::SeekFrom::End(-20)).unwrap();
        file.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();

        // 3. 恢复
        let mut kv = KVStore::new(watcher, fs).unwrap();
        kv.restore().unwrap();

        // 4. 验证
        // k1 应该还在 (因为它是先写入的，且未损坏)
        assert!(kv.get("k1".to_string()).unwrap().is_some());
        // k2 应该丢失 (因为数据损坏被截断)
        assert!(kv.get("k2".to_string()).unwrap().is_none());
    }

    #[test]
    fn test_restore_unexpected_eof() {
        let path = "test_eof";
        let fs = Arc::new(MemFileSystem::new());
        let watcher = config::ConfigWatcher::new("non_existent.conf").unwrap();
        let mut cfg = watcher.get();
        cfg.data_dir = path.to_string();
        watcher.override_config(cfg);

        {
            let mut kv = KVStore::new(watcher.clone(), fs.clone()).unwrap();
            kv.set("k1".to_string(), b"v1".to_vec()).unwrap();
            kv.set("k2".to_string(), b"v2".to_vec()).unwrap();
        }

        // 截断文件，使 k2 不完整
        let file_path = Path::new(path).join("0001.bs");
        let mut file = fs.open_file(&file_path).unwrap();
        let len = file.seek(io::SeekFrom::End(0)).unwrap();
        // 假设 k2 至少有 10 字节，截断 5 字节会导致 EOF 错误
        file.set_len(len - 5).unwrap();

        let mut kv = KVStore::new(watcher, fs).unwrap();
        kv.restore().unwrap();

        assert!(kv.get("k1".to_string()).unwrap().is_some());
        assert!(kv.get("k2".to_string()).unwrap().is_none());
    }

    #[test]
    fn test_rotation() {
        let path = "test_data_rotation";
        let watcher = config::ConfigWatcher::new("non_existent_rotation.conf").unwrap();
        let mut cfg = watcher.get();
        cfg.max_file_size = 50; // 极小的文件大小限制，强制轮转
        cfg.data_dir = path.to_string();
        watcher.override_config(cfg);

        let fs = Arc::new(MemFileSystem::new());
        let mut kv = KVStore::new(watcher.clone(), fs.clone()).unwrap();

        let val = vec![0u8; 10];

        // 写入多条数据触发轮转
        // Entry overhead approx 20-30 bytes. 2 entries might fill 50 bytes.
        kv.set("k1".to_string(), val.clone()).unwrap(); // 0001.bs
        kv.set("k2".to_string(), val.clone()).unwrap(); // 0001.bs (full?)
        kv.set("k3".to_string(), val.clone()).unwrap(); // Should trigger rotation to 0002.bs

        // 验证文件存在
        assert!(fs.exists(&Path::new(path).join("0001.bs")));
        // 继续写入
        kv.set("k4".to_string(), val.clone()).unwrap();
        assert!(fs.exists(&Path::new(path).join("0002.bs")));

        // 验证所有数据可读
        assert!(kv.get("k1".to_string()).unwrap().is_some());
        assert!(kv.get("k2".to_string()).unwrap().is_some());
        assert!(kv.get("k3".to_string()).unwrap().is_some());
        assert!(kv.get("k4".to_string()).unwrap().is_some());
    }
}
