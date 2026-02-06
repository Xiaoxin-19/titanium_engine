use crate::config;
use crate::error::TitaniumError;
use crate::index::LogIndex;
use crate::log_entry::{Decoder, LogEntry};
use crate::writer::Writer;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::path::PathBuf;

/// 一个辅助结构体，用于将 read_at 适配为 Read trait
/// 这样 Decoder 就可以在不改变文件游标的情况下读取数据
pub struct FileAtReader<'a> {
    pub file: &'a File,
    pub offset: u64,
}

impl<'a> Read for FileAtReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        let n = {
            use std::os::unix::fs::FileExt;
            self.file.read_at(buf, self.offset)?
        };
        #[cfg(windows)]
        let n = {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(buf, self.offset)?
        };

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
                let len = self.file.metadata()?.len();
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
    pub(crate) map: HashMap<String, LogIndex>,
    writer: Writer<std::fs::File>,
    file_map: HashMap<u32, (std::fs::File, PathBuf)>,
    data_path: PathBuf,
    active_file_id: u32,
    pub(crate) config: config::ConfigWatcher,
}

impl KVStore {
    pub fn new(config: config::ConfigWatcher) -> Result<Self, TitaniumError> {
        // 扫描目录，查找数据文件，如果没有目录，则创建对应目录，并初始化bs文件
        let data_path = config.get().data_dir;
        let root_path = Path::new(&data_path);
        if !root_path.exists() {
            fs::create_dir_all(root_path)?;
        }

        // 1. 扫描所有 .bs 文件并提取 ID
        let mut file_ids: Vec<u32> = fs::read_dir(root_path)?
            .filter_map(|res| res.ok())
            .map(|entry| entry.path())
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
            let len = fs::metadata(&last_path).map(|m| m.len()).unwrap_or(0);

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
        let active_file = fs::File::options()
            .append(true)
            .create(true)
            .read(true) // 为了 restore 需要 read
            .write(true)
            .open(&active_path)?;

        let file_len = active_file.metadata()?.len();
        let writer = Writer::new(active_file, file_len);

        // 4. 加载旧文件到 file_map (只读)
        let mut file_map = HashMap::new();
        for &id in &file_ids {
            let path = root_path.join(format!("{:04}.bs", id));
            let file = File::options().read(true).open(&path)?;
            file_map.insert(id, (file, path));
        }

        Ok(KVStore {
            map: HashMap::new(),
            writer,
            file_map: file_map,
            data_path: root_path.to_path_buf(),
            active_file_id,
            config,
        })
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) -> Result<(), TitaniumError> {
        // 0. 检查是否需要轮转文件
        if self.writer.current_offset() >= self.config.max_file_size() as u64 {
            self.rotate()?;
        }

        // 1. write to log file
        let entry = LogEntry { key, value };
        let offset = self.writer.write_entry(&entry)?;
        // use config to decide when to sync
        match self.config.write_mod() {
            config::WriteMod::Sync => self.writer.sync()?,
            config::WriteMod::Buffer => self.writer.flush_to_os()?,
        }
        // 2. update hashmap
        self.map.insert(
            entry.key,
            LogIndex::new(self.active_file_id, offset, entry.value.len() as u32),
        );
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
        let old_file = File::options().read(true).open(&old_path)?;
        self.file_map.insert(old_id, (old_file, old_path));

        // 3. 更新 active_file_id 并创建新文件
        self.active_file_id += 1;
        let new_path = self
            .data_path
            .join(format!("{:04}.bs", self.active_file_id));

        let new_file = fs::File::options()
            .append(true)
            .create(true)
            .read(true)
            .write(true)
            .open(&new_path)?;

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
        let log_index = match self.map.get(&key) {
            Some(index) => index,
            None => return Ok(None),
        };

        // 区分读取的是归档文件还是当前的活跃文件
        // 如果是活跃文件，我们需要从 writer 中获取（或者如果 writer 的文件句柄支持 read，也可以直接用）
        // 但为了简化，我们在 new/rotate 时确保 active_file 也是可读的，
        // 并且我们不把 active_file 放入 file_map，所以这里需要特殊处理
        let file = if log_index.file_id == self.active_file_id {
            self.writer.get_ref()
        } else {
            &self.file_map[&log_index.file_id].0
        };

        // 使用 FileAtReader 替代 seek，实现无锁并发读取
        let mut reader = FileAtReader {
            file,
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
            decoder.decode_from(&mut reader)
        })?;
        Ok(Some(entry))
    }

    // 程序重启后，恢复 KVStore 状态
    pub fn restore(&mut self) -> Result<(), TitaniumError> {
        let (max_key, max_val) = self.config.max_sizes();
        let mut decoder = Decoder::new(max_key, max_val);

        // 1. 获取所有 file_id 并排序，确保按时间顺序恢复数据 (旧 -> 新)
        let mut file_ids: Vec<u32> = self.file_map.keys().cloned().collect();
        file_ids.sort();

        // 别忘了加上当前的 active_file_id，因为它不在 file_map 中
        file_ids.push(self.active_file_id);

        for file_id in &file_ids {
            let is_active = *file_id == self.active_file_id;

            let active_path_buf; // 声明变量以延长生命周期
            let (file, file_path) = if is_active {
                active_path_buf = self.data_path.join(format!("{:04}.bs", file_id));
                (self.writer.get_ref(), &active_path_buf)
            } else {
                let (f, p) = &self.file_map[file_id];
                (f, p)
            };

            let mut reader = std::io::BufReader::new(FileAtReader {
                file: file,
                offset: 0,
            });
            loop {
                let offset = reader.stream_position()?; // 记录起始位置
                match decoder.decode_header_and_key(&mut reader) {
                    // 1. 完美读取
                    Ok(Some(header)) => {
                        self.map.insert(
                            header.key,
                            LogIndex::new(*file_id as u32, offset, header.val_len),
                        );
                    }

                    // 2. 完美结束 (EOF)
                    Ok(None) => break,

                    // 3. 数据损坏 (CRC 错 或 意外EOF) -> 执行截断
                    Err(TitaniumError::CrcMismatch { .. }) => {
                        eprintln!(
                            "Recover: Corrupted data at file {} offset {}. Truncating.",
                            file_id, offset
                        );

                        // 如果是 active file (u32::MAX)，它已经是可写的，直接截断
                        if is_active {
                            file.set_len(offset)?;
                            // 关键修复：如果复用了 active file 且发生了截断，必须更新 writer 的 offset
                            self.writer.set_offset(offset);
                        } else {
                            // 对于只读的归档文件，需要重新以写模式打开才能截断
                            let write_file = fs::OpenOptions::new().write(true).open(file_path)?;
                            write_file.set_len(offset)?;
                        }

                        break; // 停止处理当前文件
                    }
                    Err(TitaniumError::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        eprintln!(
                            "Recover: Corrupted data at file {} offset {}. Truncating.",
                            file_id, offset
                        );

                        if is_active {
                            file.set_len(offset)?;
                            self.writer.set_offset(offset);
                        } else {
                            let write_file = fs::OpenOptions::new().write(true).open(file_path)?;
                            write_file.set_len(offset)?;
                        }

                        break; // 停止处理当前文件
                    }

                    // 4. 其他严重 I/O 错误 (如磁盘坏道、权限不足) -> 必须抛出
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_kv_store_integration() {
        let path = "test_data_kv_integration";

        // 传入一个不存在的文件路径即可获取默认配置，避免读取真实的 titanium.conf 干扰测试
        let watcher = config::ConfigWatcher::new("non_existent_test.conf").unwrap();

        let mut cfg = watcher.get();
        cfg.data_dir = path.to_string();
        watcher.override_config(cfg);

        // 确保测试环境干净
        if Path::new(path).exists() {
            fs::remove_dir_all(path).unwrap();
        }
        let mut kv = KVStore::new(watcher).unwrap();

        // 表驱动
        let test_cases = vec![
            ("key1", b"value1".to_vec()),
            ("key2", b"value2".to_vec()),
            ("key_empty", b"".to_vec()),
            ("key_long", vec![b'a'; 100]),
        ];

        // 1. 批量写入
        for (k, v) in &test_cases {
            kv.set(k.to_string(), v.clone()).unwrap();
        }
        // 2. 批量验证
        for (k, v) in &test_cases {
            let res = kv.get(k.to_string()).unwrap();
            assert_eq!(res.unwrap().value, *v, "Value mismatch for key: {}", k);
        }

        // 清理测试数据
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_rotation() {
        let path = "test_data_rotation";
        if Path::new(path).exists() {
            fs::remove_dir_all(path).unwrap();
        }

        // 1. 创建独立的配置实例，避免污染全局状态影响其他测试
        let watcher = config::ConfigWatcher::new("non_existent_rotation.conf").unwrap();

        // 2. 动态修改配置：将最大文件大小设置为 50 字节
        let mut cfg = watcher.get();
        cfg.max_file_size = 50;
        cfg.data_dir = path.to_string(); // 同时也设置 data_dir
        watcher.override_config(cfg);

        let mut kv = KVStore::new(watcher.clone()).unwrap();

        // 3. 写入数据
        // Entry 结构: Header(CRC 4 + KLen 1 + VLen 1) + Key + Value
        // Key="k", Value="v" (10 bytes) -> 大约 4+1+1+1+10 = 17 bytes
        let val = vec![0u8; 10];

        // 写入第 1 条 (Offset ~17)
        kv.set("k1".to_string(), val.clone()).unwrap();
        // 写入第 2 条 (Offset ~34)
        kv.set("k2".to_string(), val.clone()).unwrap();
        // 写入第 3 条 (Offset ~51) -> 此时文件大小超过 50，但轮转是在 *下一次* 写入前触发
        kv.set("k3".to_string(), val.clone()).unwrap();

        // 4. 验证：此时应该还是只有 0001.bs
        assert!(Path::new(path).join("0001.bs").exists());
        assert!(!Path::new(path).join("0002.bs").exists());

        // 写入第 4 条 -> 触发轮转，生成 0002.bs，写入新数据
        kv.set("k4".to_string(), val.clone()).unwrap();

        // 5. 验证：0002.bs 应该存在
        assert!(Path::new(path).join("0002.bs").exists());

        // 验证数据可读性
        assert_eq!(kv.get("k1".to_string()).unwrap().unwrap().value, val);
        assert_eq!(kv.get("k4".to_string()).unwrap().unwrap().value, val);

        fs::remove_dir_all(path).unwrap();
    }
}
