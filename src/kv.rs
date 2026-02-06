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
    file: &'a File,
    offset: u64,
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
    map: HashMap<String, LogIndex>,
    writer: Writer<std::fs::File>,
    // 修改: 存储 (File, PathBuf) 元组，以便在 restore 时能准确找到文件路径
    file_map: HashMap<u32, (std::fs::File, PathBuf)>,
    data_path: PathBuf,
}

// 常量字符串必须是 &str 类型
const ACTIVE_FILE: &str = "active.bs";
const ACTIVE_FILE_ID: u32 = u32::MAX;

impl KVStore {
    pub fn new(data_path: &str) -> Result<Self, TitaniumError> {
        // 扫描目录，查找数据文件，如果没有目录，则创建对应目录，并初始化bs文件
        let root_path = Path::new(data_path);
        if !root_path.exists() {
            fs::create_dir_all(root_path)?;
        }

        // 使用 join 构建健壮的跨平台路径
        let active_path = root_path.join(ACTIVE_FILE);

        // 默认 active.bs 为写入文件， 为写入文件创建读写文件描述符
        let active_file = fs::File::options()
            .append(true) // TODO: use pre allocation to avoid metadata frequency update
            .write(true)
            .read(true)
            .create(true)
            .open(&active_path)?;
        // 其他静态 bs 文件格式为： 0001.bs 0002.bs， 为其他静态文件创建只读文件描述
        let mut paths: Vec<_> = fs::read_dir(root_path)?
            .filter_map(|res| res.ok()) // 忽略读取目录项时的错误
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file()
                    && path.extension().map_or(false, |ext| ext == "bs")
                    && path.file_name().map_or(false, |name| name != ACTIVE_FILE)
            })
            .collect();

        // 生成writer使用的文件句柄
        let writer_active_file = active_file.try_clone()?;
        let mut file_map: HashMap<u32, (File, PathBuf)> = HashMap::new();
        // 首先加入 active.bs 文件句柄
        file_map.insert(ACTIVE_FILE_ID, (active_file, active_path));

        for path in paths {
            // 解析文件名，如果是非法文件名（非UTF-8或非数字）则跳过
            let file_idx = match path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u32>().ok())
            {
                Some(idx) => idx,
                None => continue, // 跳过不符合命名规范的文件，而不是崩溃
            };

            // 防止文件名解析出的 ID 与 ACTIVE_FILE_ID 冲突
            if file_idx == ACTIVE_FILE_ID {
                continue;
            }

            let file = File::options().read(true).open(&path)?;
            file_map.insert(file_idx, (file, path));
        }

        let file_len = writer_active_file.metadata()?.len();
        let writer = Writer::new(writer_active_file, file_len);

        Ok(KVStore {
            map: HashMap::new(),
            writer,
            file_map: file_map,
            data_path: root_path.to_path_buf(),
        })
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) -> Result<(), TitaniumError> {
        // 1. write to log file
        let entry = LogEntry { key, value };
        let offset = self.writer.write_entry(&entry)?;
        // use config to decide when to sync
        if let config::WriteMod::Sync = config::ConfigWatcher::global().write_mod() {
            self.writer.sync()?;
        }
        // 2. update hashmap
        self.map.insert(
            entry.key,
            LogIndex::new(ACTIVE_FILE_ID, offset, entry.value.len() as u32),
        );
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
        let (file, _) = &self.file_map[&log_index.file_id];
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
            let (max_key, max_val) = config::ConfigWatcher::global().max_sizes();
            decoder.set_limits(max_key, max_val);
            decoder.decode_from(&mut reader)
        })?;
        Ok(Some(entry))
    }

    // 程序重启后，恢复 KVStore 状态
    pub fn restore(&mut self) -> Result<(), TitaniumError> {
        let (max_key, max_val) = config::ConfigWatcher::global().max_sizes();
        let mut decoder = Decoder::new(max_key, max_val);

        // 1. 获取所有 file_id 并排序，确保按时间顺序恢复数据 (旧 -> 新)
        let mut file_ids: Vec<u32> = self.file_map.keys().cloned().collect();
        file_ids.sort();

        for file_id in &file_ids {
            let (file, file_path) = &self.file_map[file_id];
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
                        if *file_id == ACTIVE_FILE_ID {
                            file.set_len(offset)?;
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

                        if *file_id == ACTIVE_FILE_ID {
                            file.set_len(offset)?;
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
        // 初始化 ConfigWatcher，否则 KVStore::set/get 会 panic
        // 即使文件不存在也会加载默认配置，这对测试是安全的
        let _ = config::ConfigWatcher::init("titanium.conf");
        // 确保测试环境干净
        if Path::new(path).exists() {
            fs::remove_dir_all(path).unwrap();
        }

        let mut kv = KVStore::new(path).unwrap();

        // 测试写入和读取 (Set & Get)
        let key1 = "key1".to_string();
        let val1 = b"val1".to_vec();
        kv.set(key1.clone(), val1.clone()).unwrap();

        let res1 = kv.get(key1).unwrap();
        assert!(res1.is_some());
        assert_eq!(res1.unwrap().value, val1);

        // 测试读取不存在的 Key (Get Not Found)
        let res2 = kv.get("key2".to_string()).unwrap();
        assert!(res2.is_none());

        // 清理测试数据
        fs::remove_dir_all(path).unwrap();
    }
}
