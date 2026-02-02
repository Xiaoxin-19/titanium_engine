use crate::error::TitaniumError;
use crate::index::LogIndex;
use crate::log_entry::{self, Decoder, LogEntry};
use crate::writer::Writer;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

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
    file_list: Vec<std::fs::File>,
}

// 常量字符串必须是 &str 类型
const ACTIVE_FILE: &str = "active.bs";

impl KVStore {
    pub fn new(data_path: &str) -> Result<Self, TitaniumError> {
        // 扫描目录，查找数据文件，如果没有目录，则创建对应目录，并初始化bs文件
        // 使用 Path::new 检查是否存在，避免所有权被转移
        if !Path::new(&data_path).exists() {
            fs::create_dir_all(&data_path)?;
        }

        // 使用 format! 构建完整路径
        let active_path = format!("{}/{}", data_path, ACTIVE_FILE);
        // 默认 active.bs 为写入文件， 为写入文件创建读写文件描述符
        let active_file = fs::File::options()
            .append(true) // TODO: use pre allocation to avoid metadata frequency update
            .write(true)
            .read(true)
            .create(true)
            .open(active_path)?;
        // 其他静态 bs 文件格式为： 0001.bs 0002.bs， 为其他静态文件创建只读文件描述
        let mut readers = Vec::new();
        let mut paths: Vec<_> = fs::read_dir(&data_path)?
            .filter_map(|res| res.ok()) // 忽略读取目录项时的错误
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file()
                    && path.extension().map_or(false, |ext| ext == "bs")
                    && path.file_name().map_or(false, |name| name != ACTIVE_FILE)
            })
            .collect();
        // 首先加入 active.bs 文件句柄
        readers.push(active_file);
        // 生成writer使用的文件句柄
        let reader_active_file = readers[0].try_clone()?;
        // 按文件名排序，确保按顺序加载 (例如 0001.bs, 0002.bs)
        paths.sort();
        for path in paths {
            readers.push(File::open(path)?);
        }

        // 先克隆一份文件句柄给 Writer，保留原句柄给 file_list
        // 注意：这里需要 clone 文件句柄，因为 writer 会拿走一个，file_list 也要存一个
        let file_len = reader_active_file.metadata()?.len();
        let writer = Writer::new(reader_active_file, file_len);

        Ok(KVStore {
            map: HashMap::new(),
            writer,
            file_list: readers,
        })
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) -> Result<(), TitaniumError> {
        // 1. write to log file
        let entry = LogEntry { key, value };
        let offset = self.writer.write_entry(&entry)?;
        // TODO : use config to decide when to sync, now always sync after write
        self.writer.sync()?;
        // 2. update hashmap
        self.map.insert(
            entry.key,
            LogIndex::new(0, offset, entry.value.len() as u32),
        );
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<LogEntry>, TitaniumError> {
        let log_index = match self.map.get(&key) {
            Some(index) => index,
            None => return Ok(None),
        };
        let file = &self.file_list[log_index.file_id as usize];
        // 使用 FileAtReader 替代 seek，实现无锁并发读取
        let mut reader = FileAtReader {
            file,
            offset: log_index.offset,
        };

        // 使用局部的 decoder，避免借用 self.decoder 需要的可变引用
        let mut decoder = Decoder::new();
        let entry = decoder.decode_from(&mut reader)?;
        Ok(Some(entry))
    }

    // 程序重启后，恢复 KVStore 状态
    pub fn restore(&mut self) -> Result<(), TitaniumError> {
        let mut decoder = Decoder::new();
        for (file_id, file) in self.file_list.iter().enumerate() {
            let mut reader = std::io::BufReader::new(FileAtReader {
                file: file,
                offset: 0,
            });
            let mut count = 0;
            loop {
                println!("{count}");
                count += 1;
                let offset = reader.stream_position()?;
                // 传入 &mut reader 修复类型错误
                match decoder.decode_header_and_key(&mut reader) {
                    Ok(Some(header)) => {
                        self.map.insert(
                            header.key,
                            LogIndex::new(file_id as u32, offset, header.val_len),
                        );
                        // 由于decode_header_and_key中读取了值，计算CRC，所以不用Seek
                    }
                    Err(err) => match err {
                        TitaniumError::CrcMismatch { expected } => {
                            eprintln!(
                                "File {} corrupted at offset {}: CRC mismatch (expected {})",
                                file_id, offset, expected
                            );
                            // // 一旦 CRC 错位，后续数据很难恢复，通常选择截断或报错退出
                            // return Err(err);
                        }
                        TitaniumError::Io(e) => match e.kind() {
                            io::ErrorKind::UnexpectedEof => {
                                println!(
                                    "File {} ended unexpectedly (possibly truncated)",
                                    file_id
                                );
                                file.set_len(offset)?;
                                break; // 遇到 EOF 错误通常意味着文件结束，跳出循环
                            }
                            _ => (),
                        },
                        _ => (),
                    },
                    Ok(None) => break, // 文件结束
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
    use std::io::Read;
    use std::path::Path;

    #[test]
    fn test_kv_store_integration() {
        let path = "test_data_kv_integration";
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
