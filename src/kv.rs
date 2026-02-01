use crate::error::TitaniumError;
use crate::index::LogIndex;
use crate::log_entry::{Decoder, LogEntry};
use crate::writer::Writer;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

struct KVStore {
    map: HashMap<String, LogIndex>,
    writer: Writer<std::fs::File>,
    file_list: Vec<std::fs::File>,
    decoder: Decoder,
}

// 常量字符串必须是 &str 类型
const ACTIVE_FILE: &str = "active.bs";

impl KVStore {
    pub fn new(data_path: String) -> Result<Self, TitaniumError> {
        // 扫描目录，查找数据文件，如果没有目录，则创建对应目录，并初始化bs文件
        // 使用 Path::new 检查是否存在，避免所有权被转移
        if !Path::new(&data_path).exists() {
            fs::create_dir_all(&data_path)?;
        }

        // 使用 format! 构建完整路径
        let active_path = format!("{}/{}", data_path, ACTIVE_FILE);
        // 默认 active.bs 为写入文件， 为写入文件创建读写文件描述符
        let active_file = fs::File::options()
            .append(true)
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

        // 按文件名排序，确保按顺序加载 (例如 0001.bs, 0002.bs)
        paths.sort();
        for path in paths {
            readers.push(File::open(path)?);
        }

        // 先克隆一份文件句柄给 Writer，保留原句柄给 file_list
        // 注意：这里需要 clone 文件句柄，因为 writer 会拿走一个，file_list 也要存一个
        let writer = Writer::new(active_file.try_clone()?);
        readers.push(active_file);

        Ok(KVStore {
            map: HashMap::new(),
            writer,
            file_list: readers,
            decoder: Decoder::new(),
        })
    }

    pub fn set(&mut self, key: String, value: Vec<u8>) -> Result<(), TitaniumError> {
        // 1. write to log file
        let entry = LogEntry { key, value };
        let offset = self.writer.write_entry(&entry)?;
        self.writer.sync()?;
        // 2. update hashmap
        self.map.insert(
            entry.key,
            LogIndex::new(0, offset, entry.value.len() as u32),
        );
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<LogEntry>, TitaniumError> {
        let log_index = match self.map.get(&key) {
            Some(index) => index,
            None => return Ok(None),
        };

        let mut file = &self.file_list[log_index.file_id as usize];
        file.seek(SeekFrom::Start(log_index.offset))?;

        let entry = self.decoder.decode_from(&mut file)?;
        Ok(Some(entry))
    }

    /// 流式读取接口：返回 (Value长度, Reader)
    /// 调用者可以使用 reader 按需读取 Value，避免大 Value 占用过多内存
    pub fn get_reader(
        &mut self,
        key: &str,
    ) -> Result<Option<(u64, std::io::Take<&File>)>, TitaniumError> {
        let log_index = match self.map.get(key) {
            Some(index) => index,
            None => return Ok(None),
        };

        let mut file = &self.file_list[log_index.file_id as usize];
        file.seek(SeekFrom::Start(log_index.offset))?;

        // 1. 读取头部和 Key
        let (v_len, _key_in_file) = self.decoder.decode_header_and_key(&mut file)?;

        // 2. 返回限制长度的 Reader，直接指向 Value 数据
        Ok(Some((v_len as u64, file.take(v_len as u64))))
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

        let mut kv = KVStore::new(path.to_string()).unwrap();

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

        // 测试流式读取 (Get Reader)
        if let Some((len, mut reader)) = kv.get_reader("key1").unwrap() {
            assert_eq!(len, val1.len() as u64);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, val1);
        } else {
            panic!("get_reader failed to find key");
        }

        // 清理测试数据
        fs::remove_dir_all(path).unwrap();
    }
}
