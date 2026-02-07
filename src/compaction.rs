use std::{
    collections::HashMap,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::Path,
    path::PathBuf,
};

use crate::{
    config,
    error::TitaniumError,
    index::LogIndex,
    kv::{self, FileAtReader},
    log_entry::Decoder,
    storage::{FileSystem, RandomAccessFile, Storage},
    utils::{decode_varint, encode_varint},
    writer::Writer,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

struct Compacter;

// 通过对比索引offset来判断是否时最新的版本
// 采用流式模式防止双倍内存占用问题，生成一个hint文件用户快速构建hashmap
// 只有在最后替换的时候占用写锁，hashmap采用读写锁保护
impl Compacter {
    pub fn compact(
        file_map: &HashMap<u32, (Box<dyn RandomAccessFile>, PathBuf)>,
        kv_store: &mut kv::KVStore,
    ) -> Result<(), TitaniumError> {
        Ok(())
    }
}
