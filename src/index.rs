use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

/// 索引器接口：负责管理 Key 到 LogIndex 的映射
pub trait Indexer: Send + Sync {
    fn put(&mut self, key: String, index: LogIndex);
    fn get(&self, key: &str) -> Option<LogIndex>;
    fn remove(&mut self, key: &str);
}

/// 自定义的 Key Arena，用于紧凑存储 Key 的字节数据。
#[derive(Default)]
struct KeyArena {
    data: Vec<u8>,
}

impl KeyArena {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// 将 Key 写入 Arena，返回 (offset, len)
    fn alloc(&mut self, key: &str) -> KeyRef {
        let offset = self.data.len() as u32;
        let bytes = key.as_bytes();
        self.data.extend_from_slice(bytes);
        KeyRef {
            offset,
            len: bytes.len() as u32,
        }
    }

    /// 根据引用获取 Key 的原始字节
    fn get(&self, key_ref: KeyRef) -> &[u8] {
        let start = key_ref.offset as usize;
        let end = start + key_ref.len as usize;
        &self.data[start..end]
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct KeyRef {
    offset: u32,
    len: u32,
}

/// 内存布局优化：HashMap<u64, Vec<(KeyRef, LogIndex)>>
pub struct HashIndexer {
    // 存储 hash(key) -> 冲突链表
    map: HashMap<u64, Vec<(KeyRef, LogIndex)>>,
    arena: KeyArena,
}

impl HashIndexer {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            arena: KeyArena::new(),
        }
    }

    fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl Indexer for HashIndexer {
    fn put(&mut self, key: String, index: LogIndex) {
        let hash = Self::hash_key(&key);
        let key_ref = self.arena.alloc(&key);

        let bucket = self.map.entry(hash).or_insert_with(Vec::new);

        // 检查是否存在相同的 Key (更新操作)
        for item in bucket.iter_mut() {
            if self.arena.get(item.0) == key.as_bytes() {
                item.1 = index;
                return;
            }
        }

        // 如果是新 Key，追加到链表
        bucket.push((key_ref, index));
    }

    fn get(&self, key: &str) -> Option<LogIndex> {
        let hash = Self::hash_key(key);
        if let Some(bucket) = self.map.get(&hash) {
            // 遍历冲突链表，比较实际的 Key 内容
            for (key_ref, index) in bucket {
                if self.arena.get(*key_ref) == key.as_bytes() {
                    return Some(*index);
                }
            }
        }
        None
    }

    fn remove(&mut self, key: &str) {
        let hash = Self::hash_key(key);
        if let Some(bucket) = self.map.get_mut(&hash) {
            // 从 Vec 中移除对应的 Key
            bucket.retain(|(key_ref, _)| self.arena.get(*key_ref) != key.as_bytes());

            // 如果桶空了，移除 Key 以节省 HashMap 空间
            if bucket.is_empty() {
                self.map.remove(&hash);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LogIndex {
    pub file_id: u32,
    pub val_len: u32,
    pub offset: u64,
}

impl LogIndex {
    pub fn new(file_id: u32, offset: u64, val_len: u32) -> Self {
        LogIndex {
            file_id,
            offset,
            val_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_size() {
        // 打印 LogIndex 的内存大小
        println!(
            "Size of LogIndex: {} bytes",
            std::mem::size_of::<LogIndex>()
        );
    }

    #[test]
    fn test_basic_put_get() {
        let mut indexer = HashIndexer::new();
        let idx = LogIndex::new(1, 100, 50);
        indexer.put("key1".to_string(), idx);

        assert_eq!(indexer.get("key1"), Some(idx));
        assert_eq!(indexer.get("key2"), None);
    }

    #[test]
    fn test_overwrite_update() {
        let mut indexer = HashIndexer::new();
        let idx1 = LogIndex::new(1, 100, 50);
        let idx2 = LogIndex::new(2, 200, 60);

        indexer.put("key1".to_string(), idx1);
        assert_eq!(indexer.get("key1"), Some(idx1));

        // Update
        indexer.put("key1".to_string(), idx2);
        assert_eq!(indexer.get("key1"), Some(idx2));
    }

    #[test]
    fn test_remove() {
        let mut indexer = HashIndexer::new();
        let idx = LogIndex::new(1, 100, 50);
        indexer.put("key1".to_string(), idx);

        indexer.remove("key1");
        assert_eq!(indexer.get("key1"), None);

        // Remove non-existent
        indexer.remove("key2"); // Should not panic
    }

    #[test]
    fn test_empty_key() {
        let mut indexer = HashIndexer::new();
        let idx = LogIndex::new(1, 0, 0);
        indexer.put("".to_string(), idx);
        assert_eq!(indexer.get(""), Some(idx));

        indexer.remove("");
        assert_eq!(indexer.get(""), None);
    }

    #[test]
    fn test_many_keys() {
        let mut indexer = HashIndexer::new();
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let idx = LogIndex::new(1, i, 10);
            indexer.put(key, idx);
        }

        for i in 0..1000 {
            let key = format!("key-{}", i);
            let expected = LogIndex::new(1, i, 10);
            assert_eq!(indexer.get(&key), Some(expected));
        }
    }
}
