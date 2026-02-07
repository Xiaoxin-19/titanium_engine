use hashbrown::HashTable;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

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

pub struct HashIndexer {
    table: HashTable<(KeyRef, LogIndex)>,
    arena: KeyArena,
    hasher_builder: RandomState,
}

impl HashIndexer {
    pub fn new() -> Self {
        Self {
            table: HashTable::new(),
            arena: KeyArena::new(),
            hasher_builder: RandomState::new(),
        }
    }

    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = self.hasher_builder.build_hasher();
        key.as_bytes().hash(&mut hasher);
        hasher.finish()
    }

    /// [Test Helper] 估算当前索引的内存占用 (Bytes)
    #[cfg(test)]
    pub fn memory_usage_approx(&self) -> usize {
        let table_mem = self.table.capacity() * std::mem::size_of::<(KeyRef, LogIndex)>();
        let arena_mem = self.arena.data.capacity();
        table_mem + arena_mem
    }
}

impl Indexer for HashIndexer {
    fn put(&mut self, key: String, index: LogIndex) {
        let hash = self.hash_key(&key);

        if let Some((_, val)) = self
            .table
            .find_mut(hash, |(kref, _)| self.arena.get(*kref) == key.as_bytes())
        {
            *val = index;
            return;
        }

        let key_ref = self.arena.alloc(&key);

        self.table
            .insert_unique(hash, (key_ref, index), |(kref, _)| {
                let bytes = self.arena.get(*kref);
                let mut hasher = self.hasher_builder.build_hasher();
                bytes.hash(&mut hasher);
                hasher.finish()
            });
    }

    fn get(&self, key: &str) -> Option<LogIndex> {
        let hash = self.hash_key(key);
        self.table
            .find(hash, |(kref, _)| self.arena.get(*kref) == key.as_bytes())
            .map(|(_, val)| *val)
    }

    fn remove(&mut self, key: &str) {
        let hash = self.hash_key(key);
        if let Ok(entry) = self
            .table
            .find_entry(hash, |(kref, _)| self.arena.get(*kref) == key.as_bytes())
        {
            entry.remove();
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
    use std::collections::HashMap;
    use std::time::Instant;

    #[test]
    fn test_index_entry_size() {
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

    #[test]
    #[ignore] // 默认忽略此测试，因为耗时较长。运行命令: cargo test --release -- --nocapture --ignored
    fn benchmark_memory_and_performance() {
        // 1. 定义基准对照组 (Standard HashMap)
        struct StandardHashMapIndexer {
            map: HashMap<String, LogIndex>,
        }

        impl StandardHashMapIndexer {
            fn new() -> Self {
                Self {
                    map: HashMap::new(),
                }
            }
        }

        impl Indexer for StandardHashMapIndexer {
            fn put(&mut self, key: String, index: LogIndex) {
                self.map.insert(key, index);
            }
            fn get(&self, key: &str) -> Option<LogIndex> {
                self.map.get(key).copied()
            }
            fn remove(&mut self, key: &str) {
                self.map.remove(key);
            }
        }

        // 2. 准备测试数据
        let n = 1_000_000; // 100万 keys
        println!("Generating {} keys...", n);
        let keys: Vec<String> = (0..n).map(|i| format!("key-{:010}", i)).collect();
        let idx = LogIndex::new(1, 0, 0);

        // 3. 测试 HashIndexer (Arena + RawTable)
        println!("\n--- Testing HashIndexer (Arena) ---");
        let mut arena_indexer = HashIndexer::new();

        let start = Instant::now();
        for key in &keys {
            arena_indexer.put(key.clone(), idx);
        }
        let put_duration_arena = start.elapsed();
        println!("Put time: {:?}", put_duration_arena);

        let start = Instant::now();
        for key in &keys {
            arena_indexer.get(key);
        }
        let get_duration_arena = start.elapsed();
        println!("Get time: {:?}", get_duration_arena);

        let mem_arena = arena_indexer.memory_usage_approx();
        println!(
            "Approx Memory: {:.2} MB",
            mem_arena as f64 / 1024.0 / 1024.0
        );

        // 4. 测试 Standard HashMap
        println!("\n--- Testing Standard HashMap ---");
        let mut std_indexer = StandardHashMapIndexer::new();

        let start = Instant::now();
        for key in &keys {
            std_indexer.put(key.clone(), idx);
        }
        let put_duration_std = start.elapsed();
        println!("Put time: {:?}", put_duration_std);

        let start = Instant::now();
        for key in &keys {
            std_indexer.get(key);
        }
        let get_duration_std = start.elapsed();
        println!("Get time: {:?}", get_duration_std);

        // 估算 HashMap 内存: Capacity * (SizeOf(String) + SizeOf(LogIndex)) + Heap(Strings)
        // String(24B) + LogIndex(16B) = 40B per entry (stack)
        let map_cap = std_indexer.map.capacity();
        let struct_overhead = std::mem::size_of::<String>() + std::mem::size_of::<LogIndex>();
        // 粗略估算：所有 Key 的堆内存占用 (String content)
        let heap_size: usize = keys.iter().map(|k| k.capacity()).sum();
        let mem_std = map_cap * struct_overhead + heap_size;
        println!("Approx Memory: {:.2} MB", mem_std as f64 / 1024.0 / 1024.0);

        // 5. 总结对比
        println!("\n--- Comparison ---");
        println!(
            "Memory Savings: {:.2}%",
            (1.0 - mem_arena as f64 / mem_std as f64) * 100.0
        );
        println!(
            "Put Speedup: {:.2}x",
            put_duration_std.as_secs_f64() / put_duration_arena.as_secs_f64()
        );
        println!(
            "Get Speedup: {:.2}x",
            get_duration_std.as_secs_f64() / get_duration_arena.as_secs_f64()
        );
    }
}
