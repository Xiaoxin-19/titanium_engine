use crate::{
    error::TitaniumError,
    utils::{decode_varint, encode_varint},
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EntryType(pub u8);

impl EntryType {
    /// Bit 0: 墓碑标记 (Tombstone)
    /// 表示该条目是一个删除操作。
    const TOMBSTONE: u8 = 1 << 0;

    /// Bit 1: 预留 (Reserved)
    /// 暂未使用。

    /// Bit 2: 是否包含 TTL (Time To Live)
    /// 表示 Header 中是否包含过期时间戳 (expire_at)。
    /// 如果该位未设置，则表示没有过期时间，Header 中也不会写入 expire_at 字段，以节省空间。
    const TTL: u8 = 1 << 2;

    /// Bit 3-7: 预留 (Reserved)

    const NORMAL: Self = Self(0);
    const DELETE: Self = Self(Self::TOMBSTONE);

    pub fn new(val: u8) -> Self {
        Self(val)
    }

    pub fn is_tombstone(&self) -> bool {
        self.0 & Self::TOMBSTONE != 0
    }

    pub fn has_ttl(&self) -> bool {
        self.0 & Self::TTL != 0
    }

    /// 在 Encode 阶段标记 TTL 位 (设置 Bit 2)
    pub fn mark_ttl(&mut self) {
        self.0 |= Self::TTL;
    }
}

// log entry
#[derive(Debug, PartialEq)]
pub struct LogEntry {
    entry_type: EntryType,
    pub key: String,
    pub value: Vec<u8>,
    pub sequence_number: u64,
    pub created_at: u64,
    expire_at: Option<u64>,
}

/// 用于构建 LogEntry 的 Builder
pub struct LogEntryBuilder {
    entry: LogEntry,
}

impl LogEntryBuilder {
    /// 设置过期时间 (TTL)
    /// 同步设置 EntryType 的 TTL 标记位
    pub fn with_ttl(mut self, expire_at: u64) -> Self {
        self.entry.expire_at = Some(expire_at);
        self.entry.entry_type.mark_ttl();
        self
    }

    /// 构建最终的 LogEntry
    pub fn build(self) -> LogEntry {
        self.entry
    }
}

#[derive(Debug, PartialEq)]
pub struct LogHeader {
    entry_type: EntryType,
    pub key: String,
    pub val_len: u32,
    pub created_at: u64,
    pub expire_at: Option<u64>,
    pub sequence_number: u64,
}

impl LogHeader {
    pub fn is_tombstone(&self) -> bool {
        self.entry_type.is_tombstone()
    }
}

// zero allocation decoder
pub struct Decoder {
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    max_key_size: usize,
    max_val_size: usize,
}

impl LogEntry {
    /// 工厂方法：创建普通日志条目
    /// 返回一个 Builder，用于进一步配置可选参数 (如 TTL)
    pub fn new(key: String, value: Vec<u8>, sequence_number: u64) -> LogEntryBuilder {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        LogEntryBuilder {
            entry: Self {
                entry_type: EntryType::NORMAL,
                key,
                value,
                sequence_number,
                created_at,
                expire_at: None,
            },
        }
    }

    /// 工厂方法：创建删除标记 (墓碑)
    pub fn new_tombstone(key: String, sequence_number: u64) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            entry_type: EntryType::DELETE,
            key,
            value: Vec::new(),
            sequence_number,
            created_at,
            expire_at: None,
        }
    }

    pub fn expire_at(&self) -> Option<u64> {
        self.expire_at
    }

    fn encode_header<W: Write>(&self, writer: &mut W) -> Result<u64, TitaniumError> {
        // 1. 准备栈上缓冲区 (Stack Allocation)
        // 最大元数据长度：Type(1) + CreatedAt(10) + SeqNo(10) + KLen(5) + VLen(5) + ExpireAt(10) = 41 bytes
        // 使用 [u8; 64] 足够容纳，且完全在栈上分配，无堆内存开销。
        let mut buf = [0u8; 64];
        let mut offset = 0;

        // 预留 Type 的位置 (稍后回填，因为 mark_ttl 可能会修改它)
        let type_pos = offset;
        offset += 1;

        // 编码各个字段到同一个缓冲区
        offset += encode_varint(self.created_at, &mut buf[offset..]);
        offset += encode_varint(self.sequence_number, &mut buf[offset..]);

        let k_len = self.key.len() as u32;
        offset += encode_varint(k_len, &mut buf[offset..]);
        let v_len = self.value.len() as u32;
        offset += encode_varint(v_len, &mut buf[offset..]);

        if let Some(ts) = self.expire_at {
            offset += encode_varint(ts, &mut buf[offset..]);
        }

        // 回填 Type
        buf[type_pos] = self.entry_type.0;

        // 2. 计算 Header CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[..offset]);
        hasher.update(self.key.as_bytes());
        let header_crc = hasher.finalize();

        // 3. 写入 Header
        writer.write_all(&header_crc.to_le_bytes())?;
        writer.write_all(&buf[..offset])?;
        writer.write_all(self.key.as_bytes())?;

        // 返回 Header 总长度
        Ok((4 + offset + self.key.len()) as u64)
    }

    pub fn encode_to<W: Write>(&self, writer: &mut W) -> Result<u64, TitaniumError> {
        // 1. 写入 Header
        let header_len = self.encode_header(writer)?;

        // 2. 计算 Body CRC
        let mut body_hasher = crc32fast::Hasher::new();
        body_hasher.update(&self.value);
        let body_crc = body_hasher.finalize();

        // 3. 写入 Body CRC 和 Value
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&self.value)?;

        Ok(header_len + 4 + self.value.len() as u64)
    }
}

impl Decoder {
    pub fn new(max_key_size: usize, max_val_size: usize) -> Self {
        Decoder {
            key_buf: Vec::new(),
            value_buf: Vec::new(),
            max_key_size,
            max_val_size,
        }
    }

    pub fn set_limits(&mut self, max_key_size: usize, max_val_size: usize) {
        self.max_key_size = max_key_size;
        self.max_val_size = max_val_size;
    }

    pub fn decode_from<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<LogEntry>, TitaniumError> {
        // 1. 复用 decode_header_and_key 读取头部和 Key
        // decode_header_and_key 会处理 Header CRC 校验和 Key 的读取
        let header = match self.decode_header_and_key(reader)? {
            Some(h) => h,
            None => return Ok(None),
        };

        // 2. Read Body CRC
        let body_crc = reader.read_u32::<LittleEndian>()?;

        // 3. Read Value
        self.value_buf.resize(header.val_len as usize, 0);
        reader.read_exact(&mut self.value_buf)?;

        // 4. Verify Body CRC
        let mut body_hasher = crc32fast::Hasher::new();
        body_hasher.update(&self.value_buf);

        if body_hasher.finalize() != body_crc {
            return Err(TitaniumError::CrcMismatch { expected: body_crc });
        }

        Ok(Some(LogEntry {
            entry_type: header.entry_type,
            created_at: header.created_at,
            sequence_number: header.sequence_number,
            key: header.key,
            value: self.value_buf.clone(), // must clone to return owned data
            expire_at: header.expire_at,
        }))
    }

    /// 解码头部、Key 和 Value 以进行 CRC 校验，但仅返回头部信息 (CRC, Key, Value长度)。
    /// 优化：此方法现在只读取 Key 并验证 Header CRC，**不读取** Value 和 BodyCRC。
    /// 调用者需要负责跳过 BodyCRC 和 Value (seek 4 + val_len)。
    pub fn decode_header_and_key<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<LogHeader>, TitaniumError> {
        let mut header_crc_buf = [0u8; 4];
        // 尝试读取第一个字节来判断 EOF
        match reader.read(&mut header_crc_buf[0..1]) {
            Ok(0) => return Ok(None), // Clean EOF
            Ok(1) => reader.read_exact(&mut header_crc_buf[1..])?,
            Ok(_) => unreachable!(),
            Err(e) => return Err(TitaniumError::Io(e)),
        }
        let header_crc = u32::from_le_bytes(header_crc_buf);
        let type_byte = reader.read_u8()?;
        let entry_type = EntryType::new(type_byte);
        let created_at: u64 = decode_varint(reader)?;
        let sequence_number: u64 = decode_varint(reader)?;
        let k_len: u32 = decode_varint(reader)?;
        let v_len: u32 = decode_varint(reader)?;

        // 进行大小检查
        if k_len > self.max_key_size as u32 || v_len > self.max_val_size as u32 {
            return Err(TitaniumError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Entry too large",
            )));
        }

        // Read ExpireAt if TTL bit is set
        let mut expire_at: Option<u64> = None;
        if entry_type.has_ttl() {
            expire_at = Some(decode_varint::<_, u64>(reader)?);
        }

        // 1. 计算 Header CRC (Metadata 部分)
        let mut header_hasher = crc32fast::Hasher::new();
        header_hasher.update(&[type_byte]);

        let mut temp_buf = [0u8; 10];
        let n = encode_varint(created_at, &mut temp_buf);
        header_hasher.update(&temp_buf[..n]);
        let n = encode_varint(sequence_number, &mut temp_buf);
        header_hasher.update(&temp_buf[..n]);

        let mut len_buf = [0u8; 5];
        let n = encode_varint(k_len, &mut len_buf);
        header_hasher.update(&len_buf[..n]);
        let n = encode_varint(v_len, &mut len_buf);
        header_hasher.update(&len_buf[..n]);
        if let Some(ts) = expire_at {
            let n = encode_varint(ts, &mut temp_buf);
            header_hasher.update(&temp_buf[..n]);
        }

        // 2. 读取 Key 并更新 CRC
        self.key_buf.resize(k_len as usize, 0);
        reader.read_exact(&mut self.key_buf)?;
        header_hasher.update(&self.key_buf);

        if header_hasher.finalize() != header_crc {
            return Err(TitaniumError::CrcMismatch {
                expected: header_crc,
            });
        }

        // 3. 停止读取！
        // 我们不读取 BodyCRC 和 Value，也不进行 Body CRC 校验。
        let key = String::from_utf8(self.key_buf.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Key is not valid UTF-8"))?;

        Ok(Some(LogHeader {
            entry_type,
            created_at,
            sequence_number,
            key,
            val_len: v_len,
            expire_at,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varint() {
        let mut buf = [0u8; 5];

        // Test small number
        let n = encode_varint(50u32, &mut buf);
        assert_eq!(n, 1);
        assert_eq!(buf[0], 50);
        let mut cursor = Cursor::new(&buf[..n]);
        assert_eq!(decode_varint::<_, u32>(&mut cursor).unwrap(), 50);

        // Test number > 127 (300 = 0x12C)
        // Binary: 0000 0001 0010 1100
        // 7 bits groups: 0000010 (2) | 0101100 (44)
        // Encoded: [44 | 0x80, 2] = [0xAC, 0x02]
        let n = encode_varint(300u32, &mut buf);
        assert_eq!(n, 2);
        assert_eq!(buf[0], 0xAC);
        assert_eq!(buf[1], 0x02);
        let mut cursor = Cursor::new(&buf[..n]);
        assert_eq!(decode_varint::<_, u32>(&mut cursor).unwrap(), 300);
    }

    #[test]
    fn test_log_entry_encode_decode() {
        let key = "test_key";
        let value = b"test_value";
        let mut buf = Vec::new();

        let entry = LogEntry::new(key.to_string(), value.to_vec(), 1).build();

        let written = entry.encode_to(&mut buf).unwrap();
        assert!(written > 0);

        // Decode
        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new(1024, 1024 * 1024);
        let entry = decoder.decode_from(&mut cursor).unwrap().unwrap();

        assert_eq!(entry.entry_type, EntryType::NORMAL);
        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
    }

    #[test]
    fn test_crc_mismatch() {
        let key = "key";
        let value = b"val";
        let mut buf = Vec::new();
        let entry = LogEntry::new(key.to_string(), value.to_vec(), 1).build();
        entry.encode_to(&mut buf).unwrap();

        // Corrupt the data (flip bits in the last byte of value)
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new(1024, 1024 * 1024);
        let err = decoder.decode_from(&mut cursor).unwrap_err();

        match err {
            TitaniumError::CrcMismatch { .. } => (),
            _ => panic!("Expected CrcMismatch error, got {:?}", err),
        }
    }

    #[test]
    fn test_decode_header_and_key() -> Result<(), TitaniumError> {
        let key = "long_key";
        let value = vec![1u8; 100];
        let mut buf = Vec::new();
        let entry = LogEntry::new(key.to_string(), value.clone(), 1).build();
        entry.encode_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new(1024, 1024 * 1024);
        let log_header;
        match decoder.decode_header_and_key(&mut cursor)? {
            Some(header) => log_header = header,
            None => panic!("Unexpected EOF"),
        };

        assert_eq!(log_header.entry_type, EntryType::NORMAL);
        assert_eq!(log_header.key, key);
        assert_eq!(log_header.val_len, 100);

        // Verify cursor position (should be at the END of value, because we consumed it for CRC check)
        // 修正：优化后的 decode_header_and_key 只读取 Header 和 Key，不读取 BodyCRC 和 Value
        // 因此，剩余的字节数应该是 BodyCRC (4字节) + Value (100字节)
        let mut remaining = Vec::new();
        cursor.read_to_end(&mut remaining).unwrap();
        assert_eq!(
            remaining.len(),
            4 + 100,
            "Cursor should be positioned before BodyCRC and Value"
        );
        Ok(())
    }

    #[test]
    fn test_ttl_encoding() {
        let key = "ttl_key";
        let value = b"ttl_val";
        let expire_at = 1234567890u64;
        let mut buf = Vec::new();

        let entry = LogEntry::new(key.to_string(), value.to_vec(), 1)
            .with_ttl(expire_at)
            .build();
        entry.encode_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new(1024, 1024);
        let entry = decoder.decode_from(&mut cursor).unwrap().unwrap();

        assert!(entry.entry_type.has_ttl());
        assert_eq!(entry.expire_at, Some(expire_at));
        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
    }

    #[test]
    fn test_header_crc_mismatch() {
        let key = "key";
        let value = b"val";
        let mut buf = Vec::new();
        let entry = LogEntry::new(key.to_string(), value.to_vec(), 1).build();
        entry.encode_to(&mut buf).unwrap();

        // Corrupt the header (flip bits in the Type byte, which is at index 4)
        // Layout: HeaderCRC(4) | Type(1) ...
        if buf.len() > 4 {
            buf[4] ^= 0xFF;
        }

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new(1024, 1024);
        let err = decoder.decode_from(&mut cursor).unwrap_err();

        match err {
            TitaniumError::CrcMismatch { .. } => (),
            _ => panic!("Expected CrcMismatch error, got {:?}", err),
        }
    }

    #[test]
    fn test_entry_too_large() {
        let key = "large_key";
        let value = vec![0u8; 100];
        let mut buf = Vec::new();
        let entry = LogEntry::new(key.to_string(), value, 1).build();
        entry.encode_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        // Set limits smaller than actual data
        let mut decoder = Decoder::new(5, 50);
        let err = decoder.decode_from(&mut cursor).unwrap_err();

        match err {
            TitaniumError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            _ => panic!("Expected Io Error (InvalidData), got {:?}", err),
        }
    }

    #[test]
    fn test_unexpected_eof() {
        let key = "key";
        let value = b"value";
        let mut buf = Vec::new();
        let entry = LogEntry::new(key.to_string(), value.to_vec(), 1).build();
        entry.encode_to(&mut buf).unwrap();

        // Case 1: Truncate inside header
        let mut cursor = Cursor::new(&buf[..10]);
        let mut decoder = Decoder::new(1024, 1024);
        let err = decoder.decode_from(&mut cursor).unwrap_err();
        match err {
            TitaniumError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected UnexpectedEof in header, got {:?}", err),
        }

        // Case 2: Truncate inside Value
        let len = buf.len();
        let mut cursor = Cursor::new(&buf[..len - 2]);
        let err = decoder.decode_from(&mut cursor).unwrap_err();
        match err {
            TitaniumError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected UnexpectedEof in value, got {:?}", err),
        }
    }
}
