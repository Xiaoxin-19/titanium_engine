use crate::error::TitaniumError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

const MAX_KEY_SIZE: usize = 1024; // 1 KB
const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB

// log entry
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub key: String,
    pub value: Vec<u8>,
}

pub struct LogHeader {
    pub crc: u32,
    pub key: String,
    pub val_len: u32,
}

// zero allocation decoder
pub struct Decoder {
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl LogEntry {
    // zero allocation write
    pub fn encode_to<W: Write>(
        key: &str,
        value: &[u8],
        writer: &mut W,
    ) -> Result<u64, TitaniumError> {
        let k_len = key.len() as u32;
        let v_len = value.len() as u32;

        let mut k_len_buf = [0u8; 5];
        let k_len_size = encode_varint(k_len, &mut k_len_buf);
        let mut v_len_buf = [0u8; 5];
        let v_len_size = encode_varint(v_len, &mut v_len_buf);

        // 1. First, calculate the CRC (including lengths and content)
        let crc = generate_crc(
            &k_len_buf[..k_len_size],
            &v_len_buf[..v_len_size],
            key,
            value,
        );

        // 2. Then write everything to the writer
        writer.write_u32::<LittleEndian>(crc)?;
        writer.write_all(&k_len_buf[..k_len_size])?;
        writer.write_all(&v_len_buf[..v_len_size])?;
        writer.write_all(key.as_bytes())?;
        writer.write_all(value)?;

        // Return the total number of bytes written, which is crucial for maintaining the memory index (Offset)
        Ok((4 + k_len_size + v_len_size + k_len as usize + v_len as usize) as u64)
    }
}

impl Decoder {
    pub fn new() -> Self {
        Decoder {
            key_buf: Vec::new(),
            value_buf: Vec::new(),
        }
    }

    pub fn decode_from<R: Read>(&mut self, reader: &mut R) -> Result<LogEntry, TitaniumError> {
        let crc = reader.read_u32::<LittleEndian>()?;
        let k_len = decode_varint(reader)?;
        let v_len = decode_varint(reader)?;

        // Prevent attacks with excessively large entries
        if k_len > MAX_KEY_SIZE as u32 || v_len > MAX_VALUE_SIZE as u32 {
            return Err(TitaniumError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Entry too large",
            )));
        }

        // 1. Correctly resize the buffers and read data into them
        self.key_buf.resize(k_len as usize, 0);
        reader.read_exact(&mut self.key_buf)?;

        self.value_buf.resize(v_len as usize, 0);
        reader.read_exact(&mut self.value_buf)?;

        // 2. Then verify the overall CRC
        // During verification, we directly use the data in the buffers to avoid cloning
        if validate_crc(crc, k_len, v_len, &self.key_buf, &self.value_buf) {
            return Err(TitaniumError::CrcMismatch { expected: crc });
        }

        // 3. Finally, construct the LogEntry without unnecessary cloning
        let key = String::from_utf8(self.key_buf.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Key is not valid UTF-8"))?;

        Ok(LogEntry {
            key,
            value: self.value_buf.clone(), // must clone to return owned data
        })
    }

    /// 仅解码头部和 Key，返回 (Value长度, Key)，并将 Reader 指针停留在 Value 的起始位置。
    /// 用于流式读取 Value。
    pub fn decode_header_and_key<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<LogHeader>, TitaniumError> {
        let mut crc_buf = [0u8; 4];
        // 尝试读取第一个字节来判断 EOF
        match reader.read(&mut crc_buf[0..1]) {
            Ok(0) => return Ok(None), // Clean EOF
            Ok(1) => reader.read_exact(&mut crc_buf[1..])?,
            Ok(_) => unreachable!(),
            Err(e) => return Err(TitaniumError::Io(e)),
        }
        let crc = u32::from_le_bytes(crc_buf);
        let k_len = decode_varint(reader)?;
        let v_len = decode_varint(reader)?;

        // 进行大小检查
        if k_len > MAX_KEY_SIZE as u32 || v_len > MAX_VALUE_SIZE as u32 {
            return Err(TitaniumError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Entry too large",
            )));
        }

        // 校验CRC
        self.key_buf.resize(k_len as usize, 0);
        reader.read_exact(&mut self.key_buf)?;

        self.value_buf.resize(v_len as usize, 0);
        reader.read_exact(&mut self.value_buf)?;
        if validate_crc(crc, k_len, v_len, &self.key_buf, &self.value_buf) {
            return Err(TitaniumError::CrcMismatch { expected: crc });
        }
        let key = String::from_utf8(self.key_buf.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Key is not valid UTF-8"))?;

        Ok(Some(LogHeader {
            crc,
            key,
            val_len: v_len,
        }))
    }
}

pub fn validate_crc(
    crc: u32,
    k_len: u32,
    v_len: u32,
    key_buf: &Vec<u8>,
    val_buf: &Vec<u8>,
) -> bool {
    let mut hasher = crc32fast::Hasher::new();

    let mut k_len_buf = [0u8; 5];
    let k_len_size = encode_varint(k_len, &mut k_len_buf);
    hasher.update(&k_len_buf[..k_len_size]);

    let mut v_len_buf = [0u8; 5];
    let v_len_size = encode_varint(v_len, &mut v_len_buf);
    hasher.update(&v_len_buf[..v_len_size]);

    hasher.update(key_buf);
    hasher.update(val_buf);

    let calculated_crc = hasher.finalize();
    return calculated_crc != crc;
}

pub fn generate_crc(key_len: &[u8], val_len: &[u8], key: &str, value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key_len);
    hasher.update(val_len);
    hasher.update(key.as_bytes());
    hasher.update(value);
    hasher.finalize()
}

/// Encodes a u32 integer into a variable-length format (Varint). returns the number of bytes written.
///
/// # Example: Encoding 300
///
/// ```text
/// 300 in binary:  0000 0001 0010 1100
/// First byte:     1010 1100 (0xAC) -> lower 7 bits + continuation bit
/// Second byte:    0000 0010 (0x02) -> remaining bits
/// Result:         0xAC02
/// ```
pub fn encode_varint(mut n: u32, buf: &mut [u8]) -> usize {
    let mut counter = 0;
    loop {
        let mut b = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        buf[counter] = b;
        counter += 1;
        if n == 0 {
            break;
        }
    }
    counter
}

/// Decodes a variable-length integer (Varint) from a reader. returns the decoded u32.
///
/// # Example: Decoding 300 from 0xAC, 0x02
///
/// ```text
/// Byte 0: 1010 1100 (0xAC)
/// Keep 7: 010 1100
/// Result: 0000 0000 0010 1100 (44)
/// Continuation: Yes
///
/// Byte 1: 0000 0010 (0x02)
/// Keep 7: 000 0010
/// Shift 7: 1 0000 0000 (256)
/// Result: 1 0010 1100 (300)
/// Continuation: No
/// ```
fn decode_varint<R: Read>(reader: &mut R) -> Result<u32, TitaniumError> {
    let mut result = 0;
    let mut shift = 0;
    loop {
        if shift > 28 {
            return Err(TitaniumError::VarintDecodeError);
        }
        let byte = reader.read_u8()?;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
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
        let n = encode_varint(50, &mut buf);
        assert_eq!(n, 1);
        assert_eq!(buf[0], 50);
        let mut cursor = Cursor::new(&buf[..n]);
        assert_eq!(decode_varint(&mut cursor).unwrap(), 50);

        // Test number > 127 (300 = 0x12C)
        // Binary: 0000 0001 0010 1100
        // 7 bits groups: 0000010 (2) | 0101100 (44)
        // Encoded: [44 | 0x80, 2] = [0xAC, 0x02]
        let n = encode_varint(300, &mut buf);
        assert_eq!(n, 2);
        assert_eq!(buf[0], 0xAC);
        assert_eq!(buf[1], 0x02);
        let mut cursor = Cursor::new(&buf[..n]);
        assert_eq!(decode_varint(&mut cursor).unwrap(), 300);
    }

    #[test]
    fn test_log_entry_encode_decode() {
        let key = "test_key";
        let value = b"test_value";
        let mut buf = Vec::new();

        // Encode
        let written = LogEntry::encode_to(key, value, &mut buf).unwrap();
        assert!(written > 0);

        // Decode
        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new();
        let entry = decoder.decode_from(&mut cursor).unwrap();

        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
    }

    #[test]
    fn test_crc_mismatch() {
        let key = "key";
        let value = b"val";
        let mut buf = Vec::new();
        LogEntry::encode_to(key, value, &mut buf).unwrap();

        // Corrupt the data (flip bits in the last byte of value)
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new();
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
        LogEntry::encode_to(key, &value, &mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let mut decoder = Decoder::new();
        let log_header;
        match decoder.decode_header_and_key(&mut cursor)? {
            Some(header) => log_header = header,
            None => panic!("Unexpected EOF"),
        };

        assert_eq!(log_header.key, key);
        assert_eq!(log_header.val_len, 100);

        // Verify cursor position (should be at the start of value)
        let mut remaining = Vec::new();
        cursor.read_to_end(&mut remaining).unwrap();
        assert_eq!(remaining, value);
        Ok(())
    }
}
