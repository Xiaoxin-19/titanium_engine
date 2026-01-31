use crate::error::TitaniumError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

// log entry
pub struct LogEntry {
    pub key: String,
    pub value: Vec<u8>,
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
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&k_len_buf[..k_len_size]);
        hasher.update(&v_len_buf[..v_len_size]);
        hasher.update(key.as_bytes());
        hasher.update(value);
        let crc = hasher.finalize();

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
        if k_len > 10 * 1024 * 1024 || v_len > 10 * 1024 * 1024 {
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
        let mut hasher = crc32fast::Hasher::new();

        let mut k_len_buf = [0u8; 5];
        let k_len_size = encode_varint(k_len, &mut k_len_buf);
        hasher.update(&k_len_buf[..k_len_size]);

        let mut v_len_buf = [0u8; 5];
        let v_len_size = encode_varint(v_len, &mut v_len_buf);
        hasher.update(&v_len_buf[..v_len_size]);

        hasher.update(&self.key_buf);
        hasher.update(&self.value_buf);

        let calculated_crc = hasher.finalize();
        if calculated_crc != crc {
            return Err(TitaniumError::CrcMismatch {
                expected: crc,
                actual: calculated_crc,
            });
        }

        // 3. Finally, construct the LogEntry without unnecessary cloning
        let key = String::from_utf8(self.key_buf.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Key is not valid UTF-8"))?;

        Ok(LogEntry {
            key,
            value: self.value_buf.clone(), // must clone to return owned data
        })
    }
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
fn encode_varint(mut n: u32, buf: &mut [u8]) -> usize {
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
