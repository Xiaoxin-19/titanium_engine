use std::io::Read;

use byteorder::ReadBytesExt;

use crate::error::TitaniumError;

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
pub fn decode_varint<R: Read>(reader: &mut R) -> Result<u32, TitaniumError> {
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
