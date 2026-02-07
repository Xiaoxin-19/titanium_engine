use std::io::Read;

use byteorder::ReadBytesExt;

use crate::error::TitaniumError;

pub trait Varint: Sized + Copy {
    fn to_u64(self) -> u64;
    fn from_u64(v: u64) -> Self;
    const MAX_SHIFT: usize;
}

impl Varint for u32 {
    fn to_u64(self) -> u64 {
        self as u64
    }
    fn from_u64(v: u64) -> Self {
        v as u32
    }
    const MAX_SHIFT: usize = 28;
}

impl Varint for u64 {
    fn to_u64(self) -> u64 {
        self
    }
    fn from_u64(v: u64) -> Self {
        v
    }
    const MAX_SHIFT: usize = 63;
}

/// Encodes an integer into a variable-length format (Varint). returns the number of bytes written.
pub fn encode_varint<T: Varint>(n: T, buf: &mut [u8]) -> usize {
    let mut n = n.to_u64();
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

/// Decodes a variable-length integer (Varint) from a reader.
pub fn decode_varint<R: Read, T: Varint>(reader: &mut R) -> Result<T, TitaniumError> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if shift > T::MAX_SHIFT {
            return Err(TitaniumError::VarintDecodeError);
        }
        let byte = reader.read_u8()?;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(T::from_u64(result));
        }
        shift += 7;
    }
}
