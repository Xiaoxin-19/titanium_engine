use thiserror::Error;

#[derive(Error, Debug)]
pub enum TitaniumError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC Mismatch: expected {expected}, got {actual}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Varint Decode Error")]
    VarintDecodeError,
}
