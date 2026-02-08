use thiserror::Error;

#[derive(Error, Debug)]
pub enum TitaniumError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC Mismatch: expected {expected}")]
    CrcMismatch { expected: u32 },

    #[error("Varint Decode Error")]
    VarintDecodeError,

    #[error("Config Error: {0}")]
    ConfigError(String),

    #[error("System Overload: too many pending files, compaction falling behind")]
    SystemOverload,

    #[error("Disk Full: available space {available} is less than required {required}")]
    DiskFull { available: u64, required: u64 },
}
