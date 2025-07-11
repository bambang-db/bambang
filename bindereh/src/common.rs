#[derive(Debug)]
pub enum StorageError {
    CorruptedData(String),
    InvalidData(String),
    IoError(String),
    DuplicateKey(String),
    InvalidInput(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::CorruptedData(msg) => write!(f, "Corrupted data: {}", msg),
            StorageError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            StorageError::IoError(msg) => write!(f, "IO error: {}", msg),
            StorageError::DuplicateKey(msg) => write!(f, "Duplicate key: {}", msg),
            StorageError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::IoError("I/O Error".to_string())
    }
}

impl std::error::Error for StorageError {}

pub const PAGE_SIZE: usize = 2048; // 2KB pages like most databases
pub const NODE_HEADER_SIZE: usize = 16; // Basic node metadata
pub const MAX_KEYS_PER_NODE: usize = 4; // Configurable based on key size
pub const MAGIC_NUMBER: u32 = 0xDEADBEEF; // File format identifier
