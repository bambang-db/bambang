//! Error types and constants shared across modules

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
    fn from(_err: std::io::Error) -> Self {
        StorageError::IoError("I/O Error".to_string())
    }
}

impl std::error::Error for StorageError {}