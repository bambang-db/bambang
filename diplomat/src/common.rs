#[derive(Debug)]
pub enum CatalogError {
    IoError(std::io::Error),
    SerializationError(bincode::error::EncodeError),
    DeserializationError(bincode::error::DecodeError),
    TableExists(String),
    TableNotFound(String),
    ColumnExists(String),
    ColumnNotFound(String),
    InvalidSchema(String),
}

impl From<std::io::Error> for CatalogError {
    fn from(error: std::io::Error) -> Self {
        CatalogError::IoError(error)
    }
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::IoError(e) => write!(f, "IO error: {}", e),
            CatalogError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            CatalogError::DeserializationError(e) => write!(f, "Deserialization error: {}", e),
            CatalogError::TableExists(name) => write!(f, "Table '{}' already exists", name),
            CatalogError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            CatalogError::ColumnExists(name) => write!(f, "Column '{}' already exists", name),
            CatalogError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            CatalogError::InvalidSchema(msg) => write!(f, "Invalid schema: {}", msg),
        }
    }
}

impl std::error::Error for CatalogError {}