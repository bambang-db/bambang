use crate::common::StorageError;
use std::convert::TryInto;

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Integer(i64),
    String(String),
    Float(f64),
    Boolean(bool),
    Null,
    SmallInt(i16),
    BigInt(i128),
    Decimal(String), // Store as string to maintain precision
    Binary(Vec<u8>),
    Date(i32),      // Days since epoch (1970-01-01)
    Time(u32),      // Milliseconds since midnight
    Timestamp(i64), // Milliseconds since Unix epoch
    DateTime(i64),  // Alias for Timestamp
    Json(String),   // JSON stored as string
    Uuid([u8; 16]), // 128-bit UUID
    Text(String),   // Large text (same as String but semantic difference)
    Char(char),     // Single character
    TinyInt(i8),    // 8-bit integer
}

// Type markers as constants for better maintainability
const NULL_TYPE: u8 = 0;
const INTEGER_TYPE: u8 = 1;
const STRING_TYPE: u8 = 2;
const FLOAT_TYPE: u8 = 3;
const BOOLEAN_TYPE: u8 = 4;
const SMALLINT_TYPE: u8 = 5;
const BIGINT_TYPE: u8 = 6;
const DECIMAL_TYPE: u8 = 7;
const BINARY_TYPE: u8 = 8;
const DATE_TYPE: u8 = 9;
const TIME_TYPE: u8 = 10;
const TIMESTAMP_TYPE: u8 = 11;
const DATETIME_TYPE: u8 = 12;
const JSON_TYPE: u8 = 13;
const UUID_TYPE: u8 = 14;
const TEXT_TYPE: u8 = 15;
const CHAR_TYPE: u8 = 16;
const TINYINT_TYPE: u8 = 17;

impl Value {
    /// Convert the Value to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match self {
            Value::Null => {
                bytes.push(NULL_TYPE);
            }
            Value::Integer(val) => {
                bytes.push(INTEGER_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::String(val) => {
                bytes.push(STRING_TYPE);
                Self::serialize_string_like(&mut bytes, val.as_bytes());
            }
            Value::Float(val) => {
                bytes.push(FLOAT_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Boolean(val) => {
                bytes.push(BOOLEAN_TYPE);
                bytes.push(if *val { 1 } else { 0 });
            }
            Value::SmallInt(val) => {
                bytes.push(SMALLINT_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::BigInt(val) => {
                bytes.push(BIGINT_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Decimal(val) => {
                bytes.push(DECIMAL_TYPE);
                Self::serialize_string_like(&mut bytes, val.as_bytes());
            }
            Value::Binary(val) => {
                bytes.push(BINARY_TYPE);
                Self::serialize_binary(&mut bytes, val);
            }
            Value::Date(val) => {
                bytes.push(DATE_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Time(val) => {
                bytes.push(TIME_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Timestamp(val) => {
                bytes.push(TIMESTAMP_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::DateTime(val) => {
                bytes.push(DATETIME_TYPE);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Json(val) => {
                bytes.push(JSON_TYPE);
                Self::serialize_string_like(&mut bytes, val.as_bytes());
            }
            Value::Uuid(uuid) => {
                bytes.push(UUID_TYPE);
                bytes.extend_from_slice(uuid);
            }
            Value::Text(val) => {
                bytes.push(TEXT_TYPE);
                Self::serialize_string_like(&mut bytes, val.as_bytes());
            }
            Value::Char(c) => {
                bytes.push(CHAR_TYPE);
                let mut char_bytes = [0u8; 4];
                let char_str = c.encode_utf8(&mut char_bytes);
                let char_bytes = char_str.as_bytes();
                bytes.push(char_bytes.len() as u8);
                bytes.extend_from_slice(char_bytes);
            }
            Value::TinyInt(val) => {
                bytes.push(TINYINT_TYPE);
                bytes.push(*val as u8);
            }
        }

        bytes
    }

    /// Deserialize a Value from bytes
    pub fn from_bytes(bytes: &[u8], offset: &mut usize) -> Result<Self, StorageError> {
        if *offset >= bytes.len() {
            return Err(StorageError::CorruptedData("Unexpected end of data".into()));
        }

        let type_marker = bytes[*offset];
        *offset += 1;

        match type_marker {
            NULL_TYPE => Ok(Value::Null),
            INTEGER_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 8, "integer")?;
                let val =
                    i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid integer bytes".into())
                    })?);
                *offset += 8;
                Ok(Value::Integer(val))
            }
            STRING_TYPE => Self::deserialize_string(bytes, offset).map(Value::String),
            FLOAT_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 8, "float")?;
                let val = f64::from_le_bytes(
                    bytes[*offset..*offset + 8]
                        .try_into()
                        .map_err(|_| StorageError::CorruptedData("Invalid float bytes".into()))?,
                );
                *offset += 8;
                Ok(Value::Float(val))
            }
            BOOLEAN_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 1, "boolean")?;
                let val = bytes[*offset] != 0;
                *offset += 1;
                Ok(Value::Boolean(val))
            }
            SMALLINT_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 2, "small int")?;
                let val =
                    i16::from_le_bytes(bytes[*offset..*offset + 2].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid small int bytes".into())
                    })?);
                *offset += 2;
                Ok(Value::SmallInt(val))
            }
            BIGINT_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 16, "big int")?;
                let val =
                    i128::from_le_bytes(bytes[*offset..*offset + 16].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid big int bytes".into())
                    })?);
                *offset += 16;
                Ok(Value::BigInt(val))
            }
            DECIMAL_TYPE => Self::deserialize_string(bytes, offset).map(Value::Decimal),
            BINARY_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 4, "binary length")?;
                let len =
                    u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid binary length bytes".into())
                    })?) as usize;
                *offset += 4;

                Self::ensure_bytes_available(bytes, *offset, len, "binary data")?;
                let data = bytes[*offset..*offset + len].to_vec();
                *offset += len;
                Ok(Value::Binary(data))
            }
            DATE_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 4, "date")?;
                let val = i32::from_le_bytes(
                    bytes[*offset..*offset + 4]
                        .try_into()
                        .map_err(|_| StorageError::CorruptedData("Invalid date bytes".into()))?,
                );
                *offset += 4;
                Ok(Value::Date(val))
            }
            TIME_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 4, "time")?;
                let val = u32::from_le_bytes(
                    bytes[*offset..*offset + 4]
                        .try_into()
                        .map_err(|_| StorageError::CorruptedData("Invalid time bytes".into()))?,
                );
                *offset += 4;
                Ok(Value::Time(val))
            }
            TIMESTAMP_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 8, "timestamp")?;
                let val =
                    i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid timestamp bytes".into())
                    })?);
                *offset += 8;
                Ok(Value::Timestamp(val))
            }
            DATETIME_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 8, "datetime")?;
                let val =
                    i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().map_err(|_| {
                        StorageError::CorruptedData("Invalid datetime bytes".into())
                    })?);
                *offset += 8;
                Ok(Value::DateTime(val))
            }
            JSON_TYPE => Self::deserialize_string(bytes, offset).map(Value::Json),
            UUID_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 16, "UUID")?;
                let mut uuid_bytes = [0u8; 16];
                uuid_bytes.copy_from_slice(&bytes[*offset..*offset + 16]);
                *offset += 16;
                Ok(Value::Uuid(uuid_bytes))
            }
            TEXT_TYPE => Self::deserialize_string(bytes, offset).map(Value::Text),
            CHAR_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 1, "char length")?;
                let char_len = bytes[*offset] as usize;
                *offset += 1;

                if char_len == 0 || char_len > 4 {
                    return Err(StorageError::CorruptedData(
                        "Invalid char length: must be 1-4 bytes".into(),
                    ));
                }

                Self::ensure_bytes_available(bytes, *offset, char_len, "char data")?;
                let char_str = std::str::from_utf8(&bytes[*offset..*offset + char_len])
                    .map_err(|_| StorageError::CorruptedData("Invalid UTF-8 char".into()))?;
                let c = char_str
                    .chars()
                    .next()
                    .ok_or_else(|| StorageError::CorruptedData("Empty char data".into()))?;
                *offset += char_len;
                Ok(Value::Char(c))
            }
            TINYINT_TYPE => {
                Self::ensure_bytes_available(bytes, *offset, 1, "tiny int")?;
                let val = bytes[*offset] as i8;
                *offset += 1;
                Ok(Value::TinyInt(val))
            }
            _ => Err(StorageError::CorruptedData(format!(
                "Unknown type marker: {}",
                type_marker
            ))),
        }
    }

    /// Helper function to serialize string-like data (String, Text, Json, Decimal)
    fn serialize_string_like(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
    }

    /// Helper function to serialize binary data
    fn serialize_binary(bytes: &mut Vec<u8>, data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
    }

    /// Helper function to ensure enough bytes are available
    fn ensure_bytes_available(
        bytes: &[u8],
        offset: usize,
        required: usize,
        data_type: &str,
    ) -> Result<(), StorageError> {
        if offset + required > bytes.len() {
            return Err(StorageError::CorruptedData(format!(
                "Invalid {} data: need {} bytes, got {}",
                data_type,
                required,
                bytes.len() - offset
            )));
        }
        Ok(())
    }

    /// Helper function to deserialize string data
    fn deserialize_string(bytes: &[u8], offset: &mut usize) -> Result<String, StorageError> {
        Self::ensure_bytes_available(bytes, *offset, 4, "string length")?;
        let len = u32::from_le_bytes(
            bytes[*offset..*offset + 4]
                .try_into()
                .map_err(|_| StorageError::CorruptedData("Invalid string length bytes".into()))?,
        ) as usize;
        *offset += 4;

        Self::ensure_bytes_available(bytes, *offset, len, "string data")?;
        let s = String::from_utf8(bytes[*offset..*offset + len].to_vec())
            .map_err(|_| StorageError::CorruptedData("Invalid UTF-8 string".into()))?;
        *offset += len;
        Ok(s)
    }

    /// Get the size in bytes this value would take when serialized
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Integer(_) => 9,           // 1 byte type + 8 bytes data
            Value::String(s) => 5 + s.len(),  // 1 byte type + 4 bytes length + data
            Value::Float(_) => 9,             // 1 byte type + 8 bytes data
            Value::Boolean(_) => 2,           // 1 byte type + 1 byte data
            Value::SmallInt(_) => 3,          // 1 byte type + 2 bytes data
            Value::BigInt(_) => 17,           // 1 byte type + 16 bytes data
            Value::Decimal(s) => 5 + s.len(), // 1 byte type + 4 bytes length + data
            Value::Binary(b) => 5 + b.len(),  // 1 byte type + 4 bytes length + data
            Value::Date(_) => 5,              // 1 byte type + 4 bytes data
            Value::Time(_) => 5,              // 1 byte type + 4 bytes data
            Value::Timestamp(_) => 9,         // 1 byte type + 8 bytes data
            Value::DateTime(_) => 9,          // 1 byte type + 8 bytes data
            Value::Json(s) => 5 + s.len(),    // 1 byte type + 4 bytes length + data
            Value::Uuid(_) => 17,             // 1 byte type + 16 bytes data
            Value::Text(s) => 5 + s.len(),    // 1 byte type + 4 bytes length + data
            Value::Char(c) => {
                let mut buf = [0u8; 4];
                let char_str = c.encode_utf8(&mut buf);
                3 + char_str.len() // 1 byte type + 1 byte length + char bytes
            }
            Value::TinyInt(_) => 2, // 1 byte type + 1 byte data
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Integer(_) => "integer",
            Value::String(_) => "string",
            Value::Float(_) => "float",
            Value::Boolean(_) => "boolean",
            Value::SmallInt(_) => "smallint",
            Value::BigInt(_) => "bigint",
            Value::Decimal(_) => "decimal",
            Value::Binary(_) => "binary",
            Value::Date(_) => "date",
            Value::Time(_) => "time",
            Value::Timestamp(_) => "timestamp",
            Value::DateTime(_) => "datetime",
            Value::Json(_) => "json",
            Value::Uuid(_) => "uuid",
            Value::Text(_) => "text",
            Value::Char(_) => "char",
            Value::TinyInt(_) => "tinyint",
        }
    }
}
