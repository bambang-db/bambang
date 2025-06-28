use crate::common::StorageError;

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

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        match self {
            Value::Null => {
                bytes.push(0);
            }
            Value::Integer(val) => {
                bytes.push(1);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::String(val) => {
                bytes.push(2);
                let s_bytes = val.as_bytes();
                bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes()); // len of string
                bytes.extend_from_slice(s_bytes);
            }
            Value::Float(val) => {
                bytes.push(3);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Boolean(val) => {
                bytes.push(4);
                bytes.push(if *val { 1 } else { 0 });
            }
            Value::SmallInt(val) => {
                bytes.push(5);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::BigInt(val) => {
                bytes.push(6);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Decimal(val) => {
                bytes.push(7);
                let s_bytes = val.as_bytes();
                bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes()); // len of string
                bytes.extend_from_slice(s_bytes);
            }
            Value::Binary(val) => {
                bytes.push(8);
                bytes.extend_from_slice(&(val.len() as u32).to_le_bytes());
                bytes.extend_from_slice(val);
            }
            Value::Date(val) => {
                bytes.push(9);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Time(val) => {
                bytes.push(10);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Timestamp(val) => {
                bytes.push(11);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::DateTime(val) => {
                bytes.push(12);
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            Value::Json(s) => {
                bytes.push(13);
                let s_bytes = s.as_bytes();
                bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s_bytes);
            }
            Value::Uuid(uuid) => {
                bytes.push(14);
                bytes.extend_from_slice(uuid);
            }
            Value::Text(s) => {
                bytes.push(15);
                let s_bytes = s.as_bytes();
                bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s_bytes);
            }
            Value::Char(c) => {
                bytes.push(16);
                let mut char_bytes = [0u8; 4];
                let char_str = c.encode_utf8(&mut char_bytes);
                let char_bytes = char_str.as_bytes();
                bytes.push(char_bytes.len() as u8);
                bytes.extend_from_slice(char_bytes);
            }
            Value::TinyInt(val) => {
                bytes.push(17);
                bytes.push(*val as u8);
            }
        }

        return bytes;
    }

    pub fn from_bytes(&self, bytes: &[u8], offset: &mut usize) -> Result<Self, StorageError> {
        if *offset >= bytes.len() {
            return Err(StorageError::CorruptedData("Unexpected end of data".into()));
        }

        let type_marker = bytes[*offset];
        *offset += 1;

        match type_marker {
            0 => Ok(Value::Null),
            1 => {
                if *offset + 8 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid integer data".into()));
                }

                let val = i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());

                *offset += 8;
                Ok(Value::Integer(val))
            }
            2 => Self::deserialize_string(bytes, offset).map(Value::String),
            3 => {
                if *offset + 8 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid float data".into()));
                }
                let val = f64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                Ok(Value::Float(val))
            }
            4 => {
                if *offset >= bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid boolean data".into()));
                }
                let val = bytes[*offset] != 0;
                *offset += 1;
                Ok(Value::Boolean(val))
            }
            5 => {
                if *offset + 2 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid small int data".into()));
                }
                let val = i16::from_le_bytes(bytes[*offset..*offset + 2].try_into().unwrap());
                *offset += 2;
                Ok(Value::SmallInt(val))
            }
            6 => {
                if *offset + 16 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid big int data".into()));
                }
                let mut big_int_bytes = [0u8; 16];
                big_int_bytes.copy_from_slice(&bytes[*offset..*offset + 16]);
                let val = i128::from_le_bytes(big_int_bytes);
                *offset += 16;
                Ok(Value::BigInt(val))
            }
            7 => Self::deserialize_string(bytes, offset).map(Value::Decimal),
            8 => {
                if *offset + 4 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid binary length".into()));
                }
                let len =
                    u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
                *offset += 4;

                if *offset + len > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid binary data".into()));
                }
                let data = bytes[*offset..*offset + len].to_vec();
                *offset += len;
                Ok(Value::Binary(data))
            }
            9 => {
                if *offset + 4 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid date data".into()));
                }
                let val = i32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;
                Ok(Value::Date(val))
            }
            10 => {
                if *offset + 4 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid time data".into()));
                }
                let val = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;
                Ok(Value::Time(val))
            }
            11 => {
                if *offset + 8 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid timestamp data".into()));
                }
                let val = i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                Ok(Value::Timestamp(val))
            }
            12 => {
                if *offset + 8 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid datetime data".into()));
                }
                let val = i64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                Ok(Value::DateTime(val))
            }
            13 => Self::deserialize_string(bytes, offset).map(Value::Json),
            14 => {
                if *offset + 16 > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid UUID data".into()));
                }
                let mut uuid_bytes = [0u8; 16];
                uuid_bytes.copy_from_slice(&bytes[*offset..*offset + 16]);
                *offset += 16;
                Ok(Value::Uuid(uuid_bytes))
            }
            15 => Self::deserialize_string(bytes, offset).map(Value::Text),
            16 => {
                if *offset >= bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid char length".into()));
                }
                let char_len = bytes[*offset] as usize;
                *offset += 1;

                if *offset + char_len > bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid char data".into()));
                }
                let char_str = std::str::from_utf8(&bytes[*offset..*offset + char_len])
                    .map_err(|_| StorageError::CorruptedData("Invalid UTF-8 char".into()))?;
                let c = char_str
                    .chars()
                    .next()
                    .ok_or_else(|| StorageError::CorruptedData("Empty char data".into()))?;
                *offset += char_len;
                Ok(Value::Char(c))
            }
            17 => {
                if *offset >= bytes.len() {
                    return Err(StorageError::CorruptedData("Invalid tiny int data".into()));
                }
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

    fn deserialize_string(bytes: &[u8], offset: &mut usize) -> Result<String, StorageError> {
        if *offset + 4 > bytes.len() {
            return Err(StorageError::CorruptedData("Invalid string length".into()));
        }
        let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;

        if *offset + len > bytes.len() {
            return Err(StorageError::CorruptedData("Invalid string data".into()));
        }
        let s = String::from_utf8(bytes[*offset..*offset + len].to_vec())
            .map_err(|_| StorageError::CorruptedData("Invalid UTF-8 string".into()))?;
        *offset += len;
        Ok(s)
    }
}
