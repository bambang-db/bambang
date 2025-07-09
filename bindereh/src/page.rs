use std::io::{Cursor, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    common::{MAGIC_NUMBER, NODE_HEADER_SIZE, PAGE_SIZE, StorageError},
    value::Value,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub id: u64,
    pub data: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub page_id: u64,
    pub is_leaf: bool,
    pub parent_page_id: Option<u64>,
    pub keys: Vec<u64>,
    pub values: Vec<Row>,               // Only for leaf nodes
    pub child_page_ids: Vec<u64>,       // Page IDs for children (internal nodes)
    pub next_leaf_page_id: Option<u64>, // Leaf node linking
    pub is_dirty: bool,                 // Track if node needs to be written to disk
}

impl Page {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write magic number
        bytes.write_u32::<LittleEndian>(MAGIC_NUMBER).unwrap();

        // Write node metadata
        bytes.write_u64::<LittleEndian>(self.page_id).unwrap();
        bytes.write_u8(if self.is_leaf { 1 } else { 0 }).unwrap();
        bytes
            .write_u64::<LittleEndian>(self.parent_page_id.unwrap_or(0))
            .unwrap();
        bytes
            .write_u64::<LittleEndian>(self.next_leaf_page_id.unwrap_or(0))
            .unwrap();

        // Write keys
        bytes
            .write_u32::<LittleEndian>(self.keys.len() as u32)
            .unwrap();
        for key in &self.keys {
            bytes.write_u64::<LittleEndian>(*key).unwrap();
        }

        if self.is_leaf {
            // Write values (rows)
            bytes
                .write_u32::<LittleEndian>(self.values.len() as u32)
                .unwrap();
            for row in &self.values {
                // Write row ID
                bytes.write_u64::<LittleEndian>(row.id).unwrap();

                // Write row data
                bytes
                    .write_u32::<LittleEndian>(row.data.len() as u32)
                    .unwrap();
                for value in &row.data {
                    let value_bytes = value.to_bytes();
                    bytes
                        .write_u32::<LittleEndian>(value_bytes.len() as u32)
                        .unwrap();
                    bytes.write_all(&value_bytes).unwrap();
                }
            }
        } else {
            // Write child page IDs
            bytes
                .write_u32::<LittleEndian>(self.child_page_ids.len() as u32)
                .unwrap();
            for page_id in &self.child_page_ids {
                bytes.write_u64::<LittleEndian>(*page_id).unwrap();
            }
        }

        // Pad to page to PAGE_SIZE value
        bytes.resize(PAGE_SIZE, 0);

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() < NODE_HEADER_SIZE {
            return Err(StorageError::CorruptedData("Invalid node size".into()));
        }

        let mut reader = Cursor::new(bytes);

        // Read magic number
        let magic = reader
            .read_u32::<LittleEndian>()
            .map_err(|_| StorageError::CorruptedData("Failed to read magic number".into()))?;
        if magic != MAGIC_NUMBER {
            return Err(StorageError::CorruptedData("Invalid magic number".into()));
        }

        // Read node metadata
        let page_id = reader
            .read_u64::<LittleEndian>()
            .map_err(|_| StorageError::CorruptedData("Failed to read page_id".into()))?;

        let is_leaf = reader
            .read_u8()
            .map_err(|_| StorageError::CorruptedData("Failed to read is_leaf".into()))?
            == 1;

        let parent_page_id_raw = reader
            .read_u64::<LittleEndian>()
            .map_err(|_| StorageError::CorruptedData("Failed to read parent_page_id".into()))?;
        let parent_page_id = if parent_page_id_raw == 0 {
            None
        } else {
            Some(parent_page_id_raw)
        };

        let next_leaf_page_id_raw = reader
            .read_u64::<LittleEndian>()
            .map_err(|_| StorageError::CorruptedData("Failed to read next_leaf_page_id".into()))?;
        let next_leaf_page_id = if next_leaf_page_id_raw == 0 {
            None
        } else {
            Some(next_leaf_page_id_raw)
        };

        // Read keys
        let key_count = reader
            .read_u32::<LittleEndian>()
            .map_err(|_| StorageError::CorruptedData("Failed to read key count".into()))?
            as usize;

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let key = reader
                .read_u64::<LittleEndian>()
                .map_err(|_| StorageError::CorruptedData("Failed to read key".into()))?;
            keys.push(key);
        }

        let mut values = Vec::new();
        let mut child_page_ids = Vec::new();

        if is_leaf {
            // Read values (rows)
            let value_count = reader
                .read_u32::<LittleEndian>()
                .map_err(|_| StorageError::CorruptedData("Failed to read value count".into()))?
                as usize;

            values.reserve(value_count);
            for _ in 0..value_count {
                // Read row ID
                let row_id = reader
                    .read_u64::<LittleEndian>()
                    .map_err(|_| StorageError::CorruptedData("Failed to read row ID".into()))?;

                // Read row data
                let data_count = reader
                    .read_u32::<LittleEndian>()
                    .map_err(|_| StorageError::CorruptedData("Failed to read data count".into()))?
                    as usize;

                let mut row_data = Vec::with_capacity(data_count);
                for _ in 0..data_count {
                    let value_len = reader.read_u32::<LittleEndian>().map_err(|_| {
                        StorageError::CorruptedData("Failed to read value length".into())
                    })? as usize;

                    let current_position = reader.position() as usize;
                    if current_position + value_len > bytes.len() {
                        return Err(StorageError::CorruptedData(
                            "Value length exceeds buffer".into(),
                        ));
                    }

                    let value_bytes = &bytes[current_position..current_position + value_len];
                    let mut value_offset = 0;
                    let value = Value::from_bytes(value_bytes, &mut value_offset)?;
                    row_data.push(value);

                    // Advance the reader position
                    reader.set_position((current_position + value_len) as u64);
                }

                values.push(Row {
                    id: row_id,
                    data: row_data,
                });
            }
        } else {
            // Read child page IDs
            let child_count = reader
                .read_u32::<LittleEndian>()
                .map_err(|_| StorageError::CorruptedData("Failed to read child count".into()))?
                as usize;

            child_page_ids.reserve(child_count);
            for _ in 0..child_count {
                let page_id = reader.read_u64::<LittleEndian>().map_err(|_| {
                    StorageError::CorruptedData("Failed to read child page ID".into())
                })?;
                child_page_ids.push(page_id);
            }
        }

        Ok(Page {
            page_id,
            is_leaf,
            parent_page_id,
            keys,
            values,
            child_page_ids,
            next_leaf_page_id,
            is_dirty: false,
        })
    }
}
