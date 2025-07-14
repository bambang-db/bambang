use crate::row::Row;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub enum DataType {
    Integer,
    String,
    Float,
    Boolean,
    SmallInt,
    BigInt,
    Decimal,
    Binary,
    Date,
    Time,
    Timestamp,
    DateTime,
    Json,
    Uuid,
    Text,
    Char,
    TinyInt,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool, primary_key: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
            primary_key,
        }
    }

    pub fn primary_key(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, false, true)
    }

    pub fn nullable(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, true, false)
    }

    pub fn not_null(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, false, false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub column_map: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut column_map = HashMap::new();
        for (idx, column) in columns.iter().enumerate() {
            column_map.insert(column.name.clone(), idx);
        }

        Schema {
            columns,
            column_map,
        }
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.column_map
            .get(name)
            .and_then(|&idx| self.columns.get(idx))
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }

    pub fn get_column_indices(&self, column_names: &[String]) -> Option<Vec<usize>> {
        let mut indices = Vec::new();
        for name in column_names {
            if let Some(&idx) = self.column_map.get(name) {
                indices.push(idx);
            } else {
                return None;
            }
        }
        Some(indices)
    }

    pub fn project_row(&self, row: &Row, column_indices: &[usize]) -> Row {
        let mut projected_data = Vec::new();

        for &idx in column_indices {
            if idx < row.data.len() {
                projected_data.push(row.data[idx].clone());
            }
        }

        Row {
            id: row.id,
            data: projected_data,
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.column_map.contains_key(name)
    }

    pub fn primary_key_columns(&self) -> Vec<&Column> {
        self.columns.iter().filter(|c| c.primary_key).collect()
    }
}
