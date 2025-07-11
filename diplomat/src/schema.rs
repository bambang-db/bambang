use std::collections::HashMap;

use bindereh::page::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

// Core schema types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub column_map: HashMap<String, usize>, // Map column names to indices
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
                return None; // Column not found
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
}
