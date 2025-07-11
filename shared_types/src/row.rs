//! Row type for database storage

use crate::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub id: u64,
    pub data: Vec<Value>,
}

impl Row {
    /// Create a new row with the given id and data
    pub fn new(id: u64, data: Vec<Value>) -> Self {
        Self { id, data }
    }

    /// Get the number of columns in this row
    pub fn column_count(&self) -> usize {
        self.data.len()
    }

    /// Get a value at the specified column index
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.data.get(index)
    }

    /// Set a value at the specified column index
    pub fn set_value(&mut self, index: usize, value: Value) -> Result<(), String> {
        if index < self.data.len() {
            self.data[index] = value;
            Ok(())
        } else {
            Err(format!("Column index {} out of bounds", index))
        }
    }

    /// Add a new value to the row
    pub fn push_value(&mut self, value: Value) {
        self.data.push(value);
    }

    /// Get the total serialized size of this row
    pub fn serialized_size(&self) -> usize {
        8 + // id (u64)
        4 + // data length (u32)
        self.data.iter().map(|v| v.serialized_size()).sum::<usize>()
    }
}