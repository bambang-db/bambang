use std::sync::Arc;

use crate::{common::StorageError, manager::Manager};

pub struct DeleteOperation {
    storage_manager: Arc<Manager>,
}

/// Result type for delete operations
#[derive(Debug, Clone)]
pub enum DeleteResult {
    Single(bool),
    Multiple(u64),
    Truncated,
}

#[derive(Debug, Clone)]
pub struct DeleteOptions {
    pub delete_type: DeleteType,
}

#[derive(Debug, Clone)]
pub enum DeleteType {
    ByKey(u64),
    Truncate,
}

impl DeleteOptions {
    pub fn by_key(key: u64) -> Self {
        Self {
            delete_type: DeleteType::ByKey(key),
        }
    }

    pub fn truncate() -> Self {
        Self {
            delete_type: DeleteType::Truncate,
        }
    }
}

impl DeleteOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(&self, options: DeleteOptions) -> Result<DeleteResult, StorageError> {
        match options.delete_type {
            DeleteType::ByKey(key) => {
                let deleted = self.delete_by_key(key).await?;
                Ok(DeleteResult::Single(deleted))
            }
            DeleteType::Truncate => {
                self.truncate().await?;
                Ok(DeleteResult::Truncated)
            }
        }
    }

    /// Delete a single row by key (internal implementation)
    async fn delete_by_key(&self, key: u64) -> Result<bool, StorageError> {
        Ok(true)
    }

    pub async fn truncate(&self) -> Result<(), StorageError> {
        self.storage_manager.truncate().await?;
        Ok(())
    }
}
