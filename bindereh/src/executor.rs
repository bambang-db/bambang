use std::sync::Arc;

use crate::{common::StorageError, manager::Manager, page::Row};

pub struct Executor {
    pub storage_manager: Arc<Manager>,
    pub root_page_id: u64,
    pub max_workers: usize,
    pub batch_size: usize,
}

impl Executor {
    pub fn new(storage_manager: Arc<Manager>, root_page_id: u64, max_workers: usize) -> Self {
        Self {
            storage_manager,
            root_page_id,
            max_workers,
            batch_size: 1000,
        }
    }

    // Insert a new row into the B+ tree
    pub async fn insert(&self, row: Row) -> Result<(), StorageError> {
        todo!()
    }
}
