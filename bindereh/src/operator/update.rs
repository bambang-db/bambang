use std::sync::Arc;

use shared_types::Row;

use crate::{common::StorageError, manager::Manager, operator::tree::TreeOperations};

pub struct UpdateOperation {
    storage_manager: Arc<Manager>,
}

impl UpdateOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(
        &self,
        key: u64,
        row: Row,
        root_page_id: u64,
    ) -> Result<bool, StorageError> {
        let root_page = self.storage_manager.read_page(root_page_id).await?;

        // Find the leaf node containing the old key
        let leaf_page_id =
            TreeOperations::find_leaf_for_key(&self.storage_manager, key, &root_page).await?;
        let mut leaf_node = (*self.storage_manager.read_page(leaf_page_id).await?).clone();

        // Find the exact position of the old key
        match leaf_node.keys.binary_search(&key) {
            Ok(pos) => {
                // Simple update - key stays the same, just update the data
                leaf_node.values[pos] = row;
                leaf_node.is_dirty = true;
                self.storage_manager.write_page(&leaf_node).await?;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
}
