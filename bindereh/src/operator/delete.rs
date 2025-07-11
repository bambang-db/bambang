use std::sync::Arc;

use crate::{common::StorageError, manager::Manager, operator::tree::TreeOperations};

pub struct DeleteOperation {
    storage_manager: Arc<Manager>,
}

impl DeleteOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(&self, key: u64, root_page_id: u64) -> Result<bool, StorageError> {
        let root_page = self.storage_manager.read_page(root_page_id).await?;
        let leaf_page_id =
            TreeOperations::find_leaf_for_key(&self.storage_manager, key, &root_page).await?;
        let leaf_page_arc = self.storage_manager.read_page(leaf_page_id).await?;

        let mut leaf_page_data = (*leaf_page_arc).clone();

        // Find and remove the key
        if let Ok(pos) = leaf_page_data.keys.binary_search(&key) {
            leaf_page_data.keys.remove(pos);
            leaf_page_data.values.remove(pos);
            leaf_page_data.is_dirty = true;

            // TODO: Handle underflow and tree rebalancing

            // Write back to storage
            self.storage_manager.write_page(&leaf_page_data).await?;
            Ok(true)
        } else {
            Ok(false) // Key not found
        }
    }
}
