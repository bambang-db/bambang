use std::sync::Arc;

use crate::{
    common::{MAX_KEYS_PER_NODE, StorageError},
    manager::Manager,
    operator::tree::TreeOperations,
    page::Row,
};

pub struct InsertOperation {
    storage_manager: Arc<Manager>,
}

impl InsertOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(&self, row: Row, root_page_id: u64) -> Result<u64, StorageError> {
        let root_page = self.storage_manager.read_page(root_page_id).await?;

        // Find the appropriate leaf node for insertion
        let leaf_page_id =
            TreeOperations::find_leaf_for_key(&self.storage_manager, row.id, &root_page).await?;
        let leaf_page_arc = self.storage_manager.read_page(leaf_page_id).await?;

        // Clone the page data to make it mutable
        let mut leaf_page_data = (*leaf_page_arc).clone();

        // Find position for insertion
        let insert_pos = leaf_page_data
            .keys
            .binary_search(&row.id)
            .unwrap_or_else(|pos| pos);

        leaf_page_data.keys.insert(insert_pos, row.id);
        leaf_page_data.values.insert(insert_pos, row);
        leaf_page_data.is_dirty = true;

        // Check if leaf node needs to be split
        if leaf_page_data.keys.len() > MAX_KEYS_PER_NODE {
            let promoted_key =
                TreeOperations::split_leaf_node(&self.storage_manager, &mut leaf_page_data).await?;

            // If we have a promoted key, insert it into the parent
            if let Some(key) = promoted_key {
                TreeOperations::insert_into_parent(
                    &self.storage_manager,
                    leaf_page_data.parent_page_id,
                    key,
                    leaf_page_data.next_leaf_page_id.unwrap(),
                )
                .await?;
            }
        }

        // Write back to storage
        self.storage_manager.write_page(&leaf_page_data).await?;

        Ok(root_page_id)
    }
}
