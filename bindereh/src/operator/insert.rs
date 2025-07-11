use std::sync::Arc;

use shared_types::Row;

use crate::{
    common::{MAX_KEYS_PER_NODE, StorageError},
    manager::Manager,
    operator::tree::{SplitResult, TreeOperations},
};

pub struct InsertOperation {
    storage_manager: Arc<Manager>,
}

#[derive(Debug)]
pub struct InsertResult {
    pub new_root_id: Option<u64>,
    pub success: bool,
}

impl InsertOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    /// Execute the insert operation and return the result with potential new root ID
    pub async fn execute(&self, row: Row, root_page_id: u64) -> Result<InsertResult, StorageError> {
        let root_page = self.storage_manager.read_page(root_page_id).await?;

        // Find the appropriate leaf node for insertion
        let leaf_page_id =
            TreeOperations::find_leaf_for_key(&self.storage_manager, row.id, &root_page).await?;
        let leaf_page_arc = self.storage_manager.read_page(leaf_page_id).await?;

        // Clone the page data to make it mutable
        let mut leaf_page_data = (*leaf_page_arc).clone();

        // Check if key already exists (prevent duplicates)
        if let Ok(_) = leaf_page_data.keys.binary_search(&row.id) {
            return Err(StorageError::DuplicateKey(format!(
                "Key {} already exists",
                row.id
            )));
        }

        // Find position for insertion
        let insert_pos = leaf_page_data
            .keys
            .binary_search(&row.id)
            .unwrap_or_else(|pos| pos);

        leaf_page_data.keys.insert(insert_pos, row.id);
        leaf_page_data.values.insert(insert_pos, row);
        leaf_page_data.is_dirty = true;

        let mut new_root_id = None;

        // Check if leaf node needs to be split
        if leaf_page_data.keys.len() > MAX_KEYS_PER_NODE {
            let split_result =
                TreeOperations::split_leaf_node(&self.storage_manager, &mut leaf_page_data).await?;

            match split_result {
                SplitResult::NewRoot(root_id) => {
                    // A new root was created, update the root ID
                    new_root_id = Some(root_id);
                }
                SplitResult::PromotedKey(promoted_key, new_right_child_id) => {
                    // Insert the promoted key into the parent
                    if let Some(root_id) = TreeOperations::insert_into_parent(
                        &self.storage_manager,
                        leaf_page_data.parent_page_id,
                        promoted_key,
                        new_right_child_id,
                    )
                    .await?
                    {
                        // A new root was created during parent insertion
                        new_root_id = Some(root_id);
                    }
                }
            }
        }

        // Write back the modified leaf page to storage
        self.storage_manager.write_page(&leaf_page_data).await?;

        Ok(InsertResult {
            new_root_id,
            success: true,
        })
    }

    /// Insert multiple rows in a batch operation
    pub async fn execute_batch(
        &self,
        rows: Vec<Row>,
        root_page_id: u64,
    ) -> Result<InsertResult, StorageError> {
        let mut current_root_id = root_page_id;
        let mut final_new_root_id = None;

        for row in rows {
            let result = self.execute(row, current_root_id).await?;

            // Update the current root ID if a new root was created
            if let Some(new_root) = result.new_root_id {
                current_root_id = new_root;
                final_new_root_id = Some(new_root);
            }
        }

        Ok(InsertResult {
            new_root_id: final_new_root_id,
            success: true,
        })
    }
}
