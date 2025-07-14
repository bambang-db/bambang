use std::sync::Arc;

use shared_types::{Predicate, Schema};

use crate::{
    common::StorageError,
    manager::Manager,
    operator::compare::{evaluate_predicate_optimized, extract_predicate_column_indices},
};

pub struct DeleteOperation {
    storage_manager: Arc<Manager>,
}

#[derive(Debug, Clone)]
pub enum DeleteResult {
    Single(bool),
    Multiple(u64),
    Truncated,
}

#[derive(Debug, Clone)]
pub struct DeleteOptions {
    pub delete_type: DeleteType,
    pub schema: Option<Schema>,
    pub predicate: Option<Predicate>,
}

#[derive(Debug, Clone)]
pub enum DeleteType {
    ByPredicate,
    Truncate,
}

impl DeleteOptions {
    pub fn by_predicate(schema: Schema, predicate: Predicate) -> Self {
        Self {
            delete_type: DeleteType::ByPredicate,
            schema: Some(schema),
            predicate: Some(predicate),
        }
    }

    pub fn truncate() -> Self {
        Self {
            delete_type: DeleteType::Truncate,
            schema: None,
            predicate: None,
        }
    }
}

impl DeleteOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(&self, options: DeleteOptions) -> Result<DeleteResult, StorageError> {
        match options.delete_type {
            DeleteType::ByPredicate => {
                let deleted_count = self.delete_by_predicate(options).await?;
                Ok(DeleteResult::Multiple(deleted_count))
            }

            DeleteType::Truncate => {
                self.truncate().await?;
                Ok(DeleteResult::Truncated)
            }
        }
    }

    async fn delete_by_predicate(&self, options: DeleteOptions) -> Result<u64, StorageError> {
        let schema = options.schema.ok_or(StorageError::InvalidInput(
            "Schema is required for predicate-based deletion".to_string(),
        ))?;
        let predicate = options.predicate.ok_or(StorageError::InvalidInput(
            "Predicate is required for predicate-based deletion".to_string(),
        ))?;

        let mut deleted_count = 0u64;

        // Get all leaf page IDs from the registry
        let leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;

        // Pre-compute predicate column indices for efficiency
        let predicate_column_indices = extract_predicate_column_indices(&predicate, &schema);

        for leaf_id in leaf_page_ids {
            // Read the leaf page
            let page_arc = match self.storage_manager.read_page(leaf_id).await {
                Ok(page) => page,
                Err(e) => {
                    eprintln!("Failed to read page {} during delete: {:?}", leaf_id, e);
                    continue;
                }
            };

            // Create a mutable copy of the page for modification
            let mut leaf_page = (*page_arc).clone();

            // Track rows to delete (indices in reverse order to avoid index shifting)
            let mut rows_to_delete = Vec::new();

            // Evaluate predicate for each row
            for (row_index, row) in leaf_page.values.iter().enumerate() {
                if evaluate_predicate_optimized(
                    &predicate,
                    row,
                    &schema,
                    &Some(predicate_column_indices.clone()),
                ) {
                    rows_to_delete.push(row_index);
                }
            }

            // Check if there are rows to delete before modifying
            if !rows_to_delete.is_empty() {
                // Delete rows in reverse order to maintain correct indices
                rows_to_delete.reverse();
                for row_index in rows_to_delete {
                    leaf_page.values.remove(row_index);
                    deleted_count += 1;
                }

                leaf_page.is_dirty = true;
                self.storage_manager.write_page(&leaf_page).await?;
            }
        }

        Ok(deleted_count)
    }
    async fn get_next_leaf_id(&self, current_leaf_id: u64) -> Result<Option<u64>, StorageError> {
        // Helper method to get next leaf ID in case of read errors
        let leaf_page = self.storage_manager.read_page(current_leaf_id).await?;
        Ok(leaf_page.next_leaf_page_id)
    }

    pub async fn truncate(&self) -> Result<(), StorageError> {
        self.storage_manager.truncate().await?;
        Ok(())
    }
}

// Additional helper function for batch deletion optimization
impl DeleteOperation {
    /// Optimized batch delete for large deletions
    pub async fn delete_batch_by_predicate(
        &self,
        options: DeleteOptions,
        batch_size: usize,
    ) -> Result<u64, StorageError> {
        let schema = options.schema.ok_or(StorageError::InvalidInput(
            "Schema is required for predicate-based deletion".to_string(),
        ))?;
        let predicate = options.predicate.ok_or(StorageError::InvalidInput(
            "Predicate is required for predicate-based deletion".to_string(),
        ))?;

        let mut total_deleted = 0u64;

        // Get all leaf page IDs from the registry
        let leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;

        // Pre-compute predicate column indices for efficiency
        let predicate_column_indices = extract_predicate_column_indices(&predicate, &schema);

        for leaf_id in leaf_page_ids {
            let page_arc = self.storage_manager.read_page(leaf_id).await?;
            let mut leaf_page = (*page_arc).clone();
            let mut batch_deleted = 0;

            // Process in batches to avoid memory issues with large pages
            let mut processed = 0;
            while processed < leaf_page.values.len() && batch_deleted < batch_size {
                let mut rows_to_delete = Vec::new();

                // Process up to batch_size rows
                let end_idx = std::cmp::min(processed + batch_size, leaf_page.values.len());
                for row_index in processed..end_idx {
                    if evaluate_predicate_optimized(
                        &predicate,
                        &leaf_page.values[row_index],
                        &schema,
                        &Some(predicate_column_indices.clone()),
                    ) {
                        rows_to_delete.push(row_index);
                    }
                }

                // Delete rows in reverse order
                rows_to_delete.reverse();
                for row_index in rows_to_delete {
                    leaf_page.values.remove(row_index);
                    batch_deleted += 1;
                    total_deleted += 1;
                }

                processed = end_idx;
            }

            // Write the modified page back if any rows were deleted
            if batch_deleted > 0 {
                leaf_page.is_dirty = true;
                self.storage_manager.write_page(&leaf_page).await?;
            }
        }

        Ok(total_deleted)
    }
}
