use std::sync::Arc;
use shared_types::{Predicate, Schema};
use crate::{
    common::StorageError,
    manager::Manager,
    operator::compare::{evaluate_predicate_optimized, extract_predicate_column_indices},
    operator::tree::{DeleteResult as TreeDeleteResult, TreeOperations},
};

pub struct DeleteOperation {
    storage_manager: Arc<Manager>,
}

#[derive(Debug, Clone)]
pub enum DeleteResult {
    Single(bool),
    Multiple { deleted_count: u64, new_root_id: Option<u64> },
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
                let (deleted_count, new_root_id) = self.delete_by_predicate_with_tree_maintenance(options).await?;
                Ok(DeleteResult::Multiple { deleted_count, new_root_id })
            }
            DeleteType::Truncate => {
                self.truncate().await?;
                Ok(DeleteResult::Truncated)
            }
        }
    }

    async fn delete_by_predicate_with_tree_maintenance(
        &self,
        options: DeleteOptions,
    ) -> Result<(u64, Option<u64>), StorageError> {
        let schema = options.schema.ok_or(StorageError::InvalidInput("Schema is required for predicate-based deletion".to_string()))?;
        let predicate = options.predicate.ok_or(StorageError::InvalidInput("Predicate is required for predicate-based deletion".to_string()))?;
        let mut deleted_count = 0u64;
        let mut new_root_id: Option<u64> = None;
        let leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;
        let predicate_column_indices = extract_predicate_column_indices(&predicate, &schema);

        for leaf_id in leaf_page_ids {
            let page_arc = match self.storage_manager.read_page(leaf_id).await {
                Ok(page) => page,
                Err(_) => continue,
            };
            let leaf_page = (*page_arc).clone();
            let mut rows_to_delete = Vec::new();

            for (row_index, row) in leaf_page.values.iter().enumerate() {
                if evaluate_predicate_optimized(&predicate, row, &schema, &Some(predicate_column_indices.clone())) {
                    rows_to_delete.push(row_index);
                }
            }

            if !rows_to_delete.is_empty() {
                deleted_count += rows_to_delete.len() as u64;
                let delete_result = TreeOperations::delete_entries_from_leaf(&self.storage_manager, leaf_id, rows_to_delete).await?;

                match delete_result {
                    TreeDeleteResult::Underflow => {
                        if let Some(updated_root) = TreeOperations::handle_underflow(&self.storage_manager, leaf_id).await? {
                            new_root_id = Some(updated_root);
                        }
                    }
                    TreeDeleteResult::RootDeleted => {
                        new_root_id = None; // Root was deleted, tree is now empty
                    }
                    _ => {}
                }
            }
        }

        Ok((deleted_count, new_root_id))
    }

    pub async fn delete_batch_by_predicate_with_tree_maintenance(
        &self,
        options: DeleteOptions,
        batch_size: usize,
    ) -> Result<(u64, Option<u64>), StorageError> {
        let schema = options.schema.ok_or(StorageError::InvalidInput("Schema is required for predicate-based deletion".to_string()))?;
        let predicate = options.predicate.ok_or(StorageError::InvalidInput("Predicate is required for predicate-based deletion".to_string()))?;
        let mut total_deleted = 0u64;
        let mut pages_to_rebalance = Vec::new();
        let mut new_root_id: Option<u64> = None;
        let leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;
        let predicate_column_indices = extract_predicate_column_indices(&predicate, &schema);

        for leaf_id in leaf_page_ids {
            let page_arc = self.storage_manager.read_page(leaf_id).await?;
            let leaf_page = (*page_arc).clone();
            let mut batch_deleted = 0;
            let mut all_indices_to_delete = Vec::new();
            let mut processed = 0;

            while processed < leaf_page.values.len() && batch_deleted < batch_size {
                let mut rows_to_delete = Vec::new();
                let end_idx = std::cmp::min(processed + batch_size, leaf_page.values.len());

                for row_index in processed..end_idx {
                    if evaluate_predicate_optimized(&predicate, &leaf_page.values[row_index], &schema, &Some(predicate_column_indices.clone())) {
                        rows_to_delete.push(row_index);
                    }
                }

                all_indices_to_delete.extend(rows_to_delete);
                processed = end_idx;
            }

            if !all_indices_to_delete.is_empty() {
                batch_deleted = all_indices_to_delete.len();
                total_deleted += batch_deleted as u64;
                let delete_result = TreeOperations::delete_entries_from_leaf(&self.storage_manager, leaf_id, all_indices_to_delete).await?;

                match delete_result {
                    TreeDeleteResult::Underflow => pages_to_rebalance.push(leaf_id),
                    TreeDeleteResult::RootDeleted => {
                        new_root_id = None; // Root was deleted
                    }
                    _ => {}
                }
            }
        }

        for page_id in pages_to_rebalance {
            if let Some(updated_root) = TreeOperations::handle_underflow(&self.storage_manager, page_id).await? {
                new_root_id = Some(updated_root);
            }
        }

        Ok((total_deleted, new_root_id))
    }

    pub async fn truncate(&self) -> Result<(), StorageError> {
        self.storage_manager.truncate().await?;
        Ok(())
    }

    pub async fn delete_batch_by_predicate(
        &self,
        options: DeleteOptions,
        batch_size: usize,
    ) -> Result<(u64, Option<u64>), StorageError> {
        self.delete_batch_by_predicate_with_tree_maintenance(options, batch_size).await
    }
}

pub fn validate_delete_options(options: &DeleteOptions) -> Result<(), StorageError> {
    match options.delete_type {
        DeleteType::ByPredicate => {
            if options.schema.is_none() || options.predicate.is_none() {
                return Err(StorageError::InvalidInput("Schema and predicate are required for predicate-based deletion".to_string()));
            }
        }
        DeleteType::Truncate => {}
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct DeleteStats {
    pub total_deleted: u64,
    pub pages_modified: u64,
    pub pages_merged: u64,
    pub tree_height_changed: bool,
    pub operation_duration_ms: u64,
    pub new_root_id: Option<u64>,
}

impl DeleteStats {
    pub fn new() -> Self {
        Self {
            total_deleted: 0,
            pages_modified: 0,
            pages_merged: 0,
            tree_height_changed: false,
            operation_duration_ms: 0,
            new_root_id: None,
        }
    }
}