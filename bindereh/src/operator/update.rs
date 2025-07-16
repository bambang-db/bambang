use crate::{
    common::StorageError,
    manager::Manager,
    operator::compare::{evaluate_predicate_optimized, extract_predicate_column_indices},
};
use shared_types::{Predicate, Row, Schema};
use std::sync::Arc;

pub struct UpdateOperation {
    storage_manager: Arc<Manager>,
}

#[derive(Debug, Clone)]
pub struct UpdateOptions {
    pub schema: Schema,
    pub predicate: Predicate,
    pub new_values: Row,
}

impl UpdateOptions {
    pub fn new(schema: Schema, predicate: Predicate, new_values: Row) -> Self {
        Self {
            schema,
            predicate,
            new_values,
        }
    }
}

impl UpdateOperation {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn execute(&self, options: UpdateOptions) -> Result<u64, StorageError> {
        let mut updated_count = 0u64;
        let leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;
        let predicate_column_indices =
            extract_predicate_column_indices(&options.predicate, &options.schema);

        for leaf_id in leaf_page_ids {
            let page_arc: Arc<crate::page::Page> =
                match self.storage_manager.read_page(leaf_id).await {
                    Ok(page) => page,
                    Err(_) => continue,
                };
            let mut leaf_page = (*page_arc).clone();
            let mut page_modified = false;

            for (row_index, row) in leaf_page.values.iter_mut().enumerate() {
                if evaluate_predicate_optimized(
                    &options.predicate,
                    row,
                    &options.schema,
                    &Some(predicate_column_indices.clone()),
                ) {
                    *row = options.new_values.clone();
                    updated_count += 1;
                    page_modified = true;
                }
            }

            if page_modified {
                leaf_page.is_dirty = true;
                self.storage_manager.write_page(&leaf_page).await?;
            }
        }

        Ok(updated_count)
    }
}
