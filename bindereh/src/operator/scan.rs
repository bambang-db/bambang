use std::sync::Arc;

use crate::{
    common::StorageError, manager::Manager, operator::tree::TreeOperations, page::Row, value::Value,
};

// Enhanced filtering with column-level predicates
#[derive(Debug, Clone)]
pub enum Predicate {
    // Column-based predicates
    ColumnEquals {
        column: String,
        value: Value,
    },
    ColumnNotEquals {
        column: String,
        value: Value,
    },
    ColumnGreaterThan {
        column: String,
        value: Value,
    },
    ColumnLessThan {
        column: String,
        value: Value,
    },
    ColumnGreaterThanOrEqual {
        column: String,
        value: Value,
    },
    ColumnLessThanOrEqual {
        column: String,
        value: Value,
    },
    ColumnIn {
        column: String,
        values: Vec<Value>,
    },
    ColumnNotIn {
        column: String,
        values: Vec<Value>,
    },
    ColumnIsNull {
        column: String,
    },
    ColumnIsNotNull {
        column: String,
    },
    ColumnLike {
        column: String,
        pattern: String,
    },
    ColumnBetween {
        column: String,
        start: Value,
        end: Value,
    },

    // Logical operators
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub predicate: Option<Predicate>,
    pub projection: Option<Vec<String>>, // Column names to select
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub parallel: bool,
    pub order_by: Option<Vec<OrderBy>>,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            predicate: None,
            projection: None,
            limit: None,
            offset: None,
            parallel: true,
            order_by: None,
        }
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub rows: Vec<Row>,
    pub total_scanned: usize,
    pub pages_read: usize,
    pub filtered_count: usize,
    // pub schema: Option<Schema>, // Schema for the result set
}

pub struct ScanOperation {
    storage_manager: Arc<Manager>,
    max_workers: usize,
    batch_size: usize,
}

impl ScanOperation {
    pub fn new(storage_manager: Arc<Manager>, max_workers: usize, batch_size: usize) -> Self {
        Self {
            storage_manager,
            max_workers,
            batch_size,
        }
    }

    pub async fn execute(
        &self,
        root_page_id: u64,
        options: ScanOptions,
    ) -> Result<ScanResult, StorageError> {
        // Get the leftmost leaf page (start of sequential scan)
        let leftmost_leaf_id =
            TreeOperations::find_leftmost_leaf(&self.storage_manager, root_page_id)
                .await?
                .expect("Cannot get leftmost_leaf_id");

        self.sequential_scan(leftmost_leaf_id, options).await
    }

    async fn sequential_scan(
        &self,
        start_leaf_id: u64,
        options: ScanOptions,
    ) -> Result<ScanResult, StorageError> {
        let mut result_rows: Vec<Row> = Vec::new();
        let mut pages_read = 0;
        let mut total_scanned = 0;
        let mut current_leaf_id = Some(start_leaf_id);
        let mut rows_processed = 0;

        while let Some(leaf_id) = current_leaf_id {
            let leaf_page = self.storage_manager.read_page(leaf_id).await?;
            pages_read += 1;

            for row in &leaf_page.values {
                // TODO: evaluate your row, filter, etc..
                result_rows.push(row.clone());
                rows_processed += 1;
            }

            // Check limit
            if let Some(limit) = options.limit {
                if rows_processed >= limit {
                    break;
                }
            }

            // Move to next leaf
            current_leaf_id = leaf_page.next_leaf_page_id;
        }

        Ok(ScanResult {
            rows: result_rows,
            total_scanned: 0,
            pages_read: 0,
            filtered_count: 0,
        })
    }
}
