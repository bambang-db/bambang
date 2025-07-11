use std::sync::{Arc, Mutex};

use shared_types::Row;

use crate::{
    common::StorageError,
    manager::Manager,
    operator::{
        delete::{DeleteOperation, DeleteOptions, DeleteResult},
        insert::InsertOperation,
        print::TreePrinter,
        scan::{ScanOperation, ScanOptions, ScanResult},
        update::UpdateOperation,
    },
};

pub struct Executor {
    pub storage_manager: Arc<Manager>,
    pub root_page_id: Arc<Mutex<u64>>,
    pub max_workers: usize,
    pub batch_size: usize,

    // Operations
    insert_op: InsertOperation,
    scan_op: ScanOperation,
    update_op: UpdateOperation,
    delete_op: DeleteOperation,

    // Debug utilities
    tree_printer: TreePrinter,
}

impl Executor {
    pub fn new(storage_manager: Arc<Manager>, root_page_id: u64, max_workers: usize) -> Self {
        let insert_op = InsertOperation::new(storage_manager.clone());
        let scan_op = ScanOperation::new(storage_manager.clone(), max_workers, 1000);
        let update_op = UpdateOperation::new(storage_manager.clone());
        let delete_op = DeleteOperation::new(storage_manager.clone());
        let tree_printer = TreePrinter::new(storage_manager.clone());

        Self {
            storage_manager,
            root_page_id: Arc::new(Mutex::new(root_page_id)),
            max_workers,
            batch_size: 1000,
            insert_op,
            scan_op,
            update_op,
            delete_op,
            tree_printer,
        }
    }

    pub async fn scan(&self, options: ScanOptions) -> Result<ScanResult, StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();
        self.scan_op.execute(root_id, options).await
    }

    pub async fn insert(&self, row: Row) -> Result<u64, StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();

        let result = self.insert_op.execute(row, root_id).await?;

        if let Some(new_root_id) = result.new_root_id {
            *self.root_page_id.lock().unwrap() = new_root_id;
            Ok(new_root_id)
        } else {
            Ok(root_id)
        }
    }

    /// Insert multiple rows in a batch operation
    pub async fn insert_batch(&self, rows: Vec<Row>) -> Result<u64, StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();

        let result = self.insert_op.execute_batch(rows, root_id).await?;

        if let Some(new_root_id) = result.new_root_id {
            *self.root_page_id.lock().unwrap() = new_root_id;
            Ok(new_root_id)
        } else {
            Ok(root_id)
        }
    }

    pub async fn update(&self, key: u64, row: Row) -> Result<bool, StorageError> {
        let root_page_id = *self.root_page_id.lock().unwrap();
        self.update_op.execute(key, row, root_page_id).await
    }

    pub async fn delete(&self, options: DeleteOptions) -> Result<(), StorageError> {
        let result = self.delete_op.execute(options).await.unwrap();

        match result {
            DeleteResult::Truncated => {
                *self.root_page_id.lock().unwrap() = 1;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub async fn debug_print_tree(&self) -> Result<(), StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();
        self.tree_printer.print_tree(root_id).await
    }

    pub async fn calculate_height(&self, page_id: u64) -> Result<usize, StorageError> {
        let page = self.storage_manager.read_page(page_id).await?;
        if page.is_leaf {
            Ok(1)
        } else if !page.child_page_ids.is_empty() {
            let child_height = Box::pin(self.calculate_height(page.child_page_ids[0])).await?;
            Ok(child_height + 1)
        } else {
            Ok(1)
        }
    }
}
