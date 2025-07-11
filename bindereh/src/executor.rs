use std::sync::{Arc, Mutex};

use crate::{
    common::{MAX_KEYS_PER_NODE, StorageError},
    manager::Manager,
    operator::{
        delete::{DeleteOperation, DeleteOptions, DeleteResult},
        insert::InsertOperation,
        print::TreePrinter,
        scan::{ScanOperation, ScanOptions, ScanResult},
        tree::TreeOperations,
        update::UpdateOperation,
    },
    page::{Page, Row},
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
        // Get current root id
        let root_id = *self.root_page_id.lock().unwrap();

        // when in the beginning it will just an empty Page / Node
        let root_page = self.storage_manager.read_page(root_id).await?;

        // Find the appropriate leaf node for insertion
        let leaf_page_id =
            TreeOperations::find_leaf_for_key(&self.storage_manager, row.id, &root_page).await?;
        let leaf_page_arc = self.storage_manager.read_page(leaf_page_id).await?;

        // Clone the page data to make it mutable (Arc<Page> -> Page)
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
            let promoted_key = self.split_leaf_node(&mut leaf_page_data).await?;

            // If we have a promoted key, we need to insert it into the parent
            if let Some(key) = promoted_key {
                self.insert_into_parent(
                    leaf_page_data.parent_page_id,
                    key,
                    leaf_page_data.next_leaf_page_id.unwrap(),
                )
                .await?;
            }
        }

        // Write back to storage
        self.storage_manager.write_page(&leaf_page_data).await?;

        Ok(*self.root_page_id.lock().unwrap())
    }

    // Split a leaf node, will return promoted key, split leaf to two-node and create a new root
    async fn split_leaf_node(&self, node: &mut Page) -> Result<Option<u64>, StorageError> {
        let mid_point = node.keys.len() / 2;

        // Create new leaf node for the right half, will be the next_page_id
        let new_page_id = self.storage_manager.allocate_page().await;

        let new_node = Page {
            page_id: new_page_id,
            is_leaf: true,
            parent_page_id: node.parent_page_id,
            keys: node.keys.split_off(mid_point),
            values: node.values.split_off(mid_point),
            child_page_ids: Vec::new(),
            next_leaf_page_id: node.next_leaf_page_id,
            is_dirty: true,
        };

        // The promoted key is the first key of the new (right) node
        let promoted_key = new_node.keys[0];

        // Update linking
        node.next_leaf_page_id = Some(new_page_id);

        // Write the new node to storage
        self.storage_manager.write_page(&new_node).await?;

        // If this is the root node, create a new root
        if node.parent_page_id.is_none() {
            self.create_new_root(node.page_id, promoted_key, new_page_id)
                .await?;
            return Ok(None); // No need to propagate further
        }

        Ok(Some(promoted_key))
    }

    // Create a new root node when the current root splits
    async fn create_new_root(
        &self,
        left_child_id: u64,
        key: u64,
        right_child_id: u64,
    ) -> Result<(), StorageError> {
        let new_root_id = self.storage_manager.allocate_page().await;
        let new_root = Page {
            page_id: new_root_id,
            is_leaf: false,
            parent_page_id: None,
            keys: vec![key],
            values: Vec::new(),
            child_page_ids: vec![left_child_id, right_child_id],
            next_leaf_page_id: None,
            is_dirty: true,
        };

        // Update parent pointers for the children
        let mut left_child = (*self.storage_manager.read_page(left_child_id).await?).clone();
        left_child.parent_page_id = Some(new_root_id);
        left_child.is_dirty = true;
        self.storage_manager.write_page(&left_child).await?;

        let mut right_child = (*self.storage_manager.read_page(right_child_id).await?).clone();
        right_child.parent_page_id = Some(new_root_id);
        right_child.is_dirty = true;
        self.storage_manager.write_page(&right_child).await?;

        // Write the new root
        self.storage_manager.write_page(&new_root).await?;

        // Update the root_page_id in the executor
        *self.root_page_id.lock().unwrap() = new_root_id;

        Ok(())
    }

    // Split an internal node when it becomes too full
    async fn split_internal_node(&self, node: &mut Page) -> Result<Option<u64>, StorageError> {
        let mid_point = node.keys.len() / 2;
        let promoted_key = node.keys[mid_point];

        // Create new internal node for the right half
        let new_page_id = self.storage_manager.allocate_page().await;
        let new_node = Page {
            page_id: new_page_id,
            is_leaf: false,
            parent_page_id: node.parent_page_id,
            keys: node.keys.split_off(mid_point + 1), // Skip the promoted key
            values: Vec::new(),
            child_page_ids: node.child_page_ids.split_off(mid_point + 1),
            next_leaf_page_id: None,
            is_dirty: true,
        };

        // Remove the promoted key from the original node
        node.keys.pop(); // Remove the promoted key
        node.is_dirty = true;

        // Update parent pointers for moved children
        for &child_id in &new_node.child_page_ids {
            let mut child = (*self.storage_manager.read_page(child_id).await?).clone();
            child.parent_page_id = Some(new_page_id);
            child.is_dirty = true;
            self.storage_manager.write_page(&child).await?;
        }

        // Write the new node to storage
        self.storage_manager.write_page(&new_node).await?;

        // If this is the root node, create a new root
        if node.parent_page_id.is_none() {
            self.create_new_root(node.page_id, promoted_key, new_page_id)
                .await?;
            return Ok(None); // No need to propagate further
        }

        Ok(Some(promoted_key))
    }

    // Insert a key into the parent node
    async fn insert_into_parent(
        &self,
        parent_page_id: Option<u64>,
        key: u64,
        right_child_id: u64,
    ) -> Result<(), StorageError> {
        if let Some(parent_id) = parent_page_id {
            let mut parent_node = (*self.storage_manager.read_page(parent_id).await?).clone();

            // Find insertion position
            let insert_pos = parent_node
                .keys
                .binary_search(&key)
                .unwrap_or_else(|pos| pos);

            // Insert the key and corresponding child pointer
            parent_node.keys.insert(insert_pos, key);
            parent_node
                .child_page_ids
                .insert(insert_pos + 1, right_child_id);
            parent_node.is_dirty = true;

            // Check if parent needs to be split
            if parent_node.keys.len() > MAX_KEYS_PER_NODE {
                let promoted_key = self.split_internal_node(&mut parent_node).await?;

                // If we have a promoted key, recursively insert into grandparent
                if let Some(promoted) = promoted_key {
                    Box::pin(self.insert_into_parent(
                        parent_node.parent_page_id,
                        promoted,
                        parent_node.child_page_ids.last().cloned().unwrap(),
                    ))
                    .await?;
                }
            }

            // Write back to storage
            self.storage_manager.write_page(&parent_node).await?;
        }

        Ok(())
    }

    pub async fn update() {}

    pub async fn delete(&self, options: DeleteOptions) -> Result<(), StorageError> {
        let result = self.delete_op.execute(options).await.unwrap();

        match result {
            DeleteResult::Truncated => {
                // Reset root_page_id to 1
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
