use std::sync::Arc;

use crate::{
    common::{MAX_KEYS_PER_NODE, StorageError},
    manager::Manager,
    page::{Page, Row},
};

// Scan condition for filtering
#[derive(Debug, Clone, PartialEq)]
pub enum ScanCondition {
    Equal(u64),       // key = value
    Range(u64, u64),  // key BETWEEN start AND end
    GreaterThan(u64), // key > value
    LessThan(u64),    // key < value
    All,              // no condition (full scan)
}

pub struct Executor {
    pub storage_manager: Arc<Manager>,
    pub root_page_id: u64,
    pub max_workers: usize,
    pub batch_size: usize,
}

impl Executor {
    pub fn new(storage_manager: Arc<Manager>, root_page_id: u64, max_workers: usize) -> Self {
        Self {
            storage_manager,
            root_page_id,
            max_workers,
            batch_size: 1000,
        }
    }

    pub async fn scan() {}

    pub async fn parallel() {}

    // Insert a new row into the B+ tree
    pub async fn insert(&self, row: Row) -> Result<(), StorageError> {
        let root_page = self.storage_manager.read_page(self.root_page_id).await?;

        // Find the appropriate leaf node for insertion
        let leaf_page_id = self.find_leaf_for_key(row.id, &root_page).await?;
        let mut leaf_node = (*self.storage_manager.read_page(leaf_page_id).await?).clone();

        // Insert into leaf node
        let insert_pos = leaf_node
            .keys
            .binary_search(&row.id)
            .unwrap_or_else(|pos| pos);

        leaf_node.keys.insert(insert_pos, row.id);
        leaf_node.values.insert(insert_pos, row);
        leaf_node.is_dirty = true;

        // Check if leaf node needs to be split
        if leaf_node.keys.len() > MAX_KEYS_PER_NODE {
            self.split_leaf_node(&mut leaf_node).await?;
        }

        // Write back to storage
        self.storage_manager.write_page(&leaf_node).await?;

        Ok(())
    }

    // Find the leaf node that should contain the given key
    async fn find_leaf_for_key(&self, key: u64, node: &Page) -> Result<u64, StorageError> {
        if node.is_leaf {
            return Ok(node.page_id);
        }

        // Find the appropriate child
        let mut child_index = 0;
        for (i, &node_key) in node.keys.iter().enumerate() {
            if key < node_key {
                child_index = i;
                break;
            }
            child_index = i + 1;
        }

        let child_page_id = node.child_page_ids[child_index];
        let child_node = self.storage_manager.read_page(child_page_id).await?;

        Box::pin(self.find_leaf_for_key(key, &child_node)).await
    }

    // Split a leaf node when it becomes too full
    async fn split_leaf_node(&self, node: &mut Page) -> Result<(), StorageError> {
        let mid_point = node.keys.len() / 2;

        // Create new leaf node for the right half
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

        // Update linking
        node.next_leaf_page_id = Some(new_page_id);

        // Write the new node to storage
        self.storage_manager.write_page(&new_node).await?;

        // TODO: Update parent node to include new key (simplified for this example)

        Ok(())
    }

    pub async fn update() {}

    pub async fn destroy() {}
}
