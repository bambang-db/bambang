use std::sync::{Arc, Mutex};

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
    pub root_page_id: Arc<Mutex<u64>>,
    pub max_workers: usize,
    pub batch_size: usize,
}

impl Executor {
    pub fn new(storage_manager: Arc<Manager>, root_page_id: u64, max_workers: usize) -> Self {
        Self {
            storage_manager,
            root_page_id: Arc::new(Mutex::new(root_page_id)),
            max_workers,
            batch_size: 1000,
        }
    }

    pub async fn scan() {}

    pub async fn parallel() {}

    pub async fn insert(&self, row: Row) -> Result<u64, StorageError> {
        // Get current root id
        let root_id = *self.root_page_id.lock().unwrap();

        // when in the beginning it will just an empty Page / Node
        let root_page = self.storage_manager.read_page(root_id).await?;

        // Find the appropriate leaf node for insertion
        let leaf_page_id = self.find_leaf_for_key(row.id, &root_page).await?;
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

    // Recursive-ly traverse until it reach find leaf for insertion, return page_id
    async fn find_leaf_for_key(&self, key: u64, node: &Page) -> Result<u64, StorageError> {
        // if current node already a leaf just return the page_id
        if node.is_leaf {
            return Ok(node.page_id);
        }

        let mut child_index = 0;

        // traverse where the given key position
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

    pub async fn destroy() {}

    // Utility function to get tree height (for debugging)
    pub async fn get_tree_height(&self) -> Result<usize, StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();
        self.calculate_height(root_id).await
    }

    async fn calculate_height(&self, page_id: u64) -> Result<usize, StorageError> {
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

    // Debug function to print tree structure
    pub async fn debug_print_tree(&self) -> Result<(), StorageError> {
        let root_id = *self.root_page_id.lock().unwrap();
        println!("\n┌─────────────────────────────────────────────────────────────┐");
        println!("│                        B+ Tree Structure                    │");
        println!("└─────────────────────────────────────────────────────────────┘");
        self.print_subtree(root_id, 0, true, String::new()).await?;

        // Print leaf level connections
        println!("\nLeaf Level Connections:");
        self.print_leaf_connections(root_id).await?;
        Ok(())
    }

    async fn print_subtree(
        &self,
        page_id: u64,
        depth: usize,
        is_last: bool,
        prefix: String,
    ) -> Result<(), StorageError> {
        let page = self.storage_manager.read_page(page_id).await?;

        // Print current node
        let connector = if depth == 0 {
            "ROOT"
        } else if is_last {
            "└── "
        } else {
            "├── "
        };

        let node_type = if page.is_leaf { " LEAF" } else { " INTERNAL" };
        let keys_str = page
            .keys
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        println!(
            "{}{}{} [{}] │ {} │",
            prefix, connector, node_type, page_id, keys_str
        );

        // Print children for internal nodes
        if !page.is_leaf {
            let new_prefix = if depth == 0 {
                String::new()
            } else if is_last {
                format!("{}    ", prefix)
            } else {
                format!("{}│   ", prefix)
            };

            for (i, &child_id) in page.child_page_ids.iter().enumerate() {
                let is_last_child = i == page.child_page_ids.len() - 1;
                Box::pin(self.print_subtree(
                    child_id,
                    depth + 1,
                    is_last_child,
                    new_prefix.clone(),
                ))
                .await?;
            }
        }

        Ok(())
    }

    async fn print_leaf_connections(&self, page_id: u64) -> Result<(), StorageError> {
        let mut current_leaf = self.find_leftmost_leaf(page_id).await?;
        let mut leaf_chain = Vec::new();

        // Collect all leaf pages in order
        while let Some(leaf_id) = current_leaf {
            let page = self.storage_manager.read_page(leaf_id).await?;
            leaf_chain.push((leaf_id, page.keys.clone()));
            current_leaf = page.next_leaf_page_id;
        }

        // Print the leaf chain
        if !leaf_chain.is_empty() {
            print!("🍃 ");
            for (i, (page_id, keys)) in leaf_chain.iter().enumerate() {
                let keys_str = keys
                    .iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                print!("[{}:{}]", page_id, keys_str);

                if i < leaf_chain.len() - 1 {
                    print!(" → ");
                }
            }
            println!(" → NULL");
        }

        Ok(())
    }

    async fn find_leftmost_leaf(&self, page_id: u64) -> Result<Option<u64>, StorageError> {
        let page = self.storage_manager.read_page(page_id).await?;

        if page.is_leaf {
            Ok(Some(page_id))
        } else if !page.child_page_ids.is_empty() {
            Box::pin(self.find_leftmost_leaf(page.child_page_ids[0])).await
        } else {
            Ok(None)
        }
    }
}
