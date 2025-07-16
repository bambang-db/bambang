use std::sync::Arc;
use crate::{
    common::{MAX_KEYS_PER_NODE, StorageError},
    manager::Manager,
    page::Page,
};
use shared_types::{Row, constant::MIN_KEYS_PER_NODE};

#[derive(Debug)]
pub enum SplitResult {
    NewRoot(u64),
    PromotedKey(u64, u64),
}

#[derive(Debug)]
pub enum DeleteResult {
    Success,
    Underflow,
    RootDeleted,
}

#[derive(Debug)]
pub enum MergeResult {
    Merged,
    Borrowed,
    RootUpdated(u64),
}

pub struct TreeOperations;

impl TreeOperations {
    // ========== SEARCH OPERATIONS ==========
    
    pub async fn find_leaf_for_key(
        storage_manager: &Arc<Manager>,
        key: u64,
        node: &Page,
    ) -> Result<u64, StorageError> {
        if node.is_leaf {
            return Ok(node.page_id);
        }
        let mut child_index = 0;
        for (i, &node_key) in node.keys.iter().enumerate() {
            if key < node_key {
                child_index = i;
                break;
            }
            child_index = i + 1;
        }
        let child_page_id = node.child_page_ids[child_index];
        let child_node = storage_manager.read_page(child_page_id).await?;
        Box::pin(Self::find_leaf_for_key(storage_manager, key, &child_node)).await
    }

    pub async fn find_leftmost_leaf(
        storage_manager: &Arc<Manager>,
        page_id: u64,
    ) -> Result<Option<u64>, StorageError> {
        let page = storage_manager.read_page(page_id).await?;
        if page.is_leaf {
            Ok(Some(page_id))
        } else if !page.child_page_ids.is_empty() {
            Box::pin(Self::find_leftmost_leaf(storage_manager, page.child_page_ids[0])).await
        } else {
            Ok(None)
        }
    }

    // ========== INSERT OPERATIONS ==========
    
    pub async fn split_leaf_node(
        storage_manager: &Arc<Manager>,
        node: &mut Page,
    ) -> Result<SplitResult, StorageError> {
        let mid_point = node.keys.len() / 2;
        let new_page_id = storage_manager.allocate_page().await;
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
        let promoted_key = new_node.keys[0];
        node.next_leaf_page_id = Some(new_page_id);
        storage_manager.write_page(&new_node).await?;
        storage_manager.register_leaf_page(new_page_id).await?;
        if node.parent_page_id.is_none() {
            let new_root_id = Self::create_new_root(storage_manager, node.page_id, promoted_key, new_page_id).await?;
            return Ok(SplitResult::NewRoot(new_root_id));
        }
        Ok(SplitResult::PromotedKey(promoted_key, new_page_id))
    }

    pub async fn create_new_root(
        storage_manager: &Arc<Manager>,
        left_child_id: u64,
        key: u64,
        right_child_id: u64,
    ) -> Result<u64, StorageError> {
        let new_root_id = storage_manager.allocate_page().await;
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
        let mut left_child = (*storage_manager.read_page(left_child_id).await?).clone();
        left_child.parent_page_id = Some(new_root_id);
        left_child.is_dirty = true;
        storage_manager.write_page(&left_child).await?;
        let mut right_child = (*storage_manager.read_page(right_child_id).await?).clone();
        right_child.parent_page_id = Some(new_root_id);
        right_child.is_dirty = true;
        storage_manager.write_page(&right_child).await?;
        storage_manager.write_page(&new_root).await?;
        Ok(new_root_id)
    }

    pub async fn insert_into_parent(
        storage_manager: &Arc<Manager>,
        parent_page_id: Option<u64>,
        key: u64,
        right_child_id: u64,
    ) -> Result<Option<u64>, StorageError> {
        if let Some(parent_id) = parent_page_id {
            let mut parent_node = (*storage_manager.read_page(parent_id).await?).clone();
            let insert_pos = parent_node.keys.binary_search(&key).unwrap_or_else(|pos| pos);
            parent_node.keys.insert(insert_pos, key);
            parent_node.child_page_ids.insert(insert_pos + 1, right_child_id);
            parent_node.is_dirty = true;
            if parent_node.keys.len() > MAX_KEYS_PER_NODE {
                let split_result = Self::split_internal_node(storage_manager, &mut parent_node).await?;
                match split_result {
                    SplitResult::NewRoot(new_root_id) => {
                        storage_manager.write_page(&parent_node).await?;
                        return Ok(Some(new_root_id));
                    }
                    SplitResult::PromotedKey(promoted_key, new_right_child_id) => {
                        storage_manager.write_page(&parent_node).await?;
                        return Box::pin(Self::insert_into_parent(
                            storage_manager,
                            parent_node.parent_page_id,
                            promoted_key,
                            new_right_child_id,
                        )).await;
                    }
                }
            }
            storage_manager.write_page(&parent_node).await?;
        }
        Ok(None)
    }

    pub async fn split_internal_node(
        storage_manager: &Arc<Manager>,
        node: &mut Page,
    ) -> Result<SplitResult, StorageError> {
        let mid_point = node.keys.len() / 2;
        let promoted_key = node.keys[mid_point];
        let new_page_id = storage_manager.allocate_page().await;
        let new_node = Page {
            page_id: new_page_id,
            is_leaf: false,
            parent_page_id: node.parent_page_id,
            keys: node.keys.split_off(mid_point + 1),
            values: Vec::new(),
            child_page_ids: node.child_page_ids.split_off(mid_point + 1),
            next_leaf_page_id: None,
            is_dirty: true,
        };
        node.keys.pop();
        node.is_dirty = true;
        for &child_id in &new_node.child_page_ids {
            let mut child = (*storage_manager.read_page(child_id).await?).clone();
            child.parent_page_id = Some(new_page_id);
            child.is_dirty = true;
            storage_manager.write_page(&child).await?;
        }
        storage_manager.write_page(&new_node).await?;
        if node.parent_page_id.is_none() {
            let new_root_id = Self::create_new_root(storage_manager, node.page_id, promoted_key, new_page_id).await?;
            return Ok(SplitResult::NewRoot(new_root_id));
        }
        Ok(SplitResult::PromotedKey(promoted_key, new_page_id))
    }

    // ========== DELETE OPERATIONS ==========
    
    /// Delete a key-value pair from a leaf node
    pub async fn delete_from_leaf(
        storage_manager: &Arc<Manager>,
        leaf_page_id: u64,
        key: u64,
    ) -> Result<DeleteResult, StorageError> {
        let mut leaf_page = (*storage_manager.read_page(leaf_page_id).await?).clone();
        
        // Find and remove the key-value pair
        if let Some(pos) = leaf_page.keys.iter().position(|&k| k == key) {
            leaf_page.keys.remove(pos);
            leaf_page.values.remove(pos);
            leaf_page.is_dirty = true;
            
            // Check for underflow
            if leaf_page.keys.len() < MIN_KEYS_PER_NODE && leaf_page.parent_page_id.is_some() {
                storage_manager.write_page(&leaf_page).await?;
                return Ok(DeleteResult::Underflow);
            }
            
            storage_manager.write_page(&leaf_page).await?;
            Ok(DeleteResult::Success)
        } else {
            Ok(DeleteResult::Success) // Key not found, consider it successful
        }
    }

    /// Delete multiple entries from a leaf node based on indices
    pub async fn delete_entries_from_leaf(
        storage_manager: &Arc<Manager>,
        leaf_page_id: u64,
        indices_to_delete: Vec<usize>,
    ) -> Result<DeleteResult, StorageError> {
        let mut leaf_page = (*storage_manager.read_page(leaf_page_id).await?).clone();
        
        if indices_to_delete.is_empty() {
            return Ok(DeleteResult::Success);
        }
        
        // Sort indices in descending order to avoid index shifting issues
        let mut sorted_indices = indices_to_delete;
        sorted_indices.sort_by(|a, b| b.cmp(a));
        
        // Remove entries
        for &index in &sorted_indices {
            if index < leaf_page.keys.len() {
                leaf_page.keys.remove(index);
                leaf_page.values.remove(index);
            }
        }
        
        leaf_page.is_dirty = true;
        
        // Check for underflow
        if leaf_page.keys.len() < MIN_KEYS_PER_NODE && leaf_page.parent_page_id.is_some() {
            storage_manager.write_page(&leaf_page).await?;
            return Ok(DeleteResult::Underflow);
        }
        
        // Check if page is now empty and is root
        if leaf_page.keys.is_empty() && leaf_page.parent_page_id.is_none() {
            storage_manager.write_page(&leaf_page).await?;
            return Ok(DeleteResult::RootDeleted);
        }
        
        storage_manager.write_page(&leaf_page).await?;
        Ok(DeleteResult::Success)
    }

    /// Handle underflow by borrowing from siblings or merging
    pub async fn handle_underflow(
        storage_manager: &Arc<Manager>,
        page_id: u64,
    ) -> Result<Option<u64>, StorageError> {
        let page = (*storage_manager.read_page(page_id).await?).clone();
        
        if page.parent_page_id.is_none() {
            // Root node - check if it needs to be updated
            if !page.is_leaf && page.keys.is_empty() && !page.child_page_ids.is_empty() {
                // Root has no keys but has one child - make child the new root
                let new_root_id = page.child_page_ids[0];
                let mut new_root = (*storage_manager.read_page(new_root_id).await?).clone();
                new_root.parent_page_id = None;
                new_root.is_dirty = true;
                storage_manager.write_page(&new_root).await?;
                return Ok(Some(new_root_id));
            }
            return Ok(None);
        }
        
        let parent_id = page.parent_page_id.unwrap();
        let parent = (*storage_manager.read_page(parent_id).await?).clone();
        
        // Find the position of this page in parent's children
        let page_index = match parent.child_page_ids.iter().position(|&id| id == page_id) {
            Some(index) => index,
            None => {
                // Page not found in parent - this can happen during concurrent operations
                // or if the page was already deallocated. Try to refresh parent and check again.
                let fresh_parent = (*storage_manager.read_page(parent_id).await?).clone();
                match fresh_parent.child_page_ids.iter().position(|&id| id == page_id) {
                    Some(index) => index,
                    None => {
                        // Page is truly not in parent - it may have been already handled
                        // Return None to indicate no further action needed
                        return Ok(None);
                    }
                }
            }
        };
        
        // Try to borrow from left sibling
        if page_index > 0 {
            let left_sibling_id = parent.child_page_ids[page_index - 1];
            if let Ok(merge_result) = Self::try_borrow_from_left(
                storage_manager, 
                page_id, 
                left_sibling_id, 
                parent_id, 
                page_index
            ).await {
                match merge_result {
                    MergeResult::Borrowed => return Ok(None),
                    MergeResult::RootUpdated(new_root) => return Ok(Some(new_root)),
                    _ => {}
                }
            }
        }
        
        // Try to borrow from right sibling
        if page_index < parent.child_page_ids.len() - 1 {
            let right_sibling_id = parent.child_page_ids[page_index + 1];
            if let Ok(merge_result) = Self::try_borrow_from_right(
                storage_manager, 
                page_id, 
                right_sibling_id, 
                parent_id, 
                page_index
            ).await {
                match merge_result {
                    MergeResult::Borrowed => return Ok(None),
                    MergeResult::RootUpdated(new_root) => return Ok(Some(new_root)),
                    _ => {}
                }
            }
        }
        
        // If borrowing failed, merge with a sibling
        if page_index > 0 {
            // Merge with left sibling
            let left_sibling_id = parent.child_page_ids[page_index - 1];
            Self::merge_with_left_sibling(
                storage_manager, 
                page_id, 
                left_sibling_id, 
                parent_id, 
                page_index
            ).await
        } else if page_index < parent.child_page_ids.len() - 1 {
            // Merge with right sibling
            let right_sibling_id = parent.child_page_ids[page_index + 1];
            Self::merge_with_right_sibling(
                storage_manager, 
                page_id, 
                right_sibling_id, 
                parent_id, 
                page_index
            ).await
        } else {
            Ok(None)
        }
    }

    /// Try to borrow a key from the left sibling
    async fn try_borrow_from_left(
        storage_manager: &Arc<Manager>,
        page_id: u64,
        left_sibling_id: u64,
        parent_id: u64,
        page_index: usize,
    ) -> Result<MergeResult, StorageError> {
        let mut left_sibling = (*storage_manager.read_page(left_sibling_id).await?).clone();
        let mut page = (*storage_manager.read_page(page_id).await?).clone();
        let mut parent = (*storage_manager.read_page(parent_id).await?).clone();
        
        // Check if left sibling has enough keys to lend
        if left_sibling.keys.len() <= MIN_KEYS_PER_NODE {
            return Err(StorageError::InvalidInput("Cannot borrow from left sibling".to_string()));
        }
        
        if page.is_leaf {
            // Borrow from leaf sibling
            let borrowed_key = left_sibling.keys.pop().unwrap();
            let borrowed_value = left_sibling.values.pop().unwrap();
            
            page.keys.insert(0, borrowed_key);
            page.values.insert(0, borrowed_value);
            
            // Update parent key
            parent.keys[page_index - 1] = borrowed_key;
        } else {
            // Borrow from internal sibling
            let borrowed_key = left_sibling.keys.pop().unwrap();
            let borrowed_child = left_sibling.child_page_ids.pop().unwrap();
            
            // Move parent key down and borrowed key up
            let parent_key = parent.keys[page_index - 1];
            parent.keys[page_index - 1] = borrowed_key;
            
            page.keys.insert(0, parent_key);
            page.child_page_ids.insert(0, borrowed_child);
            
            // Update borrowed child's parent
            let mut borrowed_child_page = (*storage_manager.read_page(borrowed_child).await?).clone();
            borrowed_child_page.parent_page_id = Some(page_id);
            borrowed_child_page.is_dirty = true;
            storage_manager.write_page(&borrowed_child_page).await?;
        }
        
        left_sibling.is_dirty = true;
        page.is_dirty = true;
        parent.is_dirty = true;
        
        storage_manager.write_page(&left_sibling).await?;
        storage_manager.write_page(&page).await?;
        storage_manager.write_page(&parent).await?;
        
        Ok(MergeResult::Borrowed)
    }

    /// Try to borrow a key from the right sibling
    async fn try_borrow_from_right(
        storage_manager: &Arc<Manager>,
        page_id: u64,
        right_sibling_id: u64,
        parent_id: u64,
        page_index: usize,
    ) -> Result<MergeResult, StorageError> {
        let mut right_sibling = (*storage_manager.read_page(right_sibling_id).await?).clone();
        let mut page = (*storage_manager.read_page(page_id).await?).clone();
        let mut parent = (*storage_manager.read_page(parent_id).await?).clone();
        
        // Check if right sibling has enough keys to lend
        if right_sibling.keys.len() <= MIN_KEYS_PER_NODE {
            return Err(StorageError::InvalidInput("Cannot borrow from right sibling".to_string()));
        }
        
        if page.is_leaf {
            // Borrow from leaf sibling
            let borrowed_key = right_sibling.keys.remove(0);
            let borrowed_value = right_sibling.values.remove(0);
            
            page.keys.push(borrowed_key);
            page.values.push(borrowed_value);
            
            // Update parent key
            parent.keys[page_index] = right_sibling.keys[0];
        } else {
            // Borrow from internal sibling
            let borrowed_key = right_sibling.keys.remove(0);
            let borrowed_child = right_sibling.child_page_ids.remove(0);
            
            // Move parent key down and borrowed key up
            let parent_key = parent.keys[page_index];
            parent.keys[page_index] = borrowed_key;
            
            page.keys.push(parent_key);
            page.child_page_ids.push(borrowed_child);
            
            // Update borrowed child's parent
            let mut borrowed_child_page = (*storage_manager.read_page(borrowed_child).await?).clone();
            borrowed_child_page.parent_page_id = Some(page_id);
            borrowed_child_page.is_dirty = true;
            storage_manager.write_page(&borrowed_child_page).await?;
        }
        
        right_sibling.is_dirty = true;
        page.is_dirty = true;
        parent.is_dirty = true;
        
        storage_manager.write_page(&right_sibling).await?;
        storage_manager.write_page(&page).await?;
        storage_manager.write_page(&parent).await?;
        
        Ok(MergeResult::Borrowed)
    }

    /// Merge page with its left sibling
    async fn merge_with_left_sibling(
        storage_manager: &Arc<Manager>,
        page_id: u64,
        left_sibling_id: u64,
        parent_id: u64,
        page_index: usize,
    ) -> Result<Option<u64>, StorageError> {
        let mut left_sibling = (*storage_manager.read_page(left_sibling_id).await?).clone();
        let page = (*storage_manager.read_page(page_id).await?).clone();
        let mut parent = (*storage_manager.read_page(parent_id).await?).clone();
        
        if page.is_leaf {
            // Merge leaf pages
            left_sibling.keys.extend(page.keys);
            left_sibling.values.extend(page.values);
            left_sibling.next_leaf_page_id = page.next_leaf_page_id;
        } else {
            // Merge internal pages
            let separator_key = parent.keys[page_index - 1];
            left_sibling.keys.push(separator_key);
            left_sibling.keys.extend(page.keys);
            
            // Update children's parent pointers before moving child_page_ids
            for &child_id in &page.child_page_ids {
                let mut child = (*storage_manager.read_page(child_id).await?).clone();
                child.parent_page_id = Some(left_sibling_id);
                child.is_dirty = true;
                storage_manager.write_page(&child).await?;
            }
            
            left_sibling.child_page_ids.extend(page.child_page_ids);
        }
        
        // Remove the separator key and page reference from parent
        parent.keys.remove(page_index - 1);
        parent.child_page_ids.remove(page_index);
        
        left_sibling.is_dirty = true;
        parent.is_dirty = true;
        
        storage_manager.write_page(&left_sibling).await?;
        storage_manager.write_page(&parent).await?;
        
        // Deallocate the merged page
        storage_manager.deallocate_page(page_id).await?;
        
        // Unregister from leaf registry if it's a leaf page
        if page.is_leaf {
            storage_manager.unregister_leaf_page(page_id).await?;
        }
        
        // Check if parent underflows
        if parent.keys.len() < MIN_KEYS_PER_NODE && parent.parent_page_id.is_some() {
            Box::pin(Self::handle_underflow(storage_manager, parent_id)).await
        } else if parent.keys.is_empty() && parent.parent_page_id.is_none() {
            // Parent is root and empty - left sibling becomes new root
            left_sibling.parent_page_id = None;
            left_sibling.is_dirty = true;
            storage_manager.write_page(&left_sibling).await?;
            Ok(Some(left_sibling_id))
        } else {
            Ok(None)
        }
    }

    /// Merge page with its right sibling
    async fn merge_with_right_sibling(
        storage_manager: &Arc<Manager>,
        page_id: u64,
        right_sibling_id: u64,
        parent_id: u64,
        page_index: usize,
    ) -> Result<Option<u64>, StorageError> {
        let mut page = (*storage_manager.read_page(page_id).await?).clone();
        let right_sibling = (*storage_manager.read_page(right_sibling_id).await?).clone();
        let mut parent = (*storage_manager.read_page(parent_id).await?).clone();
        
        if page.is_leaf {
            // Merge leaf pages
            page.keys.extend(right_sibling.keys);
            page.values.extend(right_sibling.values);
            page.next_leaf_page_id = right_sibling.next_leaf_page_id;
        } else {
            // Merge internal pages
            let separator_key = parent.keys[page_index];
            page.keys.push(separator_key);
            page.keys.extend(right_sibling.keys);
            
            // Update children's parent pointers before moving child_page_ids
            for &child_id in &right_sibling.child_page_ids {
                let mut child = (*storage_manager.read_page(child_id).await?).clone();
                child.parent_page_id = Some(page_id);
                child.is_dirty = true;
                storage_manager.write_page(&child).await?;
            }
            
            page.child_page_ids.extend(right_sibling.child_page_ids);
        }
        
        // Remove the separator key and right sibling reference from parent
        parent.keys.remove(page_index);
        parent.child_page_ids.remove(page_index + 1);
        
        page.is_dirty = true;
        parent.is_dirty = true;
        
        storage_manager.write_page(&page).await?;
        storage_manager.write_page(&parent).await?;
        
        // Deallocate the merged page
        storage_manager.deallocate_page(right_sibling_id).await?;
        
        // Unregister from leaf registry if it's a leaf page
        if right_sibling.is_leaf {
            storage_manager.unregister_leaf_page(right_sibling_id).await?;
        }
        
        // Check if parent underflows
        if parent.keys.len() < MIN_KEYS_PER_NODE && parent.parent_page_id.is_some() {
            Box::pin(Self::handle_underflow(storage_manager, parent_id)).await
        } else if parent.keys.is_empty() && parent.parent_page_id.is_none() {
            // Parent is root and empty - merged page becomes new root
            page.parent_page_id = None;
            page.is_dirty = true;
            storage_manager.write_page(&page).await?;
            Ok(Some(page_id))
        } else {
            Ok(None)
        }
    }

    // ========== UTILITY OPERATIONS ==========
    
    /// Get sibling page IDs for a given page
    pub async fn get_siblings(
        storage_manager: &Arc<Manager>,
        page_id: u64,
    ) -> Result<(Option<u64>, Option<u64>), StorageError> {
        let page = storage_manager.read_page(page_id).await?;
        
        if let Some(parent_id) = page.parent_page_id {
            let parent = storage_manager.read_page(parent_id).await?;
            let page_index = match parent.child_page_ids.iter().position(|&id| id == page_id) {
                Some(index) => index,
                None => {
                    // Page not found in parent - return no siblings
                    return Ok((None, None));
                }
            };
            
            let left_sibling = if page_index > 0 {
                Some(parent.child_page_ids[page_index - 1])
            } else {
                None
            };
            
            let right_sibling = if page_index < parent.child_page_ids.len() - 1 {
                Some(parent.child_page_ids[page_index + 1])
            } else {
                None
            };
            
            Ok((left_sibling, right_sibling))
        } else {
            Ok((None, None))
        }
    }

    /// Check if a page has underflow
    pub fn has_underflow(page: &Page) -> bool {
        page.keys.len() < MIN_KEYS_PER_NODE && page.parent_page_id.is_some()
    }

    /// Check if a page can lend a key
    pub fn can_lend(page: &Page) -> bool {
        page.keys.len() > MIN_KEYS_PER_NODE
    }
}
