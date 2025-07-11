use std::sync::Arc;

use crate::{common::{StorageError, MAX_KEYS_PER_NODE}, manager::Manager, page::Page};

pub struct TreeOperations;

impl TreeOperations {
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

    pub async fn split_leaf_node(
        storage_manager: &Arc<Manager>,
        node: &mut Page,
    ) -> Result<Option<u64>, StorageError> {
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

        if node.parent_page_id.is_none() {
            Self::create_new_root(storage_manager, node.page_id, promoted_key, new_page_id).await?;
            return Ok(None);
        }

        Ok(Some(promoted_key))
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

        // Update parent pointers
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
    ) -> Result<(), StorageError> {
        if let Some(parent_id) = parent_page_id {
            let mut parent_node = (*storage_manager.read_page(parent_id).await?).clone();

            let insert_pos = parent_node
                .keys
                .binary_search(&key)
                .unwrap_or_else(|pos| pos);

            parent_node.keys.insert(insert_pos, key);
            parent_node
                .child_page_ids
                .insert(insert_pos + 1, right_child_id);
            parent_node.is_dirty = true;

            if parent_node.keys.len() > MAX_KEYS_PER_NODE {
                let promoted_key =
                    Self::split_internal_node(storage_manager, &mut parent_node).await?;

                if let Some(promoted) = promoted_key {
                    Box::pin(Self::insert_into_parent(
                        storage_manager,
                        parent_node.parent_page_id,
                        promoted,
                        parent_node.child_page_ids.last().cloned().unwrap(),
                    ))
                    .await?;
                }
            }

            storage_manager.write_page(&parent_node).await?;
        }

        Ok(())
    }

    pub async fn split_internal_node(
        storage_manager: &Arc<Manager>,
        node: &mut Page,
    ) -> Result<Option<u64>, StorageError> {
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

        // Update parent pointers for moved children
        for &child_id in &new_node.child_page_ids {
            let mut child = (*storage_manager.read_page(child_id).await?).clone();
            child.parent_page_id = Some(new_page_id);
            child.is_dirty = true;
            storage_manager.write_page(&child).await?;
        }

        storage_manager.write_page(&new_node).await?;

        if node.parent_page_id.is_none() {
            Self::create_new_root(storage_manager, node.page_id, promoted_key, new_page_id).await?;
            return Ok(None);
        }

        Ok(Some(promoted_key))
    }

    pub async fn find_leftmost_leaf(
        storage_manager: &Arc<Manager>,
        page_id: u64,
    ) -> Result<Option<u64>, StorageError> {
        let page = storage_manager.read_page(page_id).await?;

        if page.is_leaf {
            Ok(Some(page_id))
        } else if !page.child_page_ids.is_empty() {
            Box::pin(Self::find_leftmost_leaf(
                storage_manager,
                page.child_page_ids[0],
            ))
            .await
        } else {
            Ok(None)
        }
    }
}
