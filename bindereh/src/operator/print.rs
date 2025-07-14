use std::sync::Arc;

use crate::{common::StorageError, manager::Manager, operator::tree::TreeOperations};

pub struct TreePrinter {
    storage_manager: Arc<Manager>,
}

impl TreePrinter {
    pub fn new(storage_manager: Arc<Manager>) -> Self {
        Self { storage_manager }
    }

    pub async fn print_tree(&self, root_page_id: u64) -> Result<(), StorageError> {
        println!("┌─────────────────────────────────────────────────────────────┐");
        println!("│                        B+ Tree Structure                    │");
        println!("└─────────────────────────────────────────────────────────────┘");
        self.print_subtree(root_page_id, 0, true, String::new())
            .await?;

        println!("\nLeaf Level Connections:");
        self.print_leaf_connections(root_page_id).await?;
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
        let mut current_leaf =
            TreeOperations::find_leftmost_leaf(&self.storage_manager, page_id).await?;
        let mut leaf_chain = Vec::new();

        while let Some(leaf_id) = current_leaf {
            let page = self.storage_manager.read_page(leaf_id).await?;
            leaf_chain.push((leaf_id, page.keys.clone()));
            current_leaf = page.next_leaf_page_id;
        }

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
}
