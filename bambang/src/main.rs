use std::sync::Arc;

use bindereh::{
    executor::{Executor, ScanOptions},
    manager::Manager,
    page::{Page, Row},
    value::Value,
};

#[tokio::main]
async fn main() {
    let manager = Arc::new(Manager::new("bambang.db", 128).unwrap());

    // Create initial root node
    let mut current_root_page_id = manager.allocate_page().await;
    println!("Initial root page ID: {}", current_root_page_id);

    let root_node = Page {
        page_id: current_root_page_id,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: true,
    };

    manager.write_page(&root_node).await.unwrap();

    let executor = Executor::new(manager.clone(), current_root_page_id, 2);

    // Insert multiple rows to potentially trigger splits
    for i in 1..=20 {
        let row = Row {
            id: i,
            data: vec![
                Value::Integer(i as i64 * 100),
                Value::String(format!("User{}", i)),
                Value::Boolean(i % 2 == 0),
            ],
        };

        // Get the root page ID after insertion (it might change due to splits)
        let new_root_id = executor.insert(row).await.unwrap();

        if new_root_id != current_root_page_id {
            println!(
                "Root page ID changed from {} to {} after inserting row {}",
                current_root_page_id, new_root_id, i
            );
            current_root_page_id = new_root_id;

            // Here you would update your catalog with the new root_page_id
        }
    }

    // Read the final root page
    executor.debug_print_tree().await.unwrap();

    executor.scan(ScanOptions::default()).await.unwrap();
}
