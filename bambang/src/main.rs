use std::sync::Arc;

use bindereh::{
    executor::Executor,
    manager::Manager,
    page::{Page, Row},
    value::Value,
};

#[tokio::main]
async fn main() {
    let manager = Manager::new("bambang.db", 128).unwrap();

    // Create initial root node
    let root_page_id = manager.allocate_page().await;
    let root_node = Page {
        page_id: root_page_id,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: true,
    };

    manager.write_page(&root_node).await.unwrap();

    let executor = Executor::new(Arc::new(manager), root_page_id, 2);

    for i in 1..=10 {
        let row = Row {
            id: i,
            data: vec![
                Value::Integer(i as i64 * 100),
                Value::String(format!("User{}", i)),
                Value::Boolean(i % 2 == 0),
            ],
        };

        executor.insert(row).await.unwrap();
    }
}
