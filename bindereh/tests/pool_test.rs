use std::sync::Arc;

use bindereh::{
    page::{Page, Row},
    pool::Pool,
    value::Value,
};

#[test]
fn test_page_pool() {
    let pool = Pool::new(8);

    let row1 = Row {
        id: 1,
        data: vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Float(3.14),
            Value::Boolean(true),
            Value::Null,
        ],
    };

    let row2 = Row {
        id: 2,
        data: vec![
            Value::Integer(100),
            Value::Text("world".to_string()),
            Value::Boolean(false),
        ],
    };

    let page = Page {
        page_id: 1,
        is_leaf: true,
        parent_page_id: Some(5),
        keys: vec![1, 2],
        values: vec![row1, row2],
        child_page_ids: vec![],
        next_leaf_page_id: Some(3),
        is_dirty: false,
    };

    pool.put_page(page.page_id, Arc::new(page.clone()));

    let deserialized = pool.get_page(1);

    match deserialized {
        Some(deserialized) => {
            assert_eq!(page.page_id, deserialized.page_id);
            assert_eq!(page.is_leaf, deserialized.is_leaf);
            assert_eq!(page.parent_page_id, deserialized.parent_page_id);
            assert_eq!(page.keys, deserialized.keys);
            assert_eq!(page.values.len(), deserialized.values.len());
            assert_eq!(page.values, deserialized.values);
            assert_eq!(page.child_page_ids, deserialized.child_page_ids);
            assert_eq!(page.next_leaf_page_id, deserialized.next_leaf_page_id);
        }
        None => panic!("Could not read page through pool"),
    }
}

#[test]
fn test_get_nonexistent_page() {
    let pool = Pool::new(3);
    assert!(pool.get_page(999).is_none());
}
