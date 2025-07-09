use bindereh::{
    common::{NODE_HEADER_SIZE, PAGE_SIZE, StorageError},
    page::{Page, Row},
    value::Value,
};

#[test]
fn test_leaf_page_serialization() {
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

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(page.page_id, deserialized.page_id);
    assert_eq!(page.is_leaf, deserialized.is_leaf);
    assert_eq!(page.parent_page_id, deserialized.parent_page_id);
    assert_eq!(page.keys, deserialized.keys);
    assert_eq!(page.values.len(), deserialized.values.len());
    assert_eq!(page.child_page_ids, deserialized.child_page_ids);
    assert_eq!(page.next_leaf_page_id, deserialized.next_leaf_page_id);
    assert_eq!(page.is_dirty, deserialized.is_dirty);

    // Verify row data
    for (original, deserialized) in page.values.iter().zip(deserialized.values.iter()) {
        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.data, deserialized.data);
    }
}

#[test]
fn test_internal_page_serialization() {
    let page = Page {
        page_id: 10,
        is_leaf: false,
        parent_page_id: None,
        keys: vec![50, 100, 150],
        values: vec![],
        child_page_ids: vec![11, 12, 13, 14],
        next_leaf_page_id: None,
        is_dirty: true,
    };

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(page.page_id, deserialized.page_id);
    assert_eq!(page.is_leaf, deserialized.is_leaf);
    assert_eq!(page.parent_page_id, deserialized.parent_page_id);
    assert_eq!(page.keys, deserialized.keys);
    // TODO: Should be test deeply equal
    assert_eq!(page.values.len(), deserialized.values.len());
    assert_eq!(page.child_page_ids, deserialized.child_page_ids);
    assert_eq!(page.next_leaf_page_id, deserialized.next_leaf_page_id);
    assert_eq!(deserialized.is_dirty, false); // is_dirty is always false after deserialization
}

#[test]
fn test_empty_leaf_page() {
    let page = Page {
        page_id: 0,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: false,
    };

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(page.page_id, deserialized.page_id);
    assert_eq!(page.is_leaf, deserialized.is_leaf);
    assert_eq!(page.parent_page_id, deserialized.parent_page_id);
    assert_eq!(page.keys, deserialized.keys);
    // TODO: Should be test deeply equal
    assert_eq!(page.values.len(), deserialized.values.len());
    assert_eq!(page.child_page_ids, deserialized.child_page_ids);
    assert_eq!(page.next_leaf_page_id, deserialized.next_leaf_page_id);
}

#[test]
fn test_empty_internal_page() {
    let page = Page {
        page_id: 999,
        is_leaf: false,
        parent_page_id: Some(1000),
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: false,
    };

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(page.page_id, deserialized.page_id);
    assert_eq!(page.is_leaf, deserialized.is_leaf);
    assert_eq!(page.parent_page_id, deserialized.parent_page_id);
    assert_eq!(page.keys, deserialized.keys);
    // TODO: Should be test deeply equal
    assert_eq!(page.values.len(), deserialized.values.len());
    assert_eq!(page.child_page_ids, deserialized.child_page_ids);
    assert_eq!(page.next_leaf_page_id, deserialized.next_leaf_page_id);
}

#[test]
fn test_invalid_magic_number() {
    let mut bytes = vec![0u8; PAGE_SIZE];
    // Write wrong magic number
    bytes[0..4].copy_from_slice(&0xBADC0DE_u32.to_le_bytes());

    let result = Page::from_bytes(&bytes);
    assert!(matches!(result, Err(StorageError::CorruptedData(_))));
}

#[test]
fn test_insufficient_data() {
    let bytes = vec![0u8; NODE_HEADER_SIZE - 1];
    let result = Page::from_bytes(&bytes);
    assert!(matches!(result, Err(StorageError::CorruptedData(_))));
}

#[test]
fn test_page_size_padding() {
    let page = Page {
        page_id: 1,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![1],
        values: vec![Row {
            id: 1,
            data: vec![Value::Integer(42)],
        }],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: false,
    };

    let bytes = page.to_bytes();
    assert_eq!(bytes.len(), PAGE_SIZE);
}

#[test]
fn test_large_text_value() {
    let large_text = "a".repeat(1000);
    let row = Row {
        id: 1,
        data: vec![Value::Text(large_text.clone())],
    };

    let page = Page {
        page_id: 1,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![1],
        values: vec![row],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: false,
    };

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(deserialized.values.len(), 1);
    assert_eq!(deserialized.values[0].data.len(), 1);
    if let Value::Text(text) = &deserialized.values[0].data[0] {
        assert_eq!(text, &large_text);
    } else {
        panic!("Expected Text value");
    }
}

#[test]
fn test_multiple_value_types() {
    let row = Row {
        id: 42,
        data: vec![
            Value::Integer(-1000),
            Value::Float(-3.14159),
            Value::Text("".to_string()), // Empty string
            Value::Boolean(false),
            Value::Null,
            Value::Integer(i64::MAX),
            Value::Float(f64::MIN),
        ],
    };

    let page = Page {
        page_id: 123,
        is_leaf: true,
        parent_page_id: Some(456),
        keys: vec![42],
        values: vec![row],
        child_page_ids: vec![],
        next_leaf_page_id: Some(789),
        is_dirty: true,
    };

    let bytes = page.to_bytes();
    let deserialized = Page::from_bytes(&bytes).unwrap();

    assert_eq!(deserialized.values.len(), 1);
    let deserialized_row = &deserialized.values[0];
    assert_eq!(deserialized_row.id, 42);
    assert_eq!(deserialized_row.data.len(), 7);

    // Verify each value type
    assert_eq!(deserialized_row.data[0], Value::Integer(-1000));
    assert_eq!(deserialized_row.data[1], Value::Float(-3.14159));
    assert_eq!(deserialized_row.data[2], Value::Text("".to_string()));
    assert_eq!(deserialized_row.data[3], Value::Boolean(false));
    assert_eq!(deserialized_row.data[4], Value::Null);
    assert_eq!(deserialized_row.data[5], Value::Integer(i64::MAX));
    assert_eq!(deserialized_row.data[6], Value::Float(f64::MIN));
}
