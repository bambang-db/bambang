use crate::value::Value;

#[derive(Debug, Clone)]
pub struct Row {
    pub id: u64,
    pub data: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: u64,
    pub is_leaf: bool,
    pub parent_page_id: Option<u64>,
    pub keys: Vec<u64>,
    pub values: Vec<Row>,               // Only for leaf nodes
    pub child_page_ids: Vec<u64>,       // Page IDs for children (internal nodes)
    pub next_leaf_page_id: Option<u64>, // Leaf node linking
    pub is_dirty: bool,                 // Track if node needs to be written to disk
}

impl Page {

    pub fn to_bytes(&self) {

    }

    pub fn from_bytes() {

    }

}
