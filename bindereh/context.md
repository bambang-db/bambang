## Bindereh Module

The bindereh module was part of bambang db project, an undergraduate thesis to develop a rudimentary embedded htap database,
that will employ partitioned b+ tree and taking advantage of partitioned b+ tree introduces parallel scan. the bindereh 
module basically just a storage engine layer that will responsible to-do an I/O to the disk. the part of bindereh module
will be explaining below.

### Value (value.rs)

A struct that will hold the value of a column in a row, will have `from_bytes` and `to_bytes` the struct will be like this : 

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Integer(i64),
    String(String),
    // another data type supported
}
```

### Page (page.rs)

A struct that will act as B+ Tree Node, or sometime called as a page, basically will store the `keys`(index) 
and `values` (list of rows), will also have `from_bytes` and `to_bytes` function also the struct will be like this : 

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub id: u64,
    pub data: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
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
```

### Pool (pool.rs)

A struct that will act as buffer pool, cache some page and evict if needed, will have function `new`, `get_page`, `put_page`, 
`mark_dirty`, `get_dirty_pages` and `clear_dirty` the struct will be like this : 

```rust
pub struct Pool {
    cache: Arc<Mutex<HashMap<u64, Arc<Page>>>>,
    dirty_pages: Arc<Mutex<HashMap<u64, Arc<Page>>>>,
    max_pages: usize,
}
```

### Manager (manager.rs)

A struct that will facilitate and act as middle-layer to `write` or `read` a page, will have function `new`, `read_page`, 
`write_page`, `allocate_page` and `flush_dirty_pages` he struct will be like this : 

```rust
pub struct Manager {
    file: Arc<Mutex<File>>,
    buffer_pool: Pool,
    next_page_id: Arc<Mutex<u64>>,
    file_path: String,
}
```

## Executor 

A struct that will facilitate a-full `insert`, `update`, 