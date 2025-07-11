use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    common::{PAGE_SIZE, StorageError},
    page::Page,
    pool::Pool,
};

pub struct Manager {
    file: Arc<Mutex<File>>,
    buffer_pool: Pool,
    next_page_id: Arc<Mutex<u64>>,
}

impl Manager {
    pub fn new<P: AsRef<Path>>(file_path: P, buffer_size: usize) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            buffer_pool: Pool::new(buffer_size),
            next_page_id: Arc::new(Mutex::new(1)), // Start from page 1 (0 reserved for metadata)
        })
    }

    pub async fn read_page(&self, page_id: u64) -> Result<Arc<Page>, StorageError> {
        // read from cache first
        if let Some(cached_node) = self.buffer_pool.get_page(page_id) {
            return Ok(cached_node);
        }

        // Read from disk
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;

        let node = Page::from_bytes(&buffer)?;
        let node_arc = Arc::new(node);

        // Cache the page
        self.buffer_pool.put_page(page_id, node_arc.clone());

        Ok(node_arc)
    }

    pub async fn write_page(&self, node: &Page) -> Result<(), StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(node.page_id * PAGE_SIZE as u64))?;

        let bytes = node.to_bytes();
        file.write_all(&bytes)?;
        file.sync_all()?; // Force flush to disk

        // Update the buffer pool cache with the new data
        let node_arc = Arc::new(node.clone());
        self.buffer_pool.put_page(node.page_id, node_arc);

        // Clear from dirty pages
        self.buffer_pool.clear_dirty(node.page_id);

        Ok(())
    }

    // get next_page_id
    pub async fn allocate_page(&self) -> u64 {
        let mut next_id = self.next_page_id.lock().unwrap();
        let page_id = *next_id;
        *next_id += 1;
        page_id
    }

    pub async fn flush_dirty_pages(&self) -> Result<(), StorageError> {
        let dirty_pages = self.buffer_pool.get_dirty_pages();

        for node in dirty_pages {
            self.write_page(&node).await?;
        }

        Ok(())
    }

    pub async fn truncate(&self) -> Result<(), StorageError> {
        self.buffer_pool.clear_all();

        {
            let mut next_id = self.next_page_id.lock().unwrap();
            *next_id = 1;
        }

        {
            let file = self.file.lock().unwrap();
            file.set_len(0)?;
            file.sync_all()?;
        }

        // init the b+ tree page again from the beginning (mulai dari 0 ya kaks)
        let root_node = Page {
            page_id: *self.next_page_id.lock().unwrap(),
            is_leaf: true,
            parent_page_id: None,
            keys: vec![],
            values: vec![],
            child_page_ids: vec![],
            next_leaf_page_id: None,
            is_dirty: true,
        };

        self.write_page(&root_node).await.unwrap();

        Ok(())
    }
}
