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
}
