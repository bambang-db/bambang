use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{common::StorageError, page::Page, pool::Pool};

pub struct Manager {
    file: Arc<Mutex<File>>,
    buffer_pool: Pool,
    next_page_id: Arc<Mutex<u64>>,
    file_path: String,
}

impl Manager {
    pub fn new<P: AsRef<Path>>(file_path: P, buffer_size: usize) -> Result<Self, StorageError> {
        todo!()
    }

    pub async fn read_page(&self, page_id: u64) -> Result<Arc<Page>, StorageError> {
        todo!()
    }

    pub async fn write_page(&self, node: &Page) -> Result<(), StorageError> {
        todo!()
    }

    pub async fn allocate_page(&self) -> u64 {
        todo!()
    }

    pub async fn flush_dirty_pages(&self) -> Result<(), StorageError> {
        todo!()
    }
}
