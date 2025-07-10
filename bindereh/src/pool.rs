use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::page::Page;

pub struct Pool {
    cache: Arc<Mutex<HashMap<u64, Arc<Page>>>>,
    dirty_pages: Arc<Mutex<HashMap<u64, Arc<Page>>>>,
    max_pages: usize,
}

impl Pool {
    pub fn new(max_pages: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            dirty_pages: Arc::new(Mutex::new(HashMap::new())),
            max_pages,
        }
    }

    pub fn get_page(&self, page_id: u64) -> Option<Arc<Page>> {
        self.cache.lock().unwrap().get(&page_id).cloned()
    }

    pub fn put_page(&self, page_id: u64, node: Arc<Page>) {
        let mut cache = self.cache.lock().unwrap();

        // TODO: Implement Real LRU
        // This is just mimic BufferPool behavior
        if cache.len() >= self.max_pages {
            let first_key = *cache.keys().next().unwrap();
            cache.remove(&first_key);
        }

        cache.insert(page_id, node.clone());

        if node.is_dirty {
            self.dirty_pages.lock().unwrap().insert(page_id, node);
        }
    }

    pub fn mark_dirty(&self, page_id: u64, node: Arc<Page>) {
        self.dirty_pages.lock().unwrap().insert(page_id, node);
    }

    pub fn get_dirty_pages(&self) -> Vec<Arc<Page>> {
        self.dirty_pages.lock().unwrap().values().cloned().collect()
    }

    pub fn clear_dirty(&self, page_id: u64) {
        self.dirty_pages.lock().unwrap().remove(&page_id);
    }
}
