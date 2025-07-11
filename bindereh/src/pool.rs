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

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub cached_pages: usize,
    pub dirty_pages: usize,
    pub max_pages: usize,
    pub cache_utilization: f64,
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

    /// Clear all cached pages - used for truncate operations
    pub fn clear_all(&self) {
        self.cache.lock().unwrap().clear();
        self.dirty_pages.lock().unwrap().clear();
    }

    /// Clear all dirty pages - useful for various cleanup operations
    pub fn clear_all_dirty(&self) {
        self.dirty_pages.lock().unwrap().clear();
    }

    /// Get the current number of cached pages
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Get the current number of dirty pages
    pub fn dirty_count(&self) -> usize {
        self.dirty_pages.lock().unwrap().len()
    }

    /// Check if a specific page is cached
    pub fn contains_page(&self, page_id: u64) -> bool {
        self.cache.lock().unwrap().contains_key(&page_id)
    }

    /// Check if a specific page is dirty
    pub fn is_dirty(&self, page_id: u64) -> bool {
        self.dirty_pages.lock().unwrap().contains_key(&page_id)
    }

    /// Remove a specific page from cache (but not from dirty pages)
    pub fn remove_page(&self, page_id: u64) -> Option<Arc<Page>> {
        self.cache.lock().unwrap().remove(&page_id)
    }

    /// Get all cached page IDs
    pub fn get_cached_page_ids(&self) -> Vec<u64> {
        self.cache.lock().unwrap().keys().cloned().collect()
    }

    /// Get all dirty page IDs
    pub fn get_dirty_page_ids(&self) -> Vec<u64> {
        self.dirty_pages.lock().unwrap().keys().cloned().collect()
    }

    /// Force evict oldest page if cache is full (useful for manual cache management)
    pub fn evict_oldest(&self) -> Option<Arc<Page>> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(&first_key) = cache.keys().next() {
            cache.remove(&first_key)
        } else {
            None
        }
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> PoolStats {
        let cache = self.cache.lock().unwrap();
        let dirty_pages = self.dirty_pages.lock().unwrap();

        PoolStats {
            cached_pages: cache.len(),
            dirty_pages: dirty_pages.len(),
            max_pages: self.max_pages,
            cache_utilization: cache.len() as f64 / self.max_pages as f64,
        }
    }
}
