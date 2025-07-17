use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::page::Page;

#[derive(Debug)]
struct LRUNode {
    page_id: u64,
    page: Arc<Page>,
    prev: Option<u64>,
    next: Option<u64>,
}

pub struct Pool {
    cache: Arc<Mutex<HashMap<u64, LRUNode>>>,
    dirty_pages: Arc<Mutex<HashMap<u64, Arc<Page>>>>,
    max_pages: usize,
    head: Arc<Mutex<Option<u64>>>,
    tail: Arc<Mutex<Option<u64>>>,
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
            head: Arc::new(Mutex::new(None)),
            tail: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get_page(&self, page_id: u64) -> Option<Arc<Page>> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(node) = cache.get_mut(&page_id) {
            let page = node.page.clone();
            // Move to front (most recently used)
            self.move_to_front(page_id, &mut cache);
            Some(page)
        } else {
            None
        }
    }

    pub fn put_page(&self, page_id: u64, page: Arc<Page>) {
        let mut cache = self.cache.lock().unwrap();
        
        if cache.contains_key(&page_id) {
            // Update existing page and move to front
            if let Some(node) = cache.get_mut(&page_id) {
                node.page = page.clone();
            }
            self.move_to_front(page_id, &mut cache);
        } else {
            // Add new page
            if cache.len() >= self.max_pages {
                self.evict_lru(&mut cache);
            }
            
            let new_node = LRUNode {
                page_id,
                page: page.clone(),
                prev: None,
                next: *self.head.lock().unwrap(),
            };
            
            // Update head's prev pointer
            if let Some(old_head) = *self.head.lock().unwrap() {
                if let Some(head_node) = cache.get_mut(&old_head) {
                    head_node.prev = Some(page_id);
                }
            } else {
                // First node, set as tail too
                *self.tail.lock().unwrap() = Some(page_id);
            }
            
            *self.head.lock().unwrap() = Some(page_id);
            cache.insert(page_id, new_node);
        }

        if page.is_dirty {
            self.dirty_pages.lock().unwrap().insert(page_id, page);
        }
    }

    fn move_to_front(&self, page_id: u64, cache: &mut HashMap<u64, LRUNode>) {
        if Some(page_id) == *self.head.lock().unwrap() {
            return; // Already at front
        }

        // Remove from current position
        if let Some(node) = cache.get(&page_id) {
            let prev_id = node.prev;
            let next_id = node.next;

            if let Some(prev) = prev_id {
                if let Some(prev_node) = cache.get_mut(&prev) {
                    prev_node.next = next_id;
                }
            }

            if let Some(next) = next_id {
                if let Some(next_node) = cache.get_mut(&next) {
                    next_node.prev = prev_id;
                }
            } else {
                // This was the tail
                *self.tail.lock().unwrap() = prev_id;
            }
        }

        // Move to front
        if let Some(node) = cache.get_mut(&page_id) {
            node.prev = None;
            node.next = *self.head.lock().unwrap();
        }

        if let Some(old_head) = *self.head.lock().unwrap() {
            if let Some(head_node) = cache.get_mut(&old_head) {
                head_node.prev = Some(page_id);
            }
        }

        *self.head.lock().unwrap() = Some(page_id);
    }

    fn evict_lru(&self, cache: &mut HashMap<u64, LRUNode>) {
        if let Some(tail_id) = *self.tail.lock().unwrap() {
            if let Some(tail_node) = cache.get(&tail_id) {
                let prev_id = tail_node.prev;
                
                // Update tail
                *self.tail.lock().unwrap() = prev_id;
                
                if let Some(prev) = prev_id {
                    if let Some(prev_node) = cache.get_mut(&prev) {
                        prev_node.next = None;
                    }
                } else {
                    // List becomes empty
                    *self.head.lock().unwrap() = None;
                }
                
                // Remove from dirty pages if present
                self.dirty_pages.lock().unwrap().remove(&tail_id);
                cache.remove(&tail_id);
            }
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
        self.cache.lock().unwrap().remove(&page_id).map(|node| node.page)
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
            cache.remove(&first_key).map(|node| node.page)
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
