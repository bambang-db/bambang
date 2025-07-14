use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{
    common::{PAGE_SIZE, StorageError},
    leaf_registry::LeafPageRegistry,
    page::Page,
    pool::Pool,
};

pub struct Manager {
    file: Arc<Mutex<File>>,
    buffer_pool: Pool,
    next_page_id: Arc<Mutex<u64>>,
    leaf_registry: Arc<LeafPageRegistry>,
}

impl Manager {
    pub fn new<P: AsRef<Path>>(file_path: P, buffer_size: usize) -> Result<Self, StorageError> {
        let file = OpenOptions::new().create(true).read(true).write(true).open(&file_path)?;
        let registry_path = format!("{}.registry", file_path.as_ref().to_string_lossy());
        let leaf_registry = Arc::new(LeafPageRegistry::new(registry_path)?);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            buffer_pool: Pool::new(buffer_size),
            next_page_id: Arc::new(Mutex::new(1)),
            leaf_registry,
        })
    }

    pub async fn read_page(&self, page_id: u64) -> Result<Arc<Page>, StorageError> {
        if let Some(cached_node) = self.buffer_pool.get_page(page_id) {
            return Ok(cached_node);
        }
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;
        let node = Page::from_bytes(&buffer)?;
        let node_arc = Arc::new(node);
        self.buffer_pool.put_page(page_id, node_arc.clone());
        Ok(node_arc)
    }

    pub async fn read_page_header(&self, page_id: u64) -> Result<(u64, bool, Option<u64>), StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        let mut buffer = vec![0u8; 37];
        file.read_exact(&mut buffer)?;
        let mut reader = std::io::Cursor::new(&buffer);
        let magic = ReadBytesExt::read_u32::<LittleEndian>(&mut reader).map_err(|_| StorageError::CorruptedData("Failed to read magic number".into()))?;
        if magic != crate::common::MAGIC_NUMBER {
            return Err(StorageError::CorruptedData("Invalid magic number".into()));
        }
        let actual_page_id = ReadBytesExt::read_u64::<LittleEndian>(&mut reader).map_err(|_| StorageError::CorruptedData("Failed to read page_id".into()))?;
        let is_leaf = ReadBytesExt::read_u8(&mut reader).map_err(|_| StorageError::CorruptedData("Failed to read is_leaf".into()))? == 1;
        reader.set_position(reader.position() + 8);
        let next_leaf_raw = ReadBytesExt::read_u64::<LittleEndian>(&mut reader).map_err(|_| StorageError::CorruptedData("Failed to read next_leaf_page_id".into()))?;
        let next_leaf_page_id = if next_leaf_raw == 0 { None } else { Some(next_leaf_raw) };
        Ok((actual_page_id, is_leaf, next_leaf_page_id))
    }

    pub async fn write_page(&self, node: &Page) -> Result<(), StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(node.page_id * PAGE_SIZE as u64))?;
        let bytes = node.to_bytes();
        file.write_all(&bytes)?;
        file.sync_all()?;
        let node_arc = Arc::new(node.clone());
        self.buffer_pool.put_page(node.page_id, node_arc);
        self.buffer_pool.clear_dirty(node.page_id);
        Ok(())
    }

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
        let root_page_id = *self.next_page_id.lock().unwrap();
        let root_node = Page {
            page_id: root_page_id,
            is_leaf: true,
            parent_page_id: None,
            keys: vec![],
            values: vec![],
            child_page_ids: vec![],
            next_leaf_page_id: None,
            is_dirty: true,
        };
        self.write_page(&root_node).await.unwrap();
        self.register_leaf_page(root_page_id).await.unwrap();
        Ok(())
    }

    pub fn get_leaf_registry(&self) -> Arc<LeafPageRegistry> {
        Arc::clone(&self.leaf_registry)
    }

    pub async fn register_leaf_page(&self, page_id: u64) -> Result<(), StorageError> {
        self.leaf_registry.add_leaf_page(page_id)
    }

    pub async fn unregister_leaf_page(&self, page_id: u64) -> Result<bool, StorageError> {
        self.leaf_registry.remove_leaf_page(page_id)
    }

    pub async fn get_all_leaf_page_ids(&self) -> Result<Vec<u64>, StorageError> {
        self.leaf_registry.get_all_leaf_pages()
    }

    pub fn get_all_leaf_page_ids_sync(&self) -> Result<Vec<u64>, StorageError> {
        self.leaf_registry.get_all_leaf_pages()
    }

    pub async fn get_leaf_page_batch(&self, start_index: usize, batch_size: usize) -> Result<Vec<u64>, StorageError> {
        self.leaf_registry.get_leaf_page_batch(start_index, batch_size)
    }

    pub async fn get_leaf_page_count(&self) -> Result<u64, StorageError> {
        self.leaf_registry.get_leaf_page_count()
    }

    pub async fn rebuild_leaf_registry(self: &Arc<Self>, root_page_id: u64) -> Result<(), StorageError> {
        self.leaf_registry.rebuild_from_tree(self, root_page_id).await
    }

    pub async fn validate_leaf_registry(self: &Arc<Self>, root_page_id: u64) -> Result<bool, StorageError> {
        self.leaf_registry.validate_consistency(self, root_page_id).await
    }

    pub async fn read_pages_batch(&self, page_ids: Vec<u64>) -> Result<Vec<Arc<Page>>, StorageError> {
        let mut pages = Vec::with_capacity(page_ids.len());
        let mut uncached_ids = Vec::new();
        for page_id in &page_ids {
            if let Some(cached_page) = self.buffer_pool.get_page(*page_id) {
                pages.push(cached_page);
            } else {
                uncached_ids.push(*page_id);
            }
        }
        if uncached_ids.is_empty() {
            return Ok(pages);
        }
        uncached_ids.sort_unstable();
        let mut file = self.file.lock().unwrap();
        let mut uncached_pages = Vec::with_capacity(uncached_ids.len());
        for page_id in uncached_ids {
            file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
            let mut buffer = vec![0u8; PAGE_SIZE];
            file.read_exact(&mut buffer)?;
            let page = Page::from_bytes(&buffer)?;
            let page_arc = Arc::new(page);
            self.buffer_pool.put_page(page_id, page_arc.clone());
            uncached_pages.push(page_arc);
        }
        let mut result = Vec::with_capacity(page_ids.len());
        let mut cached_iter = pages.into_iter();
        let mut uncached_iter = uncached_pages.into_iter();
        for page_id in page_ids {
            if self.buffer_pool.get_page(page_id).is_some() {
                result.push(cached_iter.next().unwrap_or_else(|| self.buffer_pool.get_page(page_id).unwrap()));
            } else {
                result.push(uncached_iter.next().unwrap());
            }
        }
        Ok(result)
    }

    pub async fn read_sequential_pages(&self, start_page_id: u64, count: usize) -> Result<Vec<Arc<Page>>, StorageError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut pages = Vec::with_capacity(count);
        let mut current_page_id = Some(start_page_id);
        let mut pages_read = 0;
        while let Some(page_id) = current_page_id {
            if pages_read >= count {
                break;
            }
            let page = if let Some(cached_page) = self.buffer_pool.get_page(page_id) {
                cached_page
            } else {
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
                let mut buffer = vec![0u8; PAGE_SIZE];
                file.read_exact(&mut buffer)?;
                let page = Page::from_bytes(&buffer)?;
                let page_arc = Arc::new(page);
                self.buffer_pool.put_page(page_id, page_arc.clone());
                page_arc
            };
            current_page_id = if page.is_leaf { page.next_leaf_page_id } else { None };
            pages.push(page);
            pages_read += 1;
        }
        Ok(pages)
    }

    pub async fn read_leaf_chain(&self, start_page_id: u64, max_pages: usize) -> Result<(Vec<Arc<Page>>, Option<u64>), StorageError> {
        let mut pages = Vec::with_capacity(max_pages);
        let mut current_page_id = Some(start_page_id);
        while let Some(page_id) = current_page_id {
            if pages.len() >= max_pages {
                break;
            }
            let page = self.read_page(page_id).await?;
            if !page.is_leaf {
                return Err(StorageError::InvalidOperation(format!("Expected leaf page, got internal page: {}", page_id)));
            }
            current_page_id = page.next_leaf_page_id;
            pages.push(page);
        }
        Ok((pages, current_page_id))
    }
}
