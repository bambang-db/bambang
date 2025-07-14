use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::common::StorageError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

/// Leaf Page Registry - maintains a separate file with all leaf page IDs for fast parallel access
/// File format: [magic_number(4)] [count(8)] [page_id_1(8)] [page_id_2(8)] ... [page_id_n(8)]
pub struct LeafPageRegistry {
    file: Arc<Mutex<File>>,
    registry_path: String,
}

const REGISTRY_MAGIC: u32 = 0xDEADBEEF;
const REGISTRY_HEADER_SIZE: usize = 12; // magic(4) + count(8)

impl LeafPageRegistry {
    pub fn new<P: AsRef<Path>>(registry_path: P) -> Result<Self, StorageError> {
        let path_str = registry_path.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&registry_path)?;
        let registry = Self {
            file: Arc::new(Mutex::new(file)),
            registry_path: path_str,
        };
        registry.initialize_if_empty()?;
        Ok(registry)
    }

    fn initialize_if_empty(&self) -> Result<(), StorageError> {
        let mut file = self.file.lock().unwrap();
        let file_size = file.seek(SeekFrom::End(0))?;
        if file_size == 0 {
            file.seek(SeekFrom::Start(0))?;
            file.write_u32::<LittleEndian>(REGISTRY_MAGIC)?;
            file.write_u64::<LittleEndian>(0)?;
            file.flush()?;
        }
        Ok(())
    }

    pub fn add_leaf_page(&self, page_id: u64) -> Result<(), StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(4))?;
        let current_count = file.read_u64::<LittleEndian>()?;
        file.seek(SeekFrom::End(0))?;
        file.write_u64::<LittleEndian>(page_id)?;
        file.seek(SeekFrom::Start(4))?;
        file.write_u64::<LittleEndian>(current_count + 1)?;
        file.flush()?;
        Ok(())
    }

    pub fn remove_leaf_page(&self, page_id: u64) -> Result<bool, StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(4))?;
        let current_count = file.read_u64::<LittleEndian>()?;
        if current_count == 0 {
            return Ok(false);
        }
        let mut page_ids = Vec::with_capacity(current_count as usize);
        file.seek(SeekFrom::Start(REGISTRY_HEADER_SIZE as u64))?;
        for _ in 0..current_count {
            let id = file.read_u64::<LittleEndian>()?;
            page_ids.push(id);
        }
        if let Some(pos) = page_ids.iter().position(|&id| id == page_id) {
            page_ids.remove(pos);
            file.seek(SeekFrom::Start(0))?;
            file.write_u32::<LittleEndian>(REGISTRY_MAGIC)?;
            file.write_u64::<LittleEndian>(page_ids.len() as u64)?;
            for &id in &page_ids {
                file.write_u64::<LittleEndian>(id)?;
            }
            let new_size = REGISTRY_HEADER_SIZE as u64 + (page_ids.len() as u64 * 8);
            file.set_len(new_size)?;
            file.flush()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_all_leaf_pages(&self) -> Result<Vec<u64>, StorageError> {
        let mut file = self.file.lock().unwrap();
        let mut header = [0u8; 12];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != REGISTRY_MAGIC {
            return Err(StorageError::CorruptedData(
                "Invalid registry magic number".into(),
            ));
        }
        let count = u64::from_le_bytes(header[4..12].try_into().unwrap());
        let bytes_to_read = count as usize * 8;
        let mut buffer = vec![0u8; bytes_to_read];
        file.read_exact(&mut buffer)?;
        let mut page_ids = Vec::with_capacity(count as usize);
        for chunk in buffer.chunks_exact(8) {
            let page_id = u64::from_le_bytes(chunk.try_into().unwrap());
            page_ids.push(page_id);
        }
        Ok(page_ids)
    }

    pub fn get_leaf_page_batch(
        &self,
        start_index: usize,
        batch_size: usize,
    ) -> Result<Vec<u64>, StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let magic = file.read_u32::<LittleEndian>()?;
        if magic != REGISTRY_MAGIC {
            return Err(StorageError::CorruptedData(
                "Invalid registry magic number".into(),
            ));
        }
        let count = file.read_u64::<LittleEndian>()?;
        if start_index >= count as usize {
            return Ok(Vec::new());
        }
        let end_index = std::cmp::min(start_index + batch_size, count as usize);
        let actual_batch_size = end_index - start_index;
        let start_offset = REGISTRY_HEADER_SIZE as u64 + (start_index as u64 * 8);
        file.seek(SeekFrom::Start(start_offset))?;
        let mut page_ids = Vec::with_capacity(actual_batch_size);
        for _ in 0..actual_batch_size {
            let page_id = file.read_u64::<LittleEndian>()?;
            page_ids.push(page_id);
        }
        Ok(page_ids)
    }

    pub fn get_leaf_page_count(&self) -> Result<u64, StorageError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let magic = file.read_u32::<LittleEndian>()?;
        if magic != REGISTRY_MAGIC {
            return Err(StorageError::CorruptedData(
                "Invalid registry magic number".into(),
            ));
        }
        let count = file.read_u64::<LittleEndian>()?;
        Ok(count)
    }

    pub async fn rebuild_from_tree(
        &self,
        storage_manager: &Arc<crate::manager::Manager>,
        root_page_id: u64,
    ) -> Result<(), StorageError> {
        use crate::operator::tree::TreeOperations;
        let leftmost_leaf_id = TreeOperations::find_leftmost_leaf(storage_manager, root_page_id)
            .await?
            .ok_or_else(|| StorageError::InvalidOperation("No leftmost leaf found".into()))?;
        let mut leaf_page_ids = Vec::new();
        let mut current_leaf_id = Some(leftmost_leaf_id);
        while let Some(leaf_id) = current_leaf_id {
            leaf_page_ids.push(leaf_id);
            match storage_manager.read_page_header(leaf_id).await {
                Ok((_, is_leaf, next_leaf_page_id)) => {
                    if !is_leaf {
                        break;
                    }
                    current_leaf_id = next_leaf_page_id;
                }
                Err(_) => break,
            }
        }
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.write_u32::<LittleEndian>(REGISTRY_MAGIC)?;
        file.write_u64::<LittleEndian>(leaf_page_ids.len() as u64)?;
        for &page_id in &leaf_page_ids {
            file.write_u64::<LittleEndian>(page_id)?;
        }
        let new_size = REGISTRY_HEADER_SIZE as u64 + (leaf_page_ids.len() as u64 * 8);
        file.set_len(new_size)?;
        file.flush()?;
        Ok(())
    }

    pub async fn validate_consistency(
        &self,
        storage_manager: &Arc<crate::manager::Manager>,
        root_page_id: u64,
    ) -> Result<bool, StorageError> {
        use crate::operator::tree::TreeOperations;
        let registry_pages = self.get_all_leaf_pages()?;
        let leftmost_leaf_id = TreeOperations::find_leftmost_leaf(storage_manager, root_page_id)
            .await?
            .ok_or_else(|| StorageError::InvalidOperation("No leftmost leaf found".into()))?;
        let mut tree_pages = Vec::new();
        let mut current_leaf_id = Some(leftmost_leaf_id);
        while let Some(leaf_id) = current_leaf_id {
            tree_pages.push(leaf_id);
            match storage_manager.read_page_header(leaf_id).await {
                Ok((_, is_leaf, next_leaf_page_id)) => {
                    if !is_leaf {
                        break;
                    }
                    current_leaf_id = next_leaf_page_id;
                }
                Err(_) => break,
            }
        }
        Ok(registry_pages == tree_pages)
    }
}
