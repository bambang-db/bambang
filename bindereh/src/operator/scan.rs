use crate::operator::compare::{
    evaluate_predicate_optimized, evaluate_predicate_optimized_static,
    extract_predicate_column_indices, sort_rows, evaluate_predicate_fast,
};
use crate::{manager::Manager, operator::tree::TreeOperations, page::Page};
use shared_types::{ScanOptions, ScanResult, Schema, StorageError};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct ReadAheadConfig {
    pub buffer_size: usize,
    pub enabled: bool,
    pub prefetch_threshold: usize,
}

impl Default for ReadAheadConfig {
    fn default() -> Self {
        Self {
            buffer_size: 256,  // Increased from 64 to 256 pages
            enabled: true,
            prefetch_threshold: 16,  // Increased from 4 to 16 pages
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReadAheadMetrics {
    pub pages_requested: usize,
    pub buffer_hits: usize,
    pub buffer_misses: usize,
    pub pages_prefetched: usize,
    pub prefetch_hits: usize,
    pub sequential_access_ratio: f64,
}

impl ReadAheadMetrics {
    pub fn hit_rate(&self) -> f64 {
        if self.pages_requested == 0 {
            0.0
        } else {
            self.buffer_hits as f64 / self.pages_requested as f64
        }
    }

    pub fn prefetch_efficiency(&self) -> f64 {
        if self.pages_prefetched == 0 {
            0.0
        } else {
            self.prefetch_hits as f64 / self.pages_prefetched as f64
        }
    }
}

#[derive(Debug)]
struct ReadAheadBuffer {
    buffer: VecDeque<Arc<Page>>,
    config: ReadAheadConfig,
    metrics: ReadAheadMetrics,
    current_page_id: Option<u64>,
    next_expected_page_id: Option<u64>,
}

impl ReadAheadBuffer {
    fn new(config: ReadAheadConfig) -> Self {
        Self {
            buffer: VecDeque::with_capacity(config.buffer_size),
            config,
            metrics: ReadAheadMetrics::default(),
            current_page_id: None,
            next_expected_page_id: None,
        }
    }

    fn get_page(&mut self, page_id: u64) -> Option<Arc<Page>> {
        self.metrics.pages_requested += 1;

        if let Some(expected_id) = self.next_expected_page_id {
            if page_id == expected_id {
                self.metrics.sequential_access_ratio = (self.metrics.sequential_access_ratio
                    * (self.metrics.pages_requested - 1) as f64
                    + 1.0)
                    / self.metrics.pages_requested as f64;
            }
        }

        if let Some(pos) = self.buffer.iter().position(|p| p.page_id == page_id) {
            self.metrics.buffer_hits += 1;
            let page = self.buffer.remove(pos).unwrap();
            self.current_page_id = Some(page_id);
            self.next_expected_page_id = page.next_leaf_page_id;
            Some(page)
        } else {
            self.metrics.buffer_misses += 1;
            None
        }
    }

    fn add_pages(&mut self, pages: Vec<Arc<Page>>) {
        for page in pages {
            if self.buffer.len() < self.config.buffer_size {
                self.buffer.push_back(page);
                self.metrics.pages_prefetched += 1;
            }
        }
    }

    fn should_prefetch(&self) -> bool {
        self.config.enabled && self.buffer.len() <= self.config.prefetch_threshold
    }

    fn get_prefetch_start_id(&self) -> Option<u64> {
        self.buffer
            .back()
            .and_then(|page| page.next_leaf_page_id)
            .or(self.next_expected_page_id)
    }

    fn clear(&mut self) {
        self.buffer.clear();
    }

    fn get_metrics(&self) -> ReadAheadMetrics {
        self.metrics.clone()
    }
}

pub struct ScanOperation {
    storage_manager: Arc<Manager>,
    max_workers: usize,
    read_ahead_config: ReadAheadConfig,
}

impl ScanOperation {
    pub fn new(storage_manager: Arc<Manager>, max_workers: usize) -> Self {
        Self {
            storage_manager,
            max_workers,
            read_ahead_config: ReadAheadConfig::default(),
        }
    }

    pub fn with_read_ahead_config(mut self, config: ReadAheadConfig) -> Self {
        self.read_ahead_config = config;
        self
    }

    async fn prefetch_pages(
        &self,
        start_page_id: u64,
        count: usize,
    ) -> Result<Vec<Arc<Page>>, StorageError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let prefetch_future = self.storage_manager.read_leaf_chain(start_page_id, count);

        match tokio::time::timeout(std::time::Duration::from_secs(30), prefetch_future).await {
            Ok(Ok((pages, _))) => Ok(pages),
            Ok(Err(e)) => {
                eprintln!("Prefetch error for page {}: {:?}", start_page_id, e);
                Err(e)
            }
            Err(_) => {
                Err(StorageError::InvalidOperation(format!(
                    "Prefetch timeout for page {}",
                    start_page_id
                )))
            }
        }
    }

    async fn safe_read_page(&self, page_id: u64) -> Result<Arc<Page>, StorageError> {
        match self.storage_manager.read_page(page_id).await {
            Ok(page) => Ok(page),
            Err(e) => {
                eprintln!("Page read error for page {}: {:?}, retrying...", page_id, e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                self.storage_manager
                    .read_page(page_id)
                    .await
                    .map_err(|retry_err| {
                        eprintln!(
                            "Page read retry failed for page {}: {:?}",
                            page_id, retry_err
                        );
                        retry_err
                    })
            }
        }
    }

    pub async fn execute(
        &self,
        root_page_id: u64,
        options: ScanOptions,
    ) -> Result<ScanResult, StorageError> {
        let leftmost_leaf_id =
            TreeOperations::find_leftmost_leaf(&self.storage_manager, root_page_id)
                .await?
                .unwrap();
        if options.parallel && self.max_workers > 1 {
            self.parallel_scan(options).await
        } else {
            self.sequential_scan(leftmost_leaf_id, options).await
        }
    }

    async fn sequential_scan(
        &self,
        start_leaf_id: u64,
        options: ScanOptions,
    ) -> Result<ScanResult, StorageError> {
        let mut read_ahead_buffer = ReadAheadBuffer::new(self.read_ahead_config.clone());

        let mut result_rows = Vec::new();
        if let Some(limit) = options.limit {
            result_rows.reserve(limit);
        }

        let mut pages_read = 0;
        let mut total_scanned = 0;
        let mut filtered_count = 0;
        let mut current_leaf_id = Some(start_leaf_id);

        let projection_indices =
            if let (Some(projection), Some(schema)) = (&options.projection, &options.schema) {
                schema.get_column_indices(projection)
            } else {
                None
            };

        let result_schema =
            if let (Some(projection), Some(schema)) = (&options.projection, &options.schema) {
                let projected_columns: Vec<_> = projection
                    .iter()
                    .filter_map(|col| schema.get_column(col).cloned())
                    .collect();
                Some(Schema::new(projected_columns))
            } else {
                options.schema.clone()
            };

        let predicate_column_indices =
            if let (Some(predicate), Some(schema)) = (&options.predicate, &options.schema) {
                Some(extract_predicate_column_indices(predicate, schema))
            } else {
                None
            };

        let effective_limit = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => Some(limit + offset),
            (Some(limit), None) => Some(limit),
            _ => None,
        };

        if self.read_ahead_config.enabled {
            if let Ok(initial_pages) = self
                .prefetch_pages(start_leaf_id, self.read_ahead_config.buffer_size)
                .await
            {
                read_ahead_buffer.add_pages(initial_pages);
            }
        }

        while let Some(leaf_id) = current_leaf_id {
            let leaf_page = if let Some(buffered_page) = read_ahead_buffer.get_page(leaf_id) {
                buffered_page
            } else {
                match self.safe_read_page(leaf_id).await {
                    Ok(page) => page,
                    Err(e) => {
                        eprintln!("Failed to read page {} during scan: {:?}", leaf_id, e);
                        if let Some(next_id) = current_leaf_id {
                            current_leaf_id = Some(next_id);
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            };

            pages_read += 1;

            // Batch process rows for better performance
            let mut batch_filtered = Vec::new();
            
            for row in &leaf_page.values {
                total_scanned += 1;

                // Early termination check
                if let Some(eff_limit) = effective_limit {
                    if filtered_count >= eff_limit {
                        current_leaf_id = None;
                        break;
                    }
                }

                // Fast predicate evaluation - fail fast on first condition
                if let Some(ref predicate) = options.predicate {
                    if let Some(ref schema) = options.schema {
                        if !evaluate_predicate_optimized(
                            predicate,
                            row,
                            schema,
                            &predicate_column_indices,
                        ) {
                            continue;
                        }
                    }
                }

                filtered_count += 1;
                batch_filtered.push(row);
                
                // Process in batches of 1000 for better cache locality
                if batch_filtered.len() >= 1000 {
                    for batch_row in batch_filtered.drain(..) {
                        let projected_row = if let Some(ref indices) = projection_indices {
                            if let Some(ref schema) = options.schema {
                                schema.project_row(batch_row, indices)
                            } else {
                                batch_row.clone()
                            }
                        } else {
                            batch_row.clone()
                        };
                        result_rows.push(projected_row);
                    }
                }
            }
            
            // Process remaining batch
            for batch_row in batch_filtered {
                let projected_row = if let Some(ref indices) = projection_indices {
                    if let Some(ref schema) = options.schema {
                        schema.project_row(batch_row, indices)
                    } else {
                        batch_row.clone()
                    }
                } else {
                    batch_row.clone()
                };
                result_rows.push(projected_row);
            }

            if let Some(eff_limit) = effective_limit {
                if filtered_count >= eff_limit {
                    break;
                }
            }

            current_leaf_id = leaf_page.next_leaf_page_id;

            if self.read_ahead_config.enabled && read_ahead_buffer.should_prefetch() {
                if let Some(prefetch_start_id) = read_ahead_buffer.get_prefetch_start_id() {
                    let storage_manager = Arc::clone(&self.storage_manager);
                    let config = self.read_ahead_config.clone();
                    tokio::spawn(async move {
                        let _ = storage_manager
                            .read_leaf_chain(prefetch_start_id, config.buffer_size)
                            .await;
                    });
                }
            }
        }

        if let Some(ref order_by) = options.order_by {
            if let Some(ref schema) = result_schema {
                if !result_rows.is_empty() {
                    sort_rows(&mut result_rows, order_by, schema);
                }
            }
        }

        if let Some(offset) = options.offset {
            if offset < result_rows.len() {
                result_rows.drain(0..offset);
            } else {
                result_rows.clear();
            }
        }

        if let Some(limit) = options.limit {
            if result_rows.len() > limit {
                result_rows.truncate(limit);
            }
        }

        let read_ahead_metrics = read_ahead_buffer.get_metrics();

        Ok(ScanResult {
            rows: result_rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema,
        })
    }

    async fn parallel_scan(&self, options: ScanOptions) -> Result<ScanResult, StorageError> {
        let now = Instant::now();
        let all_leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;
        let duration = now.elapsed();
        println!(
            "✅ all_leaf_page_ids {:.2}ms",
            duration.as_secs_f64() * 1000.0
        );

        if all_leaf_page_ids.is_empty() {
            return Ok(ScanResult {
                rows: Vec::new(),
                total_scanned: 0,
                pages_read: 0,
                filtered_count: 0,
                result_schema: options.schema,
            });
        }

        let projection_indices =
            if let (Some(projection), Some(schema)) = (&options.projection, &options.schema) {
                schema.get_column_indices(projection)
            } else {
                None
            };

        let result_schema =
            if let (Some(projection), Some(schema)) = (&options.projection, &options.schema) {
                let projected_columns: Vec<_> = projection
                    .iter()
                    .filter_map(|col| schema.get_column(col).cloned())
                    .collect();
                Some(Schema::new(projected_columns))
            } else {
                options.schema.clone()
            };

        let predicate_column_indices =
            if let (Some(predicate), Some(schema)) = (&options.predicate, &options.schema) {
                Some(extract_predicate_column_indices(predicate, schema))
            } else {
                None
            };

        let total_pages = all_leaf_page_ids.len();
        let pages_per_worker = (total_pages + self.max_workers - 1) / self.max_workers;
        let pages_per_worker = std::cmp::max(pages_per_worker, 1);

        let total_rows_found = Arc::new(AtomicUsize::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));
        let effective_limit = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => Some(limit + offset),
            (Some(limit), None) => Some(limit),
            _ => None,
        };

        let mut join_set = JoinSet::new();
        let now = Instant::now();

        for worker_id in 0..self.max_workers {
            let start_idx = worker_id * pages_per_worker;
            if start_idx >= total_pages {
                break;
            }

            let end_idx = std::cmp::min(start_idx + pages_per_worker, total_pages);
            let worker_page_ids: Vec<u64> = all_leaf_page_ids[start_idx..end_idx].to_vec();

            if worker_page_ids.is_empty() {
                continue;
            }

            let storage_manager = Arc::clone(&self.storage_manager);
            let worker_options = options.clone();
            let worker_projection_indices = projection_indices.clone();
            let worker_predicate_indices = predicate_column_indices.clone();
            let worker_total_rows = Arc::clone(&total_rows_found);
            let worker_should_stop = Arc::clone(&should_stop);

            join_set.spawn(async move {
                Self::registry_worker_scan_with_limit(
                    storage_manager,
                    worker_page_ids,
                    worker_options,
                    worker_projection_indices,
                    worker_predicate_indices,
                    worker_total_rows,
                    worker_should_stop,
                    effective_limit,
                )
                .await
            });
        }

        let duration = now.elapsed();
        println!("✅ join_set.spawn {:.2}ms", duration.as_secs_f64() * 1000.0);

        let now = Instant::now();
        let mut all_rows = Vec::new();
        let mut total_pages_read = 0;
        let mut total_scanned = 0;
        let mut total_filtered = 0;
        let mut pending_tasks = join_set.len();

        if let Some(limit) = options.limit {
            all_rows.reserve(limit);
        }

        while pending_tasks > 0 {
            tokio::select! {
                result = join_set.join_next() => {
                    if let Some(result) = result {
                        pending_tasks -= 1;
                        match result {
                            Ok(Ok(worker_result)) => {
                                all_rows.extend(worker_result.rows);
                                total_pages_read += worker_result.pages_read;
                                total_scanned += worker_result.total_scanned;
                                total_filtered += worker_result.filtered_count;

                                if let Some(effective_limit) = effective_limit {
                                    if total_filtered >= effective_limit {
                                        join_set.shutdown().await;
                                        break;
                                    }
                                }
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(StorageError::InvalidOperation(format!(
                                    "Registry worker task failed: {}",
                                    e
                                )));
                            }
                        }
                    }
                }
                _ = tokio::task::yield_now() => {
                    continue;
                }
            }
        }

        let duration = now.elapsed();
        println!("✅ collect_result {:.2}ms", duration.as_secs_f64() * 1000.0);

        if let Some(ref order_by) = options.order_by {
            if let Some(ref schema) = result_schema {
                if !all_rows.is_empty() {
                    sort_rows(&mut all_rows, order_by, schema);
                }
            }
        }

        if let Some(offset) = options.offset {
            if offset < all_rows.len() {
                all_rows.drain(0..offset);
            } else {
                all_rows.clear();
            }
        }

        if let Some(limit) = options.limit {
            if all_rows.len() > limit {
                all_rows.truncate(limit);
            }
        }

        Ok(ScanResult {
            rows: all_rows,
            total_scanned,
            pages_read: total_pages_read,
            filtered_count: total_filtered,
            result_schema,
        })
    }

    async fn registry_worker_scan_with_limit(
        storage_manager: Arc<Manager>,
        page_ids: Vec<u64>,
        options: ScanOptions,
        projection_indices: Option<Vec<usize>>,
        predicate_column_indices: Option<HashMap<String, usize>>,
        total_rows_found: Arc<AtomicUsize>,
        should_stop: Arc<AtomicBool>,
        effective_limit: Option<usize>,
    ) -> Result<ScanResult, StorageError> {
        let mut result_rows = Vec::new();
        let mut pages_read = 0;
        let mut total_scanned = 0;
        let mut filtered_count = 0;

        if let Some(limit) = options.limit {
            result_rows.reserve(limit / 4);
        }

        for page_id in page_ids {
            if should_stop.load(AtomicOrdering::Relaxed) {
                break;
            }

            if let Some(limit) = effective_limit {
                if total_rows_found.load(AtomicOrdering::Relaxed) >= limit {
                    should_stop.store(true, AtomicOrdering::Relaxed);
                    break;
                }
            }

            let leaf_page = storage_manager.read_page(page_id).await?;
            pages_read += 1;

            for row in &leaf_page.values {
                total_scanned += 1;

                if let Some(limit) = effective_limit {
                    if total_rows_found.load(AtomicOrdering::Relaxed) >= limit {
                        should_stop.store(true, AtomicOrdering::Relaxed);
                        break;
                    }
                }

                if let Some(ref predicate) = options.predicate {
                    if let Some(ref schema) = options.schema {
                        if let Some(ref indices) = predicate_column_indices {
                            if !evaluate_predicate_fast(predicate, row, indices) {
                                continue;
                            }
                        } else if !evaluate_predicate_optimized_static(
                            predicate,
                            row,
                            schema,
                            &predicate_column_indices,
                        ) {
                            continue;
                        }
                    }
                }

                filtered_count += 1;
                total_rows_found.fetch_add(1, AtomicOrdering::Relaxed);

                let projected_row = if let Some(ref indices) = projection_indices {
                    if let Some(ref schema) = options.schema {
                        schema.project_row(row, indices)
                    } else {
                        row.clone()
                    }
                } else {
                    row.clone()
                };

                result_rows.push(projected_row);
            }

            if should_stop.load(AtomicOrdering::Relaxed) {
                break;
            }
        }

        Ok(ScanResult {
            rows: result_rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema: None,
        })
    }
}