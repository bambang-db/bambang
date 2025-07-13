use crate::operator::compare::{
    evaluate_predicate_optimized, evaluate_predicate_optimized_static, extract_predicate_column_indices, sort_rows
};
use crate::{manager::Manager, operator::tree::TreeOperations};
use shared_types::{
    ScanOptions, ScanResult, Schema, StorageError,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::{cmp::Ordering, collections::HashMap, sync::Arc};
use tokio::task::JoinSet;

pub struct ScanOperation {
    storage_manager: Arc<Manager>,
    max_workers: usize,
}

impl ScanOperation {
    pub fn new(storage_manager: Arc<Manager>, max_workers: usize) -> Self {
        Self {
            storage_manager,
            max_workers,
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
        let mut result_rows = Vec::new();
        let mut pages_read = 0;
        let mut total_scanned = 0;
        let mut filtered_count = 0;
        let mut current_leaf_id = Some(start_leaf_id);
        if let Some(limit) = options.limit {
            result_rows.reserve(limit);
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
        let effective_limit = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => Some(limit + offset),
            (Some(limit), None) => Some(limit),
            _ => None,
        };
        while let Some(leaf_id) = current_leaf_id {
            let leaf_page = self.storage_manager.read_page(leaf_id).await?;
            pages_read += 1;
            for row in &leaf_page.values {
                total_scanned += 1;
                if let Some(eff_limit) = effective_limit {
                    if filtered_count >= eff_limit {
                        break;
                    }
                }
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
            if let Some(eff_limit) = effective_limit {
                if filtered_count >= eff_limit {
                    break;
                }
            }
            current_leaf_id = leaf_page.next_leaf_page_id;
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
        Ok(ScanResult {
            rows: result_rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema,
        })
    }

    async fn parallel_scan(&self, options: ScanOptions) -> Result<ScanResult, StorageError> {
        // For small limits, use sequential scan as it's more efficient
        if let Some(limit) = options.limit {
            if limit < 1000 {
                // Fall back to streaming approach for small queries
                let leftmost_leaf_id = TreeOperations::find_leftmost_leaf(&self.storage_manager, 1)
                    .await?
                    .expect("Cannot get leftmost_leaf_id");
                return self.sequential_scan(leftmost_leaf_id, options).await;
            }
        }

        let all_leaf_page_ids = self.storage_manager.get_all_leaf_page_ids().await?;

        if all_leaf_page_ids.is_empty() {
            return Ok(ScanResult {
                rows: Vec::new(),
                total_scanned: 0,
                pages_read: 0,
                filtered_count: 0,
                result_schema: options.schema,
            });
        }

        // Prepare shared data for workers
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

        // Calculate optimal batch size per worker
        let total_pages = all_leaf_page_ids.len();
        let pages_per_worker = (total_pages + self.max_workers - 1) / self.max_workers;
        let pages_per_worker = std::cmp::max(pages_per_worker, 1);

        // Shared counters for early termination
        let total_rows_found = Arc::new(AtomicUsize::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));
        let effective_limit = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => Some(limit + offset),
            (Some(limit), None) => Some(limit),
            _ => None,
        };

        // Create worker tasks with direct page ID batches
        let mut join_set = JoinSet::new();

        for worker_id in 0..self.max_workers {
            let start_idx = worker_id * pages_per_worker;
            if start_idx >= total_pages {
                break; // No more pages for this worker
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

        // Collect results from all workers
        let mut all_rows = Vec::new();
        let mut total_pages_read = 0;
        let mut total_scanned = 0;
        let mut total_filtered = 0;

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(worker_result)) => {
                    all_rows.extend(worker_result.rows);
                    total_pages_read += worker_result.pages_read;
                    total_scanned += worker_result.total_scanned;
                    total_filtered += worker_result.filtered_count;
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

        // Apply sorting if required
        if let Some(ref order_by) = options.order_by {
            if let Some(ref schema) = result_schema {
                if !all_rows.is_empty() {
                    sort_rows(&mut all_rows, order_by, schema);
                }
            }
        }

        // Apply offset and limit
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

    /// Optimized parallel scan with early termination and smart work distribution
    async fn streaming_parallel_scan(
        &self,
        start_leaf_id: u64,
        options: ScanOptions,
    ) -> Result<ScanResult, StorageError> {
        use tokio::sync::mpsc;
        use {AtomicBool, AtomicUsize, Ordering};

        // For small limits, use sequential scan as it's more efficient
        if let Some(limit) = options.limit {
            if limit < 1000 {
                return self.sequential_scan(start_leaf_id, options).await;
            }
        }

        // Prepare shared data for workers
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

        // Shared counters for early termination
        let total_rows_found = Arc::new(AtomicUsize::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));
        let effective_limit = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => Some(limit + offset),
            (Some(limit), None) => Some(limit),
            _ => None,
        };

        // Create channel for streaming page IDs to workers
        let (page_tx, page_rx) = mpsc::unbounded_channel::<u64>();
        let page_rx = Arc::new(tokio::sync::Mutex::new(page_rx));

        // Spawn page ID producer task with early termination
        let storage_manager_producer = Arc::clone(&self.storage_manager);
        let producer_should_stop = Arc::clone(&should_stop);
        let producer_handle = tokio::spawn(async move {
            let mut current_leaf_id = Some(start_leaf_id);
            let mut page_count = 0;

            while let Some(leaf_id) = current_leaf_id {
                // Check if we should stop early
                if producer_should_stop.load(AtomicOrdering::Relaxed) {
                    break;
                }

                // Send page ID to workers immediately
                if page_tx.send(leaf_id).is_err() {
                    break; // Channel closed, workers are done
                }
                page_count += 1;

                // Use optimized header-only read to get next page ID
                match storage_manager_producer.read_page_header(leaf_id).await {
                    Ok((_, is_leaf, next_leaf_page_id)) => {
                        if !is_leaf {
                            break; // Should not happen in leaf traversal
                        }
                        current_leaf_id = next_leaf_page_id;
                    }
                    Err(_) => break,
                }
            }

            drop(page_tx); // Signal end of pages
            page_count
        });

        // Create worker tasks with early termination support
        let mut join_set = JoinSet::new();
        let num_workers = self.max_workers;

        for _worker_id in 0..num_workers {
            let storage_manager = Arc::clone(&self.storage_manager);
            let worker_options = options.clone();
            let worker_projection_indices = projection_indices.clone();
            let worker_predicate_indices = predicate_column_indices.clone();
            let worker_page_rx = Arc::clone(&page_rx);
            let worker_total_rows = Arc::clone(&total_rows_found);
            let worker_should_stop = Arc::clone(&should_stop);

            join_set.spawn(async move {
                Self::streaming_worker_scan_with_limit(
                    storage_manager,
                    worker_page_rx,
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

        // Collect results from all workers
        let mut all_rows = Vec::new();
        let mut total_pages_read = 0;
        let mut total_scanned = 0;
        let mut total_filtered = 0;

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(worker_result)) => {
                    all_rows.extend(worker_result.rows);
                    total_pages_read += worker_result.pages_read;
                    total_scanned += worker_result.total_scanned;
                    total_filtered += worker_result.filtered_count;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(StorageError::InvalidOperation(format!(
                        "Streaming worker task failed: {}",
                        e
                    )));
                }
            }
        }

        // Wait for producer to finish and get total page count
        let _total_pages = producer_handle
            .await
            .map_err(|e| StorageError::InvalidOperation(format!("Producer task failed: {}", e)))?;

        // Apply sorting if required
        if let Some(ref order_by) = options.order_by {
            if let Some(ref schema) = result_schema {
                if !all_rows.is_empty() {
                    sort_rows(&mut all_rows, order_by, schema);
                }
            }
        }

        // Apply offset and limit
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

    /// Enhanced streaming worker with early termination support
    async fn streaming_worker_scan_with_limit(
        storage_manager: Arc<Manager>,
        page_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<u64>>>,
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

        // Pre-allocate based on estimated capacity
        if let Some(limit) = options.limit {
            result_rows.reserve(limit / 4); // Conservative estimate for worker share
        }

        loop {
            // Check if we should stop early due to limit reached
            if let Some(limit) = effective_limit {
                if total_rows_found.load(AtomicOrdering::Relaxed) >= limit {
                    should_stop.store(true, AtomicOrdering::Relaxed);
                    break;
                }
            }

            // Get next page ID from the channel
            let page_id = {
                let mut rx = page_rx.lock().await;
                match rx.recv().await {
                    Some(id) => id,
                    None => break, // Channel closed, no more pages
                }
            };

            let leaf_page = storage_manager.read_page(page_id).await?;
            pages_read += 1;

            for row in &leaf_page.values {
                total_scanned += 1;

                // Apply predicate filtering
                if let Some(ref predicate) = options.predicate {
                    if let Some(ref schema) = options.schema {
                        if !evaluate_predicate_optimized_static(
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

                // Update global counter and check limit
                let current_total = total_rows_found.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                if let Some(limit) = effective_limit {
                    if current_total > limit {
                        should_stop.store(true, AtomicOrdering::Relaxed);
                        break;
                    }
                }

                // Apply projection
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

            // Check if we should stop after processing this page
            if should_stop.load(AtomicOrdering::Relaxed) {
                break;
            }
        }

        Ok(ScanResult {
            rows: result_rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema: None, // Will be set by the main function
        })
    }


    /// Registry-based worker that processes a batch of page IDs with early termination
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

        // Pre-allocate based on estimated capacity
        if let Some(limit) = options.limit {
            result_rows.reserve(limit / 4); // Conservative estimate for worker share
        }

        for page_id in page_ids {
            // Check for early termination
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

                // Check early termination again for fine-grained control
                if let Some(limit) = effective_limit {
                    if total_rows_found.load(AtomicOrdering::Relaxed) >= limit {
                        should_stop.store(true, AtomicOrdering::Relaxed);
                        break;
                    }
                }

                // Apply predicate filtering
                if let Some(ref predicate) = options.predicate {
                    if let Some(ref schema) = options.schema {
                        if !evaluate_predicate_optimized_static(
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

                // Apply projection
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

            // Break out of page loop if we should stop
            if should_stop.load(AtomicOrdering::Relaxed) {
                break;
            }
        }

        Ok(ScanResult {
            rows: result_rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema: None, // Will be set by the main function
        })
    }
}
