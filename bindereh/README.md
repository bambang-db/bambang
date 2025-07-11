# Bindereh Storage Engine Context

## Overview

Bindereh is a storage engine that implements a B+ tree-based storage system. It provides functionality for storing, retrieving, updating, and deleting data efficiently. The storage engine is designed with a layered architecture that separates concerns and provides a clean API for higher-level components.

## Core Components

### Common (common.rs)

- Defines `StorageError` for error handling across the storage engine
- Provides constants like `PAGE_SIZE`, `NODE_HEADER_SIZE`, `MAX_KEYS_PER_NODE`, and `MAGIC_NUMBER`
- Input: N/A
- Output: Error types and constants used throughout the system

### Value System (value.rs)

- Implements a rich type system through the `Value` enum
- Supports various data types: Integer, String, Float, Boolean, etc.
- Provides serialization/deserialization for storage on disk
- Input: Raw data of various types
- Output: Serialized byte representations for disk storage

### Page Structure (page.rs)

- Defines the `Page` and `Row` structures that represent nodes in the B+ tree
- Provides methods for serializing/deserializing pages to/from bytes
- Pages can be either leaf nodes (containing rows) or internal nodes (containing child pointers)
- Input: Raw page data from disk
- Output: Structured page objects for in-memory operations

### Buffer Pool (pool.rs)

- Manages a cache of recently accessed pages
- Tracks dirty pages that need to be written back to disk
- Implements simple eviction policy for cache management
- Input: Page IDs to retrieve or store
- Output: Cached page objects for faster access

### Storage Manager (manager.rs)

- Manages the on-disk representation of the database
- Handles reading and writing pages to/from disk
- Coordinates with the buffer pool for caching
- Allocates new pages as needed
- Input: File operations and page requests
- Output: Page objects read from disk or confirmation of writes

### Executor (executor.rs)

- Orchestrates query execution operations
- Manages the root page ID for the B+ tree
- Delegates to specific operators for different operations
- Maintains the tree structure during operations
- Input: Query operations (insert, scan, update, delete)
- Output: Results of query operations

### Operators

- Specialized components that perform specific database operations:
  - **TreeOperations (tree.rs)**: Core B+ tree operations like finding leaf nodes, splitting nodes, etc.
  - **ScanOperation (scan.rs)**: Traverses the B+ tree to retrieve rows based on criteria
  - **InsertOperation (insert.rs)**: Adds new rows to the database
  - **UpdateOperation (update.rs)**: Modifies existing rows
  - **DeleteOperation (delete.rs)**: Removes rows from the database
  - **TreePrinter (print.rs)**: Debugging utility to visualize the B+ tree structure
- Input: Specific operation parameters
- Output: Results of the operation

## Component Interactions

### Data Storage Flow

1. The `Executor` receives an insert request with a `Row` object
2. It uses `TreeOperations` to find the appropriate leaf node
3. The row is inserted into the leaf node
4. If the node becomes too full, it's split using B+ tree algorithms
5. The `Manager` writes modified pages back to disk
6. The `Pool` caches pages for future access

### Data Retrieval Flow

1. The `Executor` receives a scan request with `ScanOptions`
2. It delegates to the `ScanOperation` component
3. `ScanOperation` finds the leftmost leaf of the tree
4. It sequentially scans through leaf nodes, following next-leaf pointers
5. Rows matching the criteria are collected and returned
6. The `Manager` reads pages from disk as needed
7. The `Pool` caches these pages for faster access in future operations

### Buffer Management Flow

1. When a page is requested by ID, the `Manager` first checks the `Pool`
2. If found in cache, the page is returned directly
3. If not found, the `Manager` reads the page from disk
4. The page is added to the `Pool` cache
5. When a page is modified, it's marked as dirty
6. Dirty pages are written back to disk by the `Manager`
7. After writing, dirty flags are cleared

### B+ Tree Operations

1. **Insert**:
   - Find appropriate leaf node
   - Insert the row
   - Split nodes if necessary
   - Update parent pointers
   - Create new root if needed
2. **Scan**:

   - Find leftmost leaf
   - Sequentially traverse leaf nodes
   - Filter rows based on predicates
   - Apply projections, limits, ordering

3. **Update**:
   - Find the row by key
   - Replace with new row data
4. **Delete**:
   - Find the row by key
   - Remove from leaf node
   - Handle tree rebalancing if needed

## Performance Considerations

- The buffer pool provides in-memory caching to reduce disk I/O
- Pages are read and written in fixed-size blocks for efficiency
- B+ tree structure enables efficient range queries through leaf node linkage
- Dirty page tracking minimizes unnecessary disk writes
- Page serialization ensures compact on-disk representation

## Debug Utilities

- `TreePrinter` visualizes the B+ tree structure for debugging purposes
- Shows both the hierarchical structure and leaf-level connections
- Helps verify the correctness of B+ tree operations

## Integration with Other Modules

Bindereh serves as the storage engine for a larger database system:

1. **Diplomat**: Handles SQL parsing and logical planning
2. **Matan**: Manages database catalogs and schemas
3. **Pambudi**: Optimizes queries for efficient execution

Bindereh provides the low-level storage operations that these higher-level components depend on.
