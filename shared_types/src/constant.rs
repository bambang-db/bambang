// Constants used across modules
pub const PAGE_SIZE: usize = 1024; // 1KB for testing only
pub const NODE_HEADER_SIZE: usize = 16; // Basic node metadata
pub const MAX_KEYS_PER_NODE: usize = 128; // Configurable based on key size
pub const MIN_KEYS_PER_NODE: usize = 63; // Configurable based on key size
pub const MAGIC_NUMBER: u32 = 0xDEADBEEF; // File format identifier