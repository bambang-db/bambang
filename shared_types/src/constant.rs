// Constants used across modules
pub const PAGE_SIZE: usize = 2048; // 2KB pages like most databases
pub const NODE_HEADER_SIZE: usize = 16; // Basic node metadata
pub const MAX_KEYS_PER_NODE: usize = 4; // Configurable based on key size
pub const MAGIC_NUMBER: u32 = 0xDEADBEEF; // File format identifier