use serde::{Deserialize, Serialize};

use crate::schema::Schema;

// Simplified table catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCatalog {
    pub table_name: String,
    pub schema: Schema,
    pub data_file_path: String,
    pub first_page_id: u64,
}

impl TableCatalog {
    pub fn new(table_name: String, schema: Schema, data_file_path: String) -> Self {
        TableCatalog {
            table_name,
            schema,
            data_file_path,
            first_page_id: 1,
        }
    }
}
