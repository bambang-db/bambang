use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use bincode::{
    Decode, Encode,
    config::{legacy, standard},
    encode_into_slice,
};
use serde::{Deserialize, Serialize};

use crate::{common::CatalogError, table::TableCatalog};

// Database catalog - single file approach
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DatabaseCatalog {
    pub database_name: String,
    pub tables: HashMap<String, TableCatalog>, // Store table catalogs directly
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TestEncode {
    pub version: usize,
}

impl DatabaseCatalog {
    pub fn new(database_name: String) -> Self {
        DatabaseCatalog {
            database_name,
            tables: HashMap::new(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, CatalogError> {
        let encoded: Vec<u8> = bincode::encode_to_vec(self, standard()).unwrap();
        Ok(encoded)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, CatalogError> {
        let (decoded, _len): (DatabaseCatalog, usize) =
            bincode::decode_from_slice(data, standard()).unwrap();
        Ok(decoded)
    }

    pub fn save_to_file(&self, path: &str) -> Result<(), CatalogError> {
        let data = self.to_bytes()?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&data)?;
        file.flush()?;
        Ok(())
    }

    pub fn load_from_file(path: &str) -> Result<Self, CatalogError> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::from_bytes(&data)
    }
}
