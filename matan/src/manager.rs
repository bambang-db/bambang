use std::path::Path;

use crate::{common::CatalogError, database::DatabaseCatalog, schema::Schema, table::TableCatalog};

pub struct CatalogManager {
    pub catalog_file: String,
    pub database_catalog: DatabaseCatalog,
}

impl CatalogManager {
    pub fn new(
        catalog_file: impl AsRef<Path>,
        database_name: String,
    ) -> Result<Self, CatalogError> {
        let catalog_file = catalog_file.as_ref().to_string_lossy().to_string();

        let database_catalog = if Path::new(&catalog_file).exists() {
            DatabaseCatalog::load_from_file(&catalog_file)?
        } else {
            let catalog = DatabaseCatalog::new(database_name);
            catalog.save_to_file(&catalog_file)?;
            catalog
        };

        Ok(CatalogManager {
            catalog_file,
            database_catalog,
        })
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        schema: Schema,
        data_file_path: String,
    ) -> Result<(), CatalogError> {
        if self.database_catalog.tables.contains_key(&table_name) {
            return Err(CatalogError::TableExists(table_name));
        }

        let table_catalog = TableCatalog::new(table_name.clone(), schema, data_file_path);
        self.database_catalog
            .tables
            .insert(table_name, table_catalog);

        self.save_catalog()?;
        Ok(())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        if self.database_catalog.tables.remove(table_name).is_some() {
            self.save_catalog()?;
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(table_name.to_string()))
        }
    }

    pub fn get_table_catalog(&self, table_name: &str) -> Option<&TableCatalog> {
        self.database_catalog.tables.get(table_name)
    }

    pub fn get_table_catalog_mut(&mut self, table_name: &str) -> Option<&mut TableCatalog> {
        self.database_catalog.tables.get_mut(table_name)
    }

    pub fn update_table_stats(
        &mut self,
        table_name: &str,
        first_page_id: u64,
    ) -> Result<(), CatalogError> {
        if let Some(catalog) = self.database_catalog.tables.get_mut(table_name) {
            catalog.first_page_id = first_page_id;
            self.save_catalog()?;
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(table_name.to_string()))
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.database_catalog.tables.keys().cloned().collect()
    }

    pub fn get_schema(&self, table_name: &str) -> Option<&Schema> {
        self.database_catalog
            .tables
            .get(table_name)
            .map(|c| &c.schema)
    }

    pub fn get_table_data_file(&self, table_name: &str) -> Option<&str> {
        self.database_catalog
            .tables
            .get(table_name)
            .map(|c| c.data_file_path.as_str())
    }

    pub fn update_table_data_file(
        &mut self,
        table_name: &str,
        new_path: String,
        first_page_id: u64,
    ) -> Result<(), CatalogError> {
        if let Some(catalog) = self.database_catalog.tables.get_mut(table_name) {
            catalog.data_file_path = new_path;
            catalog.first_page_id = first_page_id;
            self.save_catalog()?;
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(table_name.to_string()))
        }
    }

    fn save_catalog(&self) -> Result<(), CatalogError> {
        self.database_catalog.save_to_file(&self.catalog_file)
    }
}
