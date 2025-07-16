use crate::manager::Manager;
use shared_types::{Row, Schema, StorageError, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug)]
pub struct JoinResult {
    pub rows: Vec<Row>,
    pub result_schema: Schema,
    pub left_rows_processed: usize,
    pub right_rows_processed: usize,
    pub output_rows: usize,
}

pub struct HashJoinOperation {
    storage_manager: Arc<Manager>,
    join_type: JoinType,
    join_conditions: Vec<JoinCondition>,
}

impl HashJoinOperation {
    pub fn new(
        storage_manager: Arc<Manager>,
        join_type: JoinType,
        join_conditions: Vec<JoinCondition>,
    ) -> Self {
        Self {
            storage_manager,
            join_type,
            join_conditions,
        }
    }

    pub async fn execute(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<JoinResult, StorageError> {
        let result_schema = self.build_result_schema(left_schema, right_schema)?;
        
        match self.join_type {
            JoinType::Inner => self.inner_hash_join(left_rows, right_rows, left_schema, right_schema, result_schema).await,
            JoinType::LeftOuter => self.left_outer_hash_join(left_rows, right_rows, left_schema, right_schema, result_schema).await,
            JoinType::RightOuter => self.right_outer_hash_join(left_rows, right_rows, left_schema, right_schema, result_schema).await,
            JoinType::FullOuter => self.full_outer_hash_join(left_rows, right_rows, left_schema, right_schema, result_schema).await,
        }
    }

    async fn inner_hash_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        left_schema: &Schema,
        right_schema: &Schema,
        result_schema: Schema,
    ) -> Result<JoinResult, StorageError> {
        let hash_table = self.build_hash_table(&right_rows, right_schema)?;
        let mut result_rows = Vec::new();

        for left_row in &left_rows {
            let join_key = self.extract_join_key(left_row, left_schema, true)?;
            
            if let Some(matching_right_rows) = hash_table.get(&join_key) {
                for right_row in matching_right_rows {
                    let joined_row = self.merge_rows(left_row, right_row, left_schema, right_schema)?;
                    result_rows.push(joined_row);
                }
            }
        }

        let output_rows = result_rows.len();
        Ok(JoinResult {
            rows: result_rows,
            result_schema,
            left_rows_processed: left_rows.len(),
            right_rows_processed: right_rows.len(),
            output_rows,
        })
    }

    async fn left_outer_hash_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        left_schema: &Schema,
        right_schema: &Schema,
        result_schema: Schema,
    ) -> Result<JoinResult, StorageError> {
        let hash_table = self.build_hash_table(&right_rows, right_schema)?;
        let mut result_rows = Vec::new();

        for left_row in &left_rows {
            let join_key = self.extract_join_key(left_row, left_schema, true)?;
            
            if let Some(matching_right_rows) = hash_table.get(&join_key) {
                for right_row in matching_right_rows {
                    let joined_row = self.merge_rows(left_row, right_row, left_schema, right_schema)?;
                    result_rows.push(joined_row);
                }
            } else {
                let null_right_row = self.create_null_row(right_schema);
                let joined_row = self.merge_rows(left_row, &null_right_row, left_schema, right_schema)?;
                result_rows.push(joined_row);
            }
        }

        let output_rows = result_rows.len();
        Ok(JoinResult {
            rows: result_rows,
            result_schema,
            left_rows_processed: left_rows.len(),
            right_rows_processed: right_rows.len(),
            output_rows,
        })
    }

    async fn right_outer_hash_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        left_schema: &Schema,
        right_schema: &Schema,
        result_schema: Schema,
    ) -> Result<JoinResult, StorageError> {
        let hash_table = self.build_hash_table(&left_rows, left_schema)?;
        let mut result_rows = Vec::new();
        let mut matched_left_keys = std::collections::HashSet::new();

        for right_row in &right_rows {
            let join_key = self.extract_join_key(right_row, right_schema, false)?;
            
            if let Some(matching_left_rows) = hash_table.get(&join_key) {
                matched_left_keys.insert(join_key.clone());
                for left_row in matching_left_rows {
                    let joined_row = self.merge_rows(left_row, right_row, left_schema, right_schema)?;
                    result_rows.push(joined_row);
                }
            } else {
                let null_left_row = self.create_null_row(left_schema);
                let joined_row = self.merge_rows(&null_left_row, right_row, left_schema, right_schema)?;
                result_rows.push(joined_row);
            }
        }

        let output_rows = result_rows.len();
        Ok(JoinResult {
            rows: result_rows,
            result_schema,
            left_rows_processed: left_rows.len(),
            right_rows_processed: right_rows.len(),
            output_rows,
        })
    }

    async fn full_outer_hash_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        left_schema: &Schema,
        right_schema: &Schema,
        result_schema: Schema,
    ) -> Result<JoinResult, StorageError> {
        let right_hash_table = self.build_hash_table(&right_rows, right_schema)?;
        let mut result_rows = Vec::new();
        let mut matched_right_keys = std::collections::HashSet::new();

        for left_row in &left_rows {
            let join_key = self.extract_join_key(left_row, left_schema, true)?;
            
            if let Some(matching_right_rows) = right_hash_table.get(&join_key) {
                matched_right_keys.insert(join_key);
                for right_row in matching_right_rows {
                    let joined_row = self.merge_rows(left_row, right_row, left_schema, right_schema)?;
                    result_rows.push(joined_row);
                }
            } else {
                let null_right_row = self.create_null_row(right_schema);
                let joined_row = self.merge_rows(left_row, &null_right_row, left_schema, right_schema)?;
                result_rows.push(joined_row);
            }
        }

        for right_row in &right_rows {
            let join_key = self.extract_join_key(right_row, right_schema, false)?;
            
            if !matched_right_keys.contains(&join_key) {
                let null_left_row = self.create_null_row(left_schema);
                let joined_row = self.merge_rows(&null_left_row, right_row, left_schema, right_schema)?;
                result_rows.push(joined_row);
            }
        }

        let output_rows = result_rows.len();
        Ok(JoinResult {
            rows: result_rows,
            result_schema,
            left_rows_processed: left_rows.len(),
            right_rows_processed: right_rows.len(),
            output_rows,
        })
    }

    fn build_hash_table(
        &self,
        rows: &[Row],
        schema: &Schema,
    ) -> Result<HashMap<Vec<Value>, Vec<Row>>, StorageError> {
        let mut hash_table: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();

        for row in rows {
            let join_key = self.extract_join_key(row, schema, false)?;
            hash_table.entry(join_key).or_insert_with(Vec::new).push(row.clone());
        }

        Ok(hash_table)
    }

    fn extract_join_key(
        &self,
        row: &Row,
        schema: &Schema,
        is_left: bool,
    ) -> Result<Vec<Value>, StorageError> {
        let mut key = Vec::new();

        for condition in &self.join_conditions {
            let column_name = if is_left {
                &condition.left_column
            } else {
                &condition.right_column
            };

            let column_index = schema.get_column_index(column_name)
                .ok_or_else(|| StorageError::InvalidOperation(
                    format!("Column '{}' not found in schema", column_name)
                ))?;

            let value = row.get_value(column_index)
                .ok_or_else(|| StorageError::InvalidOperation(
                    format!("Column index {} out of bounds", column_index)
                ))?;

            key.push(value.clone());
        }

        Ok(key)
    }

    fn merge_rows(
        &self,
        left_row: &Row,
        right_row: &Row,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Row, StorageError> {
        let mut merged_data = Vec::new();
        
        merged_data.extend_from_slice(&left_row.data);
        merged_data.extend_from_slice(&right_row.data);

        Ok(Row::new(left_row.id, merged_data))
    }

    fn create_null_row(&self, schema: &Schema) -> Row {
        let null_data = vec![Value::Null; schema.column_count()];
        Row::new(0, null_data)
    }

    fn build_result_schema(
        &self,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Schema, StorageError> {
        let mut result_columns = Vec::new();
        
        for column in &left_schema.columns {
            result_columns.push(column.clone());
        }
        
        for column in &right_schema.columns {
            let mut new_column = column.clone();
            if left_schema.has_column(&column.name) {
                new_column.name = format!("right_{}", column.name);
            }
            result_columns.push(new_column);
        }

        Ok(Schema::new(result_columns))
    }
}