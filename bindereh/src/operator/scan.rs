use crate::{manager::Manager, operator::tree::TreeOperations};
use regex::Regex;
use shared_types::{
    OrderBy, Predicate, Row, ScanOptions, ScanResult, Schema, SortDirection, StorageError, Value,
};
use std::{cmp::Ordering, sync::Arc};

pub struct ScanOperation {
    storage_manager: Arc<Manager>,
    max_workers: usize,
    batch_size: usize,
}

impl ScanOperation {
    pub fn new(storage_manager: Arc<Manager>, max_workers: usize, batch_size: usize) -> Self {
        Self {
            storage_manager,
            max_workers,
            batch_size,
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
                .expect("Cannot get leftmost_leaf_id");

        if options.parallel && self.max_workers > 1 {
            todo!("Implement parallel scan")
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

        while let Some(leaf_id) = current_leaf_id {
            let leaf_page = self.storage_manager.read_page(leaf_id).await?;
            pages_read += 1;

            for row in &leaf_page.values {
                total_scanned += 1;

                if let Some(ref predicate) = options.predicate {
                    if let Some(ref schema) = options.schema {
                        if !self.evaluate_predicate(predicate, row, schema) {
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

            current_leaf_id = leaf_page.next_leaf_page_id;
        }

        if let Some(ref order_by) = options.order_by {
            if let Some(ref schema) = result_schema {
                self.sort_rows(&mut result_rows, order_by, schema);
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

    fn evaluate_predicate(&self, predicate: &Predicate, row: &Row, schema: &Schema) -> bool {
        match predicate {
            Predicate::ColumnEquals { column, value } => {
                self.match_value(column, row, schema, value, Ordering::Equal)
            }
            Predicate::ColumnNotEquals { column, value } => {
                !self.match_value(column, row, schema, value, Ordering::Equal)
            }
            Predicate::ColumnGreaterThan { column, value } => {
                self.match_value(column, row, schema, value, Ordering::Greater)
            }
            Predicate::ColumnLessThan { column, value } => {
                self.match_value(column, row, schema, value, Ordering::Less)
            }
            Predicate::ColumnGreaterThanOrEqual { column, value } => self.match_any(
                column,
                row,
                schema,
                value,
                &[Ordering::Greater, Ordering::Equal],
            ),
            Predicate::ColumnLessThanOrEqual { column, value } => self.match_any(
                column,
                row,
                schema,
                value,
                &[Ordering::Less, Ordering::Equal],
            ),
            Predicate::ColumnIn { column, values } => self.in_list(column, row, schema, values),
            Predicate::ColumnNotIn { column, values } => !self.in_list(column, row, schema, values),
            Predicate::ColumnIsNull { column } => self.match_null(column, row, schema, true),
            Predicate::ColumnIsNotNull { column } => self.match_null(column, row, schema, false),
            Predicate::ColumnLike { column, pattern } => {
                self.match_like(column, row, schema, pattern)
            }
            Predicate::ColumnBetween { column, start, end } => {
                if let Some(col_idx) = schema.get_column_index(column) {
                    if let Some(val) = row.data.get(col_idx) {
                        let start_cmp = self.compare_values(val, start);
                        let end_cmp = self.compare_values(val, end);
                        return (start_cmp == Ordering::Greater || start_cmp == Ordering::Equal)
                            && (end_cmp == Ordering::Less || end_cmp == Ordering::Equal);
                    }
                }
                false
            }
            Predicate::And(left, right) => {
                self.evaluate_predicate(left, row, schema)
                    && self.evaluate_predicate(right, row, schema)
            }
            Predicate::Or(left, right) => {
                self.evaluate_predicate(left, row, schema)
                    || self.evaluate_predicate(right, row, schema)
            }
            Predicate::Not(inner) => !self.evaluate_predicate(inner, row, schema),
        }
    }

    fn match_value(
        &self,
        column: &str,
        row: &Row,
        schema: &Schema,
        value: &Value,
        ord: Ordering,
    ) -> bool {
        if let Some(col_idx) = schema.get_column_index(column) {
            if let Some(row_value) = row.data.get(col_idx) {
                return self.compare_values(row_value, value) == ord;
            }
        }
        false
    }

    fn match_any(
        &self,
        column: &str,
        row: &Row,
        schema: &Schema,
        value: &Value,
        matches: &[Ordering],
    ) -> bool {
        if let Some(col_idx) = schema.get_column_index(column) {
            if let Some(row_value) = row.data.get(col_idx) {
                return matches.contains(&self.compare_values(row_value, value));
            }
        }
        false
    }

    fn in_list(&self, column: &str, row: &Row, schema: &Schema, values: &[Value]) -> bool {
        if let Some(col_idx) = schema.get_column_index(column) {
            if let Some(row_value) = row.data.get(col_idx) {
                return values
                    .iter()
                    .any(|v| self.compare_values(row_value, v) == Ordering::Equal);
            }
        }
        false
    }

    fn match_null(&self, column: &str, row: &Row, schema: &Schema, is_null: bool) -> bool {
        if let Some(col_idx) = schema.get_column_index(column) {
            if let Some(val) = row.data.get(col_idx) {
                return matches!(val, Value::Null) == is_null;
            }
        }
        false
    }

    fn match_like(&self, column: &str, row: &Row, schema: &Schema, pattern: &str) -> bool {
        if let Some(col_idx) = schema.get_column_index(column) {
            if let Some(Value::String(s)) = row.data.get(col_idx) {
                let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
                if let Ok(regex) = Regex::new(&format!("^{}$", regex_pattern)) {
                    return regex.is_match(s);
                }
            }
        }
        false
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
        }
    }

    fn sort_rows(&self, rows: &mut Vec<Row>, order_by: &[OrderBy], schema: &Schema) {
        rows.sort_by(|a, b| {
            for order in order_by {
                if let Some(col_idx) = schema.get_column_index(&order.column) {
                    if let (Some(val_a), Some(val_b)) = (a.data.get(col_idx), b.data.get(col_idx)) {
                        let cmp = self.compare_values(val_a, val_b);
                        let result = match order.direction {
                            SortDirection::Ascending => cmp,
                            SortDirection::Descending => cmp.reverse(),
                        };
                        if result != Ordering::Equal {
                            return result;
                        }
                    }
                }
            }
            Ordering::Equal
        });
    }
}
