use std::cmp::Ordering;

use shared_types::{Row, Schema, StorageError, Value};

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum { column: String },
    Avg { column: String },
    Min { column: String },
    Max { column: String },
    CountDistinct { column: String },
}

pub struct AggregateProcessor;

impl AggregateProcessor {
    pub fn process_aggregates(
        rows: &[Row],
        aggregates: &[AggregateFunction],
        schema: &Schema,
    ) -> Result<Row, StorageError> {
        let mut result_data = Vec::new();

        for aggregate in aggregates {
            let value = match aggregate {
                AggregateFunction::Count => Value::Integer(rows.len() as i64),
                AggregateFunction::Sum { column } => Self::sum_column(rows, column, schema)?,
                AggregateFunction::Avg { column } => Self::avg_column(rows, column, schema)?,
                AggregateFunction::Min { column } => Self::min_column(rows, column, schema)?,
                AggregateFunction::Max { column } => Self::max_column(rows, column, schema)?,
                AggregateFunction::CountDistinct { column } => {
                    Self::count_distinct_column(rows, column, schema)?
                }
            };
            result_data.push(value);
        }

        Ok(Row {
            id: 0, // Aggregate results don't have meaningful IDs
            data: result_data,
        })
    }

    fn sum_column(rows: &[Row], column: &str, schema: &Schema) -> Result<Value, StorageError> {
        if let Some(col_idx) = schema.get_column_index(column) {
            let mut sum = 0i64;
            let mut float_sum = 0.0f64;
            let mut is_float = false;

            for row in rows {
                if let Some(value) = row.data.get(col_idx) {
                    match value {
                        Value::Integer(i) => sum += i,
                        Value::Float(f) => {
                            if !is_float {
                                float_sum = sum as f64;
                                is_float = true;
                            }
                            float_sum += f;
                        }
                        Value::SmallInt(i) => sum += *i as i64,
                        Value::BigInt(i) => sum += *i as i64,
                        Value::TinyInt(i) => sum += *i as i64,
                        _ => {} // Skip non-numeric values
                    }
                }
            }

            if is_float {
                Ok(Value::Float(float_sum))
            } else {
                Ok(Value::Integer(sum))
            }
        } else {
            Err(StorageError::InvalidOperation(format!(
                "Column '{}' not found",
                column
            )))
        }
    }

    fn avg_column(rows: &[Row], column: &str, schema: &Schema) -> Result<Value, StorageError> {
        if let Some(col_idx) = schema.get_column_index(column) {
            let mut sum = 0.0f64;
            let mut count = 0;

            for row in rows {
                if let Some(value) = row.data.get(col_idx) {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        Value::SmallInt(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        Value::BigInt(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        Value::TinyInt(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        _ => {} // Skip non-numeric values
                    }
                }
            }

            if count > 0 {
                Ok(Value::Float(sum / count as f64))
            } else {
                Ok(Value::Null)
            }
        } else {
            Err(StorageError::InvalidOperation(format!(
                "Column '{}' not found",
                column
            )))
        }
    }

    fn min_column(rows: &[Row], column: &str, schema: &Schema) -> Result<Value, StorageError> {
        if let Some(col_idx) = schema.get_column_index(column) {
            let mut min_value: Option<Value> = None;

            for row in rows {
                if let Some(value) = row.data.get(col_idx) {
                    if !matches!(value, Value::Null) {
                        match &min_value {
                            None => min_value = Some(value.clone()),
                            Some(current_min) => {
                                if Self::compare_values_for_aggregate(value, current_min)
                                    == Ordering::Less
                                {
                                    min_value = Some(value.clone());
                                }
                            }
                        }
                    }
                }
            }

            Ok(min_value.unwrap_or(Value::Null))
        } else {
            Err(StorageError::InvalidOperation(format!(
                "Column '{}' not found",
                column
            )))
        }
    }

    fn max_column(rows: &[Row], column: &str, schema: &Schema) -> Result<Value, StorageError> {
        if let Some(col_idx) = schema.get_column_index(column) {
            let mut max_value: Option<Value> = None;

            for row in rows {
                if let Some(value) = row.data.get(col_idx) {
                    if !matches!(value, Value::Null) {
                        match &max_value {
                            None => max_value = Some(value.clone()),
                            Some(current_max) => {
                                if Self::compare_values_for_aggregate(value, current_max)
                                    == Ordering::Greater
                                {
                                    max_value = Some(value.clone());
                                }
                            }
                        }
                    }
                }
            }

            Ok(max_value.unwrap_or(Value::Null))
        } else {
            Err(StorageError::InvalidOperation(format!(
                "Column '{}' not found",
                column
            )))
        }
    }

    fn count_distinct_column(
        rows: &[Row],
        column: &str,
        schema: &Schema,
    ) -> Result<Value, StorageError> {
        if let Some(col_idx) = schema.get_column_index(column) {
            let mut distinct_values = std::collections::HashSet::new();

            for row in rows {
                if let Some(value) = row.data.get(col_idx) {
                    distinct_values.insert(format!("{:?}", value));
                }
            }

            Ok(Value::Integer(distinct_values.len() as i64))
        } else {
            Err(StorageError::InvalidOperation(format!(
                "Column '{}' not found",
                column
            )))
        }
    }

    fn compare_values_for_aggregate(a: &Value, b: &Value) -> Ordering {
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
}
