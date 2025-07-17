use std::{cmp::Ordering, collections::HashMap};

use regex::Regex;
use shared_types::{OrderBy, Predicate, Row, Schema, SortDirection, Value};
pub fn evaluate_predicate_fast(predicate: &Predicate, row: &Row, column_indices: &HashMap<String, usize>) -> bool {
    match predicate {
        Predicate::ColumnEquals { column, value } => {
            if let Some(&idx) = column_indices.get(column) {
                if let Some(row_value) = row.data.get(idx) {
                    return row_value == value;
                }
            }
            false
        }
        Predicate::ColumnLessThan { column, value } => {
            if let Some(&idx) = column_indices.get(column) {
                if let Some(row_value) = row.data.get(idx) {
                    return match (row_value, value) {
                        (Value::Integer(a), Value::Integer(b)) => a < b,
                        (Value::Float(a), Value::Float(b)) => a < b,
                        _ => false,
                    };
                }
            }
            false
        }
        Predicate::ColumnGreaterThanOrEqual { column, value } => {
            if let Some(&idx) = column_indices.get(column) {
                if let Some(row_value) = row.data.get(idx) {
                    return match (row_value, value) {
                        (Value::Integer(a), Value::Integer(b)) => a >= b,
                        (Value::Float(a), Value::Float(b)) => a >= b,
                        _ => false,
                    };
                }
            }
            false
        }
        Predicate::ColumnLessThanOrEqual { column, value } => {
            if let Some(&idx) = column_indices.get(column) {
                if let Some(row_value) = row.data.get(idx) {
                    return match (row_value, value) {
                        (Value::Integer(a), Value::Integer(b)) => a <= b,
                        (Value::Float(a), Value::Float(b)) => a <= b,
                        _ => false,
                    };
                }
            }
            false
        }
        Predicate::And(left, right) => {
            evaluate_predicate_fast(left, row, column_indices) && evaluate_predicate_fast(right, row, column_indices)
        }
        Predicate::Or(left, right) => {
            evaluate_predicate_fast(left, row, column_indices) || evaluate_predicate_fast(right, row, column_indices)
        }
        _ => false,
    }
}


pub fn compare_values_static(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::SmallInt(a), Value::SmallInt(b)) => a.cmp(b),
        (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
        (Value::TinyInt(a), Value::TinyInt(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
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

pub fn match_value_optimized(
    column: &str,
    row: &Row,
    value: &Value,
    ord: Ordering,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return compare_values(row_value, value) == ord;
        }
    }
    false
}

pub fn match_any_optimized(
    column: &str,
    row: &Row,
    value: &Value,
    matches: &[Ordering],
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return matches.contains(&compare_values(row_value, value));
        }
    }
    false
}

pub fn in_list_optimized(
    column: &str,
    row: &Row,
    values: &[Value],
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return values
                .iter()
                .any(|v| compare_values(row_value, v) == Ordering::Equal);
        }
    }
    false
}

pub fn match_null_optimized(
    column: &str,
    row: &Row,
    is_null: bool,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(val) = row.data.get(*col_idx) {
            return matches!(val, Value::Null) == is_null;
        }
    }
    false
}

pub fn match_like_optimized(
    column: &str,
    row: &Row,
    pattern: &str,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(Value::String(s)) = row.data.get(*col_idx) {
            let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
            if let Ok(regex) = Regex::new(&format!("^{}$", regex_pattern)) {
                return regex.is_match(s);
            }
        }
    }
    false
}

pub fn match_value_optimized_static(
    column: &str,
    row: &Row,
    value: &Value,
    ord: std::cmp::Ordering,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return compare_values_static(row_value, value) == ord;
        }
    }
    false
}

pub fn match_any_optimized_static(
    column: &str,
    row: &Row,
    value: &Value,
    matches: &[std::cmp::Ordering],
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return matches.contains(&compare_values_static(row_value, value));
        }
    }
    false
}

pub fn in_list_optimized_static(
    column: &str,
    row: &Row,
    values: &[Value],
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(row_value) = row.data.get(*col_idx) {
            return values
                .iter()
                .any(|v| compare_values_static(row_value, v) == std::cmp::Ordering::Equal);
        }
    }
    false
}

pub fn match_null_optimized_static(
    column: &str,
    row: &Row,
    is_null: bool,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(val) = row.data.get(*col_idx) {
            return matches!(val, Value::Null) == is_null;
        }
    }
    false
}

pub fn match_like_optimized_static(
    column: &str,
    row: &Row,
    pattern: &str,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    if let Some(col_idx) = cached_indices
        .as_ref()
        .and_then(|indices| indices.get(column))
    {
        if let Some(Value::String(s)) = row.data.get(*col_idx) {
            let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
            if let Ok(regex) = Regex::new(&format!("^{}$", regex_pattern)) {
                return regex.is_match(s);
            }
        }
    }
    false
}

pub fn sort_rows(rows: &mut Vec<Row>, order_by: &[OrderBy], schema: &Schema) {
    rows.sort_by(|a, b| {
        for order in order_by {
            if let Some(col_idx) = schema.get_column_index(&order.column) {
                if let (Some(val_a), Some(val_b)) = (a.data.get(col_idx), b.data.get(col_idx)) {
                    let cmp = compare_values(val_a, val_b);
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

pub fn evaluate_predicate_optimized_static(
    predicate: &Predicate,
    row: &Row,
    schema: &Schema,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    match predicate {
        Predicate::ColumnEquals { column, value } => match_value_optimized_static(
            column,
            row,
            value,
            std::cmp::Ordering::Equal,
            cached_indices,
        ),
        Predicate::ColumnNotEquals { column, value } => !match_value_optimized_static(
            column,
            row,
            value,
            std::cmp::Ordering::Equal,
            cached_indices,
        ),
        Predicate::ColumnGreaterThan { column, value } => match_value_optimized_static(
            column,
            row,
            value,
            std::cmp::Ordering::Greater,
            cached_indices,
        ),
        Predicate::ColumnLessThan { column, value } => match_value_optimized_static(
            column,
            row,
            value,
            std::cmp::Ordering::Less,
            cached_indices,
        ),
        Predicate::ColumnGreaterThanOrEqual { column, value } => match_any_optimized_static(
            column,
            row,
            value,
            &[std::cmp::Ordering::Greater, std::cmp::Ordering::Equal],
            cached_indices,
        ),
        Predicate::ColumnLessThanOrEqual { column, value } => match_any_optimized_static(
            column,
            row,
            value,
            &[std::cmp::Ordering::Less, std::cmp::Ordering::Equal],
            cached_indices,
        ),
        Predicate::ColumnIn { column, values } => {
            in_list_optimized_static(column, row, values, cached_indices)
        }
        Predicate::ColumnNotIn { column, values } => {
            !in_list_optimized_static(column, row, values, cached_indices)
        }
        Predicate::ColumnIsNull { column } => {
            match_null_optimized_static(column, row, true, cached_indices)
        }
        Predicate::ColumnIsNotNull { column } => {
            match_null_optimized_static(column, row, false, cached_indices)
        }
        Predicate::ColumnLike { column, pattern } => {
            match_like_optimized_static(column, row, pattern, cached_indices)
        }
        Predicate::ColumnBetween { column, start, end } => {
            if let Some(col_idx) = cached_indices
                .as_ref()
                .and_then(|indices| indices.get(column))
            {
                if let Some(val) = row.data.get(*col_idx) {
                    let start_cmp = compare_values_static(val, start);
                    let end_cmp = compare_values_static(val, end);
                    return (start_cmp == std::cmp::Ordering::Greater
                        || start_cmp == std::cmp::Ordering::Equal)
                        && (end_cmp == std::cmp::Ordering::Less
                            || end_cmp == std::cmp::Ordering::Equal);
                }
            }
            false
        }
        Predicate::And(left, right) => {
            evaluate_predicate_optimized_static(left, row, schema, cached_indices)
                && evaluate_predicate_optimized_static(right, row, schema, cached_indices)
        }
        Predicate::Or(left, right) => {
            evaluate_predicate_optimized_static(left, row, schema, cached_indices)
                || evaluate_predicate_optimized_static(right, row, schema, cached_indices)
        }
        Predicate::Not(inner) => {
            !evaluate_predicate_optimized_static(inner, row, schema, cached_indices)
        }
    }
}

pub fn extract_predicate_column_indices(
    predicate: &Predicate,
    schema: &Schema,
) -> HashMap<String, usize> {
    let mut indices = HashMap::new();
    collect_predicate_columns(predicate, schema, &mut indices);
    indices
}

pub fn collect_predicate_columns(
    predicate: &Predicate,
    schema: &Schema,
    indices: &mut HashMap<String, usize>,
) {
    match predicate {
        Predicate::ColumnEquals { column, .. }
        | Predicate::ColumnNotEquals { column, .. }
        | Predicate::ColumnGreaterThan { column, .. }
        | Predicate::ColumnLessThan { column, .. }
        | Predicate::ColumnGreaterThanOrEqual { column, .. }
        | Predicate::ColumnLessThanOrEqual { column, .. }
        | Predicate::ColumnIn { column, .. }
        | Predicate::ColumnNotIn { column, .. }
        | Predicate::ColumnIsNull { column }
        | Predicate::ColumnIsNotNull { column }
        | Predicate::ColumnLike { column, .. }
        | Predicate::ColumnBetween { column, .. } => {
            if let Some(idx) = schema.get_column_index(column) {
                indices.insert(column.clone(), idx);
            }
        }
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            collect_predicate_columns(left, schema, indices);
            collect_predicate_columns(right, schema, indices);
        }
        Predicate::Not(inner) => {
            collect_predicate_columns(inner, schema, indices);
        }
    }
}

pub fn evaluate_predicate_optimized(
    predicate: &Predicate,
    row: &Row,
    schema: &Schema,
    cached_indices: &Option<HashMap<String, usize>>,
) -> bool {
    match predicate {
        Predicate::ColumnEquals { column, value } => {
            match_value_optimized(column, row, value, Ordering::Equal, cached_indices)
        }
        Predicate::ColumnNotEquals { column, value } => {
            !match_value_optimized(column, row, value, Ordering::Equal, cached_indices)
        }
        Predicate::ColumnGreaterThan { column, value } => {
            match_value_optimized(column, row, value, Ordering::Greater, cached_indices)
        }
        Predicate::ColumnLessThan { column, value } => {
            match_value_optimized(column, row, value, Ordering::Less, cached_indices)
        }
        Predicate::ColumnGreaterThanOrEqual { column, value } => match_any_optimized(
            column,
            row,
            value,
            &[Ordering::Greater, Ordering::Equal],
            cached_indices,
        ),
        Predicate::ColumnLessThanOrEqual { column, value } => match_any_optimized(
            column,
            row,
            value,
            &[Ordering::Less, Ordering::Equal],
            cached_indices,
        ),
        Predicate::ColumnIn { column, values } => {
            in_list_optimized(column, row, values, cached_indices)
        }
        Predicate::ColumnNotIn { column, values } => {
            !in_list_optimized(column, row, values, cached_indices)
        }
        Predicate::ColumnIsNull { column } => {
            match_null_optimized(column, row, true, cached_indices)
        }
        Predicate::ColumnIsNotNull { column } => {
            match_null_optimized(column, row, false, cached_indices)
        }
        Predicate::ColumnLike { column, pattern } => {
            match_like_optimized(column, row, pattern, cached_indices)
        }
        Predicate::ColumnBetween { column, start, end } => {
            if let Some(col_idx) = cached_indices
                .as_ref()
                .and_then(|indices| indices.get(column))
            {
                if let Some(val) = row.data.get(*col_idx) {
                    let start_cmp = compare_values(val, start);
                    let end_cmp = compare_values(val, end);
                    return (start_cmp == Ordering::Greater || start_cmp == Ordering::Equal)
                        && (end_cmp == Ordering::Less || end_cmp == Ordering::Equal);
                }
            }
            false
        }
        Predicate::And(left, right) => {
            evaluate_predicate_optimized(left, row, schema, cached_indices)
                && evaluate_predicate_optimized(right, row, schema, cached_indices)
        }
        Predicate::Or(left, right) => {
            evaluate_predicate_optimized(left, row, schema, cached_indices)
                || evaluate_predicate_optimized(right, row, schema, cached_indices)
        }
        Predicate::Not(inner) => !evaluate_predicate_optimized(inner, row, schema, cached_indices),
    }
}
