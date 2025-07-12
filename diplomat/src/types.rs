//! Type definitions for the logical plan system

use serde::{Deserialize, Serialize};
use shared_types::{DataType, Value};
use std::collections::HashMap;

/// Represents a column reference in a logical plan
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Optional table qualifier
    pub table: Option<String>,
    /// Column name
    pub name: String,
}

impl ColumnRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table: None,
            name: name.into(),
        }
    }

    pub fn with_table(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    pub fn qualified_name(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.name),
            None => self.name.clone(),
        }
    }
}

/// Represents a table reference in a logical plan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Optional alias
    pub alias: Option<String>,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: None,
        }
    }

    pub fn with_alias(name: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    pub fn effective_name(&self) -> &str {
        self.alias.as_ref().unwrap_or(&self.name)
    }
}

/// Represents the schema of a logical plan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalSchema {
    /// Column definitions
    pub columns: Vec<ColumnDef>,
}

impl LogicalSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|col| col.name == name)
    }

    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|col| col.name.as_str()).collect()
    }
}

/// Represents a column definition in a schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Optional table qualifier
    pub table: Option<String>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            table: None,
        }
    }

    pub fn with_table(name: impl Into<String>, data_type: DataType, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            table: Some(table.into()),
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn qualified_name(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.name),
            None => self.name.clone(),
        }
    }
}

/// Represents join types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Represents sort order
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Represents a sort expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortExpr {
    pub expr: Box<crate::expression::Expression>,
    pub order: SortOrder,
    pub nulls_first: bool,
}

impl SortExpr {
    pub fn new(expr: crate::expression::Expression, order: SortOrder) -> Self {
        Self {
            expr: Box::new(expr),
            order,
            nulls_first: false,
        }
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }
}

/// Represents aggregate functions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Statistics about a logical plan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanStatistics {
    /// Estimated number of rows
    pub row_count: Option<usize>,
    /// Estimated size in bytes
    pub size_bytes: Option<usize>,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl PlanStatistics {
    pub fn unknown() -> Self {
        Self {
            row_count: None,
            size_bytes: None,
            column_stats: HashMap::new(),
        }
    }

    pub fn with_row_count(row_count: usize) -> Self {
        Self {
            row_count: Some(row_count),
            size_bytes: None,
            column_stats: HashMap::new(),
        }
    }
}

/// Statistics about a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Number of distinct values
    pub distinct_count: Option<usize>,
    /// Number of null values
    pub null_count: Option<usize>,
    /// Minimum value
    pub min_value: Option<Value>,
    /// Maximum value
    pub max_value: Option<Value>,
}

impl ColumnStatistics {
    pub fn unknown() -> Self {
        Self {
            distinct_count: None,
            null_count: None,
            min_value: None,
            max_value: None,
        }
    }
}