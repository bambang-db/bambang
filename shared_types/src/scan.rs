//! Scan operation types and predicates

use crate::{value::Value, schema::Schema, row::Row};

// Enhanced filtering with column-level predicates
#[derive(Debug, Clone)]
pub enum Predicate {
    // Column-based predicates
    ColumnEquals {
        column: String,
        value: Value,
    },
    ColumnNotEquals {
        column: String,
        value: Value,
    },
    ColumnGreaterThan {
        column: String,
        value: Value,
    },
    ColumnLessThan {
        column: String,
        value: Value,
    },
    ColumnGreaterThanOrEqual {
        column: String,
        value: Value,
    },
    ColumnLessThanOrEqual {
        column: String,
        value: Value,
    },
    ColumnIn {
        column: String,
        values: Vec<Value>,
    },
    ColumnNotIn {
        column: String,
        values: Vec<Value>,
    },
    ColumnIsNull {
        column: String,
    },
    ColumnIsNotNull {
        column: String,
    },
    ColumnLike {
        column: String,
        pattern: String,
    },
    ColumnBetween {
        column: String,
        start: Value,
        end: Value,
    },

    // Logical operators
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create an AND predicate
    pub fn and(left: Predicate, right: Predicate) -> Self {
        Predicate::And(Box::new(left), Box::new(right))
    }

    /// Create an OR predicate
    pub fn or(left: Predicate, right: Predicate) -> Self {
        Predicate::Or(Box::new(left), Box::new(right))
    }

    /// Create a NOT predicate
    pub fn not(predicate: Predicate) -> Self {
        Predicate::Not(Box::new(predicate))
    }

    /// Create a column equals predicate
    pub fn column_equals(column: String, value: Value) -> Self {
        Predicate::ColumnEquals { column, value }
    }

    /// Create a column greater than predicate
    pub fn column_gt(column: String, value: Value) -> Self {
        Predicate::ColumnGreaterThan { column, value }
    }

    /// Create a column less than predicate
    pub fn column_lt(column: String, value: Value) -> Self {
        Predicate::ColumnLessThan { column, value }
    }

    /// Create a column IN predicate
    pub fn column_in(column: String, values: Vec<Value>) -> Self {
        Predicate::ColumnIn { column, values }
    }

    /// Create a column IS NULL predicate
    pub fn column_is_null(column: String) -> Self {
        Predicate::ColumnIsNull { column }
    }

    /// Create a column LIKE predicate
    pub fn column_like(column: String, pattern: String) -> Self {
        Predicate::ColumnLike { column, pattern }
    }

    /// Create a column BETWEEN predicate
    pub fn column_between(column: String, start: Value, end: Value) -> Self {
        Predicate::ColumnBetween { column, start, end }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
}

impl OrderBy {
    /// Create a new OrderBy clause
    pub fn new(column: String, direction: SortDirection) -> Self {
        Self { column, direction }
    }

    /// Create an ascending order by clause
    pub fn asc(column: String) -> Self {
        Self::new(column, SortDirection::Ascending)
    }

    /// Create a descending order by clause
    pub fn desc(column: String) -> Self {
        Self::new(column, SortDirection::Descending)
    }
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub predicate: Option<Predicate>,
    pub projection: Option<Vec<String>>, // Column names to select
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub parallel: bool,
    pub order_by: Option<Vec<OrderBy>>,
    pub schema: Option<Schema>, // Schema for the table being scanned
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            predicate: None,
            projection: None,
            limit: None,
            offset: None,
            parallel: true,
            order_by: None,
            schema: None,
        }
    }
}

impl ScanOptions {
    /// Create new scan options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the predicate for filtering
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the projection (columns to select)
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projection = Some(columns);
        self
    }

    /// Set the limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set parallel execution
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Set the order by clauses
    pub fn with_order_by(mut self, order_by: Vec<OrderBy>) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Set the schema
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub rows: Vec<Row>,
    pub total_scanned: usize,
    pub pages_read: usize,
    pub filtered_count: usize,
    pub result_schema: Option<Schema>, // Schema for the result set after projection
}

impl ScanResult {
    /// Create a new scan result
    pub fn new(
        rows: Vec<Row>,
        total_scanned: usize,
        pages_read: usize,
        filtered_count: usize,
        result_schema: Option<Schema>,
    ) -> Self {
        Self {
            rows,
            total_scanned,
            pages_read,
            filtered_count,
            result_schema,
        }
    }

    /// Get the number of rows returned
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get statistics about the scan
    pub fn stats(&self) -> ScanStats {
        ScanStats {
            rows_returned: self.rows.len(),
            total_scanned: self.total_scanned,
            pages_read: self.pages_read,
            filtered_count: self.filtered_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanStats {
    pub rows_returned: usize,
    pub total_scanned: usize,
    pub pages_read: usize,
    pub filtered_count: usize,
}

impl ScanStats {
    /// Calculate the selectivity ratio (rows returned / total scanned)
    pub fn selectivity(&self) -> f64 {
        if self.total_scanned == 0 {
            0.0
        } else {
            self.rows_returned as f64 / self.total_scanned as f64
        }
    }

    /// Calculate the filter efficiency (filtered / total scanned)
    pub fn filter_efficiency(&self) -> f64 {
        if self.total_scanned == 0 {
            0.0
        } else {
            self.filtered_count as f64 / self.total_scanned as f64
        }
    }
}