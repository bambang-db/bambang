use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum LogicalPlanError {
    #[error("SQL parsing error: {0}")]
    SqlParseError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Plan validation error: {0}")]
    ValidationError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<sqlparser::parser::ParserError> for LogicalPlanError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        LogicalPlanError::SqlParseError(err.to_string())
    }
}
