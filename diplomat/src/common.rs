#[derive(Debug)]
pub enum SQLParserError {
    InvalidSyntax(String),
    UnexpectedToken(String),
    UnexpectedEof,
    InvalidLiteral(String),
    UnsupportedFeature(String),
    InvalidExpression(String),
    InvalidStatement(String),
    InternalError(String),
}

impl std::fmt::Display for SQLParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SQLParserError::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
            SQLParserError::UnexpectedToken(token) => write!(f, "Unexpected token: {}", token),
            SQLParserError::UnexpectedEof => write!(f, "Unexpected end of input"),
            SQLParserError::InvalidLiteral(lit) => write!(f, "Invalid literal: {}", lit),
            SQLParserError::UnsupportedFeature(feature) => {
                write!(f, "Unsupported feature: {}", feature)
            }
            SQLParserError::InvalidExpression(expr) => write!(f, "Invalid expression: {}", expr),
            SQLParserError::InvalidStatement(stmt) => write!(f, "Invalid statement: {}", stmt),
            SQLParserError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for SQLParserError {}
