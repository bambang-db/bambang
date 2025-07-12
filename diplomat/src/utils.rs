use shared_types::{DataType, Value};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType as SqlDataType, Expr, ObjectName, SelectItem,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::{
    common::LogicalPlanError,
    expression::{BinaryOperator, UnaryOperator},
};

/// Convert object name to string
pub fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

/// Get column name from expression for projection
pub fn expr_to_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        Expr::Function(func) => object_name_to_string(&func.name),
        _ => "expr".to_string(),
    }
}

/// Check if expression is an aggregate
pub fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = object_name_to_string(&func.name).to_lowercase();
            matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
        }
        _ => false,
    }
}

/// Check if projection has aggregates
pub fn has_aggregates(projection: &[SelectItem]) -> bool {
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if is_aggregate_expr(expr) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Convert SQL value to logical value
pub fn sql_value_to_value(value: &SqlValue) -> Result<Value, LogicalPlanError> {
    match value {
        SqlValue::Number(n, _) => {
            if let Ok(int_val) = n.parse::<i64>() {
                Ok(Value::Integer(int_val))
            } else if let Ok(float_val) = n.parse::<f64>() {
                Ok(Value::Float(float_val))
            } else {
                Err(LogicalPlanError::SqlParseError(format!(
                    "Invalid number: {}",
                    n
                )))
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::String(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(LogicalPlanError::UnsupportedOperation(format!(
            "Unsupported value: {:?}",
            value
        ))),
    }
}

/// Convert SQL data type to logical data type
pub fn sql_data_type_to_data_type(data_type: &SqlDataType) -> Result<DataType, LogicalPlanError> {
    match data_type {
        SqlDataType::Char(_) | SqlDataType::Varchar(_) | SqlDataType::Text => Ok(DataType::String),
        SqlDataType::SmallInt(_) => Ok(DataType::SmallInt),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Integer),
        SqlDataType::BigInt(_) => Ok(DataType::BigInt),
        SqlDataType::Float(_) | SqlDataType::Real => Ok(DataType::Float),
        SqlDataType::Double(_) | SqlDataType::DoublePrecision => Ok(DataType::Float),
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::Date => Ok(DataType::Date),
        SqlDataType::Time(_, _) => Ok(DataType::Time),
        SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        SqlDataType::Decimal(_) | SqlDataType::Numeric(_) => Ok(DataType::Decimal),
        SqlDataType::Bytea => Ok(DataType::Binary),
        _ => Err(LogicalPlanError::UnsupportedOperation(format!(
            "Unsupported data type: {:?}",
            data_type
        ))),
    }
}

/// Convert SQL unary operator to logical unary operator
pub fn sql_unary_op_to_unary_op(op: &SqlUnaryOperator) -> Result<UnaryOperator, LogicalPlanError> {
    match op {
        SqlUnaryOperator::Plus => Ok(UnaryOperator::Plus),
        SqlUnaryOperator::Minus => Ok(UnaryOperator::Minus),
        SqlUnaryOperator::Not => Ok(UnaryOperator::Not),
        _ => Err(LogicalPlanError::UnsupportedOperation(format!(
            "Unsupported unary operator: {:?}",
            op
        ))),
    }
}

/// Convert SQL binary operator to logical binary operator
pub fn sql_binary_op_to_binary_op(op: &SqlBinaryOperator) -> Result<BinaryOperator, LogicalPlanError> {
    match op {
        SqlBinaryOperator::Plus => Ok(BinaryOperator::Plus),
        SqlBinaryOperator::Minus => Ok(BinaryOperator::Minus),
        SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
        SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
        SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
        SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
        SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
        SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        SqlBinaryOperator::And => Ok(BinaryOperator::And),
        SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
        SqlBinaryOperator::StringConcat => Ok(BinaryOperator::StringConcat),
        SqlBinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
        SqlBinaryOperator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
        SqlBinaryOperator::BitwiseXor => Ok(BinaryOperator::BitwiseXor),
        _ => Err(LogicalPlanError::UnsupportedOperation(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}
