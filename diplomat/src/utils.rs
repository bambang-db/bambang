use shared_types::{DataType, Value};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType as SqlDataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, ObjectName, SelectItem, UnaryOperator as SqlUnaryOperator,
    Value as SqlValue, Values,
};

use crate::{
    common::LogicalPlanError,
    expression::{BinaryOperator, Expression, UnaryOperator},
    logical_plan::{LogicalPlan, ValuesNode},
    types::{AggregateFunction, ColumnDef, LogicalSchema},
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
pub fn sql_binary_op_to_binary_op(
    op: &SqlBinaryOperator,
) -> Result<BinaryOperator, LogicalPlanError> {
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

/// Convert SQL expression to logical expression
pub fn expr_to_logical_expr(expr: &Expr) -> Result<Expression, LogicalPlanError> {
    match expr {
        Expr::Identifier(ident) => Ok(Expression::column(&ident.value)),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expression::qualified_column(
                    &idents[0].value,
                    &idents[1].value,
                ))
            } else {
                Err(LogicalPlanError::UnsupportedOperation(
                    "Complex identifiers not supported".to_string(),
                ))
            }
        }
        Expr::Value(value) => {
            let logical_value = sql_value_to_value(&value.value)?;
            Ok(Expression::literal(logical_value))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_expr = expr_to_logical_expr(left)?;
            let right_expr = expr_to_logical_expr(right)?;
            let logical_op = sql_binary_op_to_binary_op(op)?;
            Ok(Expression::binary_op(left_expr, logical_op, right_expr))
        }
        Expr::UnaryOp { op, expr } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let logical_op = sql_unary_op_to_unary_op(op)?;
            Ok(Expression::unary_op(logical_op, logical_expr))
        }
        Expr::Function(function) => function_to_logical_expr(function),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let logical_data_type = sql_data_type_to_data_type(data_type)?;
            Ok(Expression::cast(logical_expr, logical_data_type))
        }
        Expr::IsNull(expr) => {
            let logical_expr = expr_to_logical_expr(expr)?;
            Ok(Expression::is_null(logical_expr))
        }
        Expr::IsNotNull(expr) => {
            let logical_expr = expr_to_logical_expr(expr)?;
            Ok(Expression::is_not_null(logical_expr))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let mut logical_list = Vec::new();
            for item in list {
                logical_list.push(expr_to_logical_expr(item)?);
            }
            Ok(Expression::in_list(logical_expr, logical_list, *negated))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let logical_low = expr_to_logical_expr(low)?;
            let logical_high = expr_to_logical_expr(high)?;
            Ok(Expression::between(
                logical_expr,
                logical_low,
                logical_high,
                *negated,
            ))
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let logical_pattern = expr_to_logical_expr(pattern)?;
            Ok(Expression::like(
                logical_expr,
                logical_pattern,
                *negated,
                false,
            ))
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let logical_expr = expr_to_logical_expr(expr)?;
            let logical_pattern = expr_to_logical_expr(pattern)?;
            Ok(Expression::like(
                logical_expr,
                logical_pattern,
                *negated,
                true,
            ))
        }
        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => {
            let operand_expr = if let Some(operand) = operand {
                Some(Box::new(expr_to_logical_expr(operand)?))
            } else {
                None
            };

            let mut when_clauses = Vec::new();
            for (condition, result) in conditions.iter().zip(else_result.iter()) {
                let when_expr = expr_to_logical_expr(&condition.condition)?;
                let then_expr = expr_to_logical_expr(result)?;
                when_clauses.push((when_expr, then_expr));
            }

            let else_clause = if let Some(else_result) = else_result {
                Some(Box::new(expr_to_logical_expr(else_result)?))
            } else {
                None
            };

            Ok(Expression::Case {
                expr: operand_expr,
                when_clauses,
                else_clause,
            })
        }
        Expr::Wildcard(wildcard) => Ok(Expression::wildcard()),
        Expr::QualifiedWildcard(object_name, attached) => {
            let table_name = object_name_to_string(object_name);
            Ok(Expression::qualified_wildcard(table_name))
        }
        _ => Err(LogicalPlanError::UnsupportedOperation(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

/// Convert SQL function to logical expression
pub fn function_to_logical_expr(function: &Function) -> Result<Expression, LogicalPlanError> {
    let function_name = object_name_to_string(&function.name);

    // Check if it's an aggregate function
    let aggregate_func = match function_name.to_lowercase().as_str() {
        "count" => Some(AggregateFunction::Count),
        "sum" => Some(AggregateFunction::Sum),
        "avg" => Some(AggregateFunction::Avg),
        "min" => Some(AggregateFunction::Min),
        "max" => Some(AggregateFunction::Max),
        _ => None,
    };

    if let Some(agg_func) = aggregate_func {
        // let distinct = function.distinct.is_some();
        let expr = match &function.args {
            FunctionArguments::None => None,
            FunctionArguments::Subquery(_) => {
                return Err(LogicalPlanError::UnsupportedOperation(
                    "Subquery function arguments not supported".to_string(),
                ));
            }
            FunctionArguments::List(args) => {
                if args.args.is_empty() {
                    None
                } else {
                    let arg = match &args.args[0] {
                        FunctionArg::Named { .. } => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "Named function arguments not supported".to_string(),
                            ));
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                            return Ok(Expression::aggregate(agg_func, None, false));
                        }
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "Unsupported function argument".to_string(),
                            ));
                        }
                    };
                    Some(expr_to_logical_expr(arg)?)
                }
            }
        };

        Ok(Expression::aggregate(agg_func, expr, false))
    } else {
        // Regular function
        let mut args = Vec::new();
        match &function.args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(_) => {
                return Err(LogicalPlanError::UnsupportedOperation(
                    "Subquery function arguments not supported".to_string(),
                ));
            }
            FunctionArguments::List(arg_list) => {
                for arg in &arg_list.args {
                    match arg {
                        FunctionArg::Named { .. } => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "Named function arguments not supported".to_string(),
                            ));
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            args.push(expr_to_logical_expr(expr)?);
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                            args.push(Expression::wildcard());
                        }
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "Unsupported function argument".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(Expression::function(function_name, args))
    }
}

/// Convert VALUES clause to a logical plan
pub fn values_to_plan(values: &Values) -> Result<LogicalPlan, LogicalPlanError> {
    let mut value_rows = Vec::new();

    for row in &values.rows {
        let mut value_row = Vec::new();
        for expr in row {
            value_row.push(expr_to_logical_expr(expr)?);
        }
        value_rows.push(value_row);
    }

    // Infer schema from first row
    let schema = if let Some(first_row) = value_rows.first() {
        let mut columns = Vec::new();
        for (i, _) in first_row.iter().enumerate() {
            columns.push(ColumnDef::new(format!("column_{}", i), DataType::String));
        }
        LogicalSchema::new(columns)
    } else {
        LogicalSchema::empty()
    };

    Ok(LogicalPlan::Values(ValuesNode {
        values: value_rows,
        schema,
        statistics: crate::types::PlanStatistics::with_row_count(values.rows.len()),
    }))
}
