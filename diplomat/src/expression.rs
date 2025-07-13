use crate::types::{AggregateFunction, ColumnRef};
use serde::{Deserialize, Serialize};
use shared_types::{DataType, Value};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Value),
    Column(ColumnRef),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    Function { name: String, args: Vec<Expression> },
    Aggregate {
        func: AggregateFunction,
        expr: Option<Box<Expression>>,
        distinct: bool,
    },
    Case {
        expr: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        negated: bool,
        case_insensitive: bool,
    },
    Wildcard { table: Option<String> },
    Alias { expr: Box<Expression>, name: String },
    Subquery {
        subquery: Box<crate::logical_plan::LogicalPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    StringConcat,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    BitwiseNot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpressionType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl ExpressionType {
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
    pub fn not_null(data_type: DataType) -> Self {
        Self::new(data_type, false)
    }
    pub fn nullable(data_type: DataType) -> Self {
        Self::new(data_type, true)
    }
}

impl Expression {
    pub fn literal(value: Value) -> Self {
        Expression::Literal(value)
    }
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(ColumnRef::new(name))
    }
    pub fn qualified_column(table: impl Into<String>, name: impl Into<String>) -> Self {
        Expression::Column(ColumnRef::with_table(table, name))
    }
    pub fn binary_op(left: Expression, op: BinaryOperator, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
    pub fn unary_op(op: UnaryOperator, expr: Expression) -> Self {
        Expression::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }
    pub fn alias(expr: Expression, name: impl Into<String>) -> Self {
        Expression::Alias {
            expr: Box::new(expr),
            name: name.into(),
        }
    }
    pub fn function(name: impl Into<String>, args: Vec<Expression>) -> Self {
        Expression::Function {
            name: name.into(),
            args,
        }
    }
    pub fn aggregate(func: AggregateFunction, expr: Option<Expression>, distinct: bool) -> Self {
        Expression::Aggregate {
            func,
            expr: expr.map(Box::new),
            distinct,
        }
    }
    pub fn cast(expr: Expression, data_type: DataType) -> Self {
        Expression::Cast {
            expr: Box::new(expr),
            data_type,
        }
    }
    pub fn is_null(expr: Expression) -> Self {
        Expression::IsNull(Box::new(expr))
    }
    pub fn is_not_null(expr: Expression) -> Self {
        Expression::IsNotNull(Box::new(expr))
    }
    pub fn in_list(expr: Expression, list: Vec<Expression>, negated: bool) -> Self {
        Expression::In {
            expr: Box::new(expr),
            list,
            negated,
        }
    }
    pub fn between(expr: Expression, low: Expression, high: Expression, negated: bool) -> Self {
        Expression::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        }
    }
    pub fn like(
        expr: Expression,
        pattern: Expression,
        negated: bool,
        case_insensitive: bool,
    ) -> Self {
        Expression::Like {
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            negated,
            case_insensitive,
        }
    }
    pub fn wildcard() -> Self {
        Expression::Wildcard { table: None }
    }
    pub fn qualified_wildcard(table: impl Into<String>) -> Self {
        Expression::Wildcard {
            table: Some(table.into()),
        }
    }
    pub fn column_refs(&self) -> Vec<&ColumnRef> {
        let mut refs = Vec::new();
        self.collect_column_refs(&mut refs);
        refs
    }
    fn collect_column_refs<'a>(&'a self, refs: &mut Vec<&'a ColumnRef>) {
        match self {
            Expression::Column(col_ref) => refs.push(col_ref),
            Expression::BinaryOp { left, right, .. } => {
                left.collect_column_refs(refs);
                right.collect_column_refs(refs);
            }
            Expression::UnaryOp { expr, .. } => expr.collect_column_refs(refs),
            Expression::Function { args, .. } => {
                for arg in args {
                    arg.collect_column_refs(refs);
                }
            }
            Expression::Aggregate { expr, .. } => {
                if let Some(expr) = expr {
                    expr.collect_column_refs(refs);
                }
            }
            Expression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                if let Some(expr) = expr {
                    expr.collect_column_refs(refs);
                }
                for (when_expr, then_expr) in when_clauses {
                    when_expr.collect_column_refs(refs);
                    then_expr.collect_column_refs(refs);
                }
                if let Some(else_expr) = else_clause {
                    else_expr.collect_column_refs(refs);
                }
            }
            Expression::Cast { expr, .. } => expr.collect_column_refs(refs),
            Expression::IsNull(expr) | Expression::IsNotNull(expr) => {
                expr.collect_column_refs(refs)
            }
            Expression::In { expr, list, .. } => {
                expr.collect_column_refs(refs);
                for item in list {
                    item.collect_column_refs(refs);
                }
            }
            Expression::Between {
                expr, low, high, ..
            } => {
                expr.collect_column_refs(refs);
                low.collect_column_refs(refs);
                high.collect_column_refs(refs);
            }
            Expression::Like { expr, pattern, .. } => {
                expr.collect_column_refs(refs);
                pattern.collect_column_refs(refs);
            }
            Expression::Alias { expr, .. } => expr.collect_column_refs(refs),
            Expression::Subquery { .. } => {
            }
            Expression::Literal(_) | Expression::Wildcard { .. } => {
            }
        }
    }
    pub fn is_deterministic(&self) -> bool {
        match self {
            Expression::Literal(_) | Expression::Column(_) | Expression::Wildcard { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                left.is_deterministic() && right.is_deterministic()
            }
            Expression::UnaryOp { expr, .. } => expr.is_deterministic(),
            Expression::Function { name, args } => {
                let non_deterministic_functions =
                    ["random", "rand", "now", "current_timestamp", "uuid"];
                let is_non_deterministic = non_deterministic_functions
                    .iter()
                    .any(|&f| name.to_lowercase() == f);
                !is_non_deterministic && args.iter().all(|arg| arg.is_deterministic())
            }
            Expression::Aggregate { expr, .. } => {
                expr.as_ref().map_or(true, |e| e.is_deterministic())
            }
            Expression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                let expr_det = expr.as_ref().map_or(true, |e| e.is_deterministic());
                let when_det = when_clauses
                    .iter()
                    .all(|(w, t)| w.is_deterministic() && t.is_deterministic());
                let else_det = else_clause.as_ref().map_or(true, |e| e.is_deterministic());
                expr_det && when_det && else_det
            }
            Expression::Cast { expr, .. } => expr.is_deterministic(),
            Expression::IsNull(expr) | Expression::IsNotNull(expr) => expr.is_deterministic(),
            Expression::In { expr, list, .. } => {
                expr.is_deterministic() && list.iter().all(|item| item.is_deterministic())
            }
            Expression::Between {
                expr, low, high, ..
            } => expr.is_deterministic() && low.is_deterministic() && high.is_deterministic(),
            Expression::Like { expr, pattern, .. } => {
                expr.is_deterministic() && pattern.is_deterministic()
            }
            Expression::Alias { expr, .. } => expr.is_deterministic(),
            Expression::Subquery { .. } => {
                false
            }
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal(value) => write!(f, "{:?}", value),
            Expression::Column(col_ref) => write!(f, "{}", col_ref.qualified_name()),
            Expression::BinaryOp { left, op, right } => {
                write!(f, "({} {} {})", left, op, right)
            }
            Expression::UnaryOp { op, expr } => write!(f, "{}{}", op, expr),
            Expression::Function { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Expression::Aggregate {
                func,
                expr,
                distinct,
            } => {
                write!(f, "{:?}(", func)?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                if let Some(expr) = expr {
                    write!(f, "{}", expr)?;
                } else {
                    write!(f, "*")?;
                }
                write!(f, ")")
            }
            Expression::Wildcard { table } => {
                if let Some(table) = table {
                    write!(f, "{}.*", table)
                } else {
                    write!(f, "*")
                }
            }
            Expression::Alias { expr, name } => write!(f, "{} AS {}", expr, name),
            _ => write!(f, "<complex_expression>"),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulo => "%",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::StringConcat => "||",
            BinaryOperator::BitwiseAnd => "&",
            BinaryOperator::BitwiseOr => "|",
            BinaryOperator::BitwiseXor => "^",
            BinaryOperator::BitwiseShiftLeft => "<<",
            BinaryOperator::BitwiseShiftRight => ">>",
        };
        write!(f, "{}", op_str)
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT ",
            UnaryOperator::BitwiseNot => "~",
        };
        write!(f, "{}", op_str)
    }
}