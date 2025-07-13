use crate::{
    common::LogicalPlanError,
    expression::{BinaryOperator, Expression, UnaryOperator},
    logical_plan::{FilterNode, LogicalPlan},
};
use shared_types::Value;

pub struct Optimizer {
    // Should have access to catalog and statistic
}

impl Optimizer {
    pub fn new() -> Self {
        Self {}
    }

    /// Main optimization entry point
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, LogicalPlanError> {
        let mut optimized_plan = plan;

        // Apply optimization rules in order
        optimized_plan = self.apply_constant_folding(optimized_plan)?;
        optimized_plan = self.apply_condition_simplification(optimized_plan)?;
        optimized_plan = self.apply_predicate_pushdown(optimized_plan)?;

        Ok(optimized_plan)
    }

    /// Rule 1: Apply WHERE conditions as close to the data source as possible
    fn apply_predicate_pushdown(&self, plan: LogicalPlan) -> Result<LogicalPlan, LogicalPlanError> {
        match plan {
            LogicalPlan::Filter(filter_node) => {
                let input = *filter_node.input;
                match input {
                    LogicalPlan::TableScan(mut table_scan) => {
                        // Push filter down to table scan
                        table_scan.filters.push(filter_node.predicate);
                        Ok(LogicalPlan::TableScan(table_scan))
                    }
                    LogicalPlan::Projection(proj_node) => {
                        // Try to push filter below projection if possible
                        if self.can_push_through_projection(
                            &filter_node.predicate,
                            &proj_node.expressions,
                        ) {
                            let rewritten_predicate = self.rewrite_predicate_for_projection(
                                &filter_node.predicate,
                                &proj_node.expressions,
                            )?;

                            let new_filter = FilterNode {
                                predicate: rewritten_predicate,
                                input: proj_node.input.clone(),
                                statistics: filter_node.statistics,
                            };

                            let optimized_input =
                                self.apply_predicate_pushdown(LogicalPlan::Filter(new_filter))?;

                            let new_projection =
                                LogicalPlan::Projection(crate::logical_plan::ProjectionNode {
                                    expressions: proj_node.expressions,
                                    input: Box::new(optimized_input),
                                    schema: proj_node.schema,
                                    statistics: proj_node.statistics,
                                });

                            Ok(new_projection)
                        } else {
                            // Can't push through, keep filter above projection
                            let optimized_input =
                                self.apply_predicate_pushdown(*proj_node.input)?;
                            let new_projection =
                                LogicalPlan::Projection(crate::logical_plan::ProjectionNode {
                                    expressions: proj_node.expressions,
                                    input: Box::new(optimized_input),
                                    schema: proj_node.schema,
                                    statistics: proj_node.statistics,
                                });

                            Ok(LogicalPlan::Filter(FilterNode {
                                predicate: filter_node.predicate,
                                input: Box::new(new_projection),
                                statistics: filter_node.statistics,
                            }))
                        }
                    }
                    _ => {
                        // Recursively optimize the input
                        let optimized_input = self.apply_predicate_pushdown(input)?;
                        Ok(LogicalPlan::Filter(FilterNode {
                            predicate: filter_node.predicate,
                            input: Box::new(optimized_input),
                            statistics: filter_node.statistics,
                        }))
                    }
                }
            }
            _ => {
                // Recursively apply to children
                let children = plan.children();
                let mut optimized_children = Vec::new();

                for child in children {
                    optimized_children.push(self.apply_predicate_pushdown(child.clone())?);
                }

                self.with_new_children(plan, optimized_children)
            }
        }
    }

    /// Rule 2: Evaluate constant expressions at optimization time
    fn apply_constant_folding(&self, plan: LogicalPlan) -> Result<LogicalPlan, LogicalPlanError> {
        match plan {
            LogicalPlan::Filter(mut filter_node) => {
                filter_node.predicate = self.fold_constants(filter_node.predicate)?;
                let optimized_input = self.apply_constant_folding(*filter_node.input)?;
                Ok(LogicalPlan::Filter(FilterNode {
                    predicate: filter_node.predicate,
                    input: Box::new(optimized_input),
                    statistics: filter_node.statistics,
                }))
            }
            LogicalPlan::Projection(mut proj_node) => {
                for expr in &mut proj_node.expressions {
                    *expr = self.fold_constants(expr.clone())?;
                }
                let optimized_input = self.apply_constant_folding(*proj_node.input)?;
                Ok(LogicalPlan::Projection(
                    crate::logical_plan::ProjectionNode {
                        expressions: proj_node.expressions,
                        input: Box::new(optimized_input),
                        schema: proj_node.schema,
                        statistics: proj_node.statistics,
                    },
                ))
            }
            _ => {
                // Recursively apply to children
                let children = plan.children();
                let mut optimized_children = Vec::new();

                for child in children {
                    optimized_children.push(self.apply_constant_folding(child.clone())?);
                }

                self.with_new_children(plan, optimized_children)
            }
        }
    }

    /// Rule 3: Remove duplicate or contradictory conditions
    fn apply_condition_simplification(
        &self,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match plan {
            LogicalPlan::Filter(mut filter_node) => {
                filter_node.predicate = self.simplify_conditions(filter_node.predicate)?;
                let optimized_input = self.apply_condition_simplification(*filter_node.input)?;
                Ok(LogicalPlan::Filter(FilterNode {
                    predicate: filter_node.predicate,
                    input: Box::new(optimized_input),
                    statistics: filter_node.statistics,
                }))
            }
            LogicalPlan::TableScan(mut table_scan) => {
                table_scan.filters = self.simplify_filter_list(table_scan.filters)?;
                Ok(LogicalPlan::TableScan(table_scan))
            }
            _ => {
                // Recursively apply to children
                let children = plan.children();
                let mut optimized_children = Vec::new();

                for child in children {
                    optimized_children.push(self.apply_condition_simplification(child.clone())?);
                }

                self.with_new_children(plan, optimized_children)
            }
        }
    }

    /// Helper: Fold constant expressions
    fn fold_constants(&self, expr: Expression) -> Result<Expression, LogicalPlanError> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                let left_folded = self.fold_constants(*left)?;
                let right_folded = self.fold_constants(*right)?;

                // Try to evaluate if both sides are literals
                if let (Expression::Literal(left_val), Expression::Literal(right_val)) =
                    (&left_folded, &right_folded)
                {
                    self.evaluate_binary_op(left_val, &op, right_val)
                        .map(Expression::Literal)
                        .or_else(|_| {
                            Ok(Expression::BinaryOp {
                                left: Box::new(left_folded),
                                op,
                                right: Box::new(right_folded),
                            })
                        })
                } else {
                    Ok(Expression::BinaryOp {
                        left: Box::new(left_folded),
                        op,
                        right: Box::new(right_folded),
                    })
                }
            }
            Expression::UnaryOp { op, expr } => {
                let expr_folded = self.fold_constants(*expr)?;

                if let Expression::Literal(val) = &expr_folded {
                    self.evaluate_unary_op(&op, val)
                        .map(Expression::Literal)
                        .or_else(|_| {
                            Ok(Expression::UnaryOp {
                                op,
                                expr: Box::new(expr_folded),
                            })
                        })
                } else {
                    Ok(Expression::UnaryOp {
                        op,
                        expr: Box::new(expr_folded),
                    })
                }
            }
            _ => Ok(expr), // Return other expressions unchanged
        }
    }

    /// Helper: Simplify conditions (remove duplicates and contradictions)
    fn simplify_conditions(&self, expr: Expression) -> Result<Expression, LogicalPlanError> {
        match expr {
            Expression::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left_simplified = self.simplify_conditions(*left)?;
                let right_simplified = self.simplify_conditions(*right)?;

                // Check for contradictions and duplicates
                if self.are_conditions_contradictory(&left_simplified, &right_simplified) {
                    // Return FALSE condition
                    return Ok(Expression::Literal(Value::Boolean(false)));
                } else if left_simplified == right_simplified {
                    // Remove duplicate
                    return Ok(left_simplified);
                } else if let Expression::BinaryOp {
                    left: nested_left,
                    op: BinaryOperator::Gt,
                    right: nested_right,
                } = &left_simplified
                {
                    // Check for x > a AND x > b pattern
                    if let Expression::BinaryOp {
                        left: nested_left2,
                        op: BinaryOperator::Gt,
                        right: nested_right2,
                    } = &right_simplified
                    {
                        if nested_left == nested_left2 {
                            // x > a AND x > b -> x > max(a, b)
                            if let (Expression::Literal(val1), Expression::Literal(val2)) =
                                (nested_right.as_ref(), nested_right2.as_ref())
                            {
                                if self.compare_values(val1, val2)? > 0 {
                                    return Ok(left_simplified);
                                } else {
                                    return Ok(right_simplified);
                                }
                            }
                        }
                    }
                }

                Ok(Expression::BinaryOp {
                    left: Box::new(left_simplified),
                    op: BinaryOperator::And,
                    right: Box::new(right_simplified),
                })
            }
            _ => Ok(expr),
        }
    }

    /// Helper: Simplify a list of filter conditions
    fn simplify_filter_list(
        &self,
        mut filters: Vec<Expression>,
    ) -> Result<Vec<Expression>, LogicalPlanError> {
        // Remove duplicates
        filters.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        filters.dedup();

        // Apply condition simplification to each filter
        let mut simplified_filters = Vec::new();
        for filter in filters {
            simplified_filters.push(self.simplify_conditions(filter)?);
        }

        Ok(simplified_filters)
    }

    /// Helper: Check if we can push a predicate through a projection
    fn can_push_through_projection(
        &self,
        predicate: &Expression,
        _projections: &[Expression],
    ) -> bool {
        // Simplified check - in practice, you'd need to verify that all columns
        // referenced in the predicate are available in the input of the projection
        matches!(
            predicate,
            Expression::Column(_) | Expression::BinaryOp { .. }
        )
    }

    /// Helper: Rewrite predicate for projection pushdown
    fn rewrite_predicate_for_projection(
        &self,
        predicate: &Expression,
        _projections: &[Expression],
    ) -> Result<Expression, LogicalPlanError> {
        // Simplified implementation - in practice, you'd need to map column references
        // from the projection output to the projection input
        Ok(predicate.clone())
    }

    /// Helper: Evaluate binary operations on literals
    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value, LogicalPlanError> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match op {
                BinaryOperator::Plus => Ok(Value::Integer(l + r)),
                BinaryOperator::Minus => Ok(Value::Integer(l - r)),
                BinaryOperator::Multiply => Ok(Value::Integer(l * r)),
                BinaryOperator::Divide => {
                    if *r == 0 {
                        Err(LogicalPlanError::InternalError(
                            "Division by zero".to_string(),
                        ))
                    } else {
                        Ok(Value::Integer(l / r))
                    }
                }
                BinaryOperator::Gt => Ok(Value::Boolean(l > r)),
                BinaryOperator::Lt => Ok(Value::Boolean(l < r)),
                BinaryOperator::GtEq => Ok(Value::Boolean(l >= r)),
                BinaryOperator::LtEq => Ok(Value::Boolean(l <= r)),
                BinaryOperator::Eq => Ok(Value::Boolean(l == r)),
                BinaryOperator::NotEq => Ok(Value::Boolean(l != r)),
                _ => Err(LogicalPlanError::InternalError(format!(
                    "Unsupported operation: {:?}",
                    op
                ))),
            },
            (Value::Boolean(l), Value::Boolean(r)) => match op {
                BinaryOperator::And => Ok(Value::Boolean(*l && *r)),
                BinaryOperator::Or => Ok(Value::Boolean(*l || *r)),
                BinaryOperator::Eq => Ok(Value::Boolean(l == r)),
                BinaryOperator::NotEq => Ok(Value::Boolean(l != r)),
                _ => Err(LogicalPlanError::InternalError(format!(
                    "Unsupported operation: {:?}",
                    op
                ))),
            },
            _ => Err(LogicalPlanError::InternalError(
                "Type mismatch in binary operation".to_string(),
            )),
        }
    }

    /// Helper: Evaluate unary operations on literals
    fn evaluate_unary_op(
        &self,
        op: &UnaryOperator,
        value: &Value,
    ) -> Result<Value, LogicalPlanError> {
        match (op, value) {
            (UnaryOperator::Minus, Value::Integer(v)) => Ok(Value::Integer(-v)),
            (UnaryOperator::Not, Value::Boolean(v)) => Ok(Value::Boolean(!v)),
            _ => Err(LogicalPlanError::InternalError(format!(
                "Unsupported unary operation: {:?}",
                op
            ))),
        }
    }

    /// Helper: Check if two conditions are contradictory
    fn are_conditions_contradictory(&self, left: &Expression, right: &Expression) -> bool {
        // Simple contradiction detection
        // Example: x > 5 AND x < 3 is contradictory
        if let (
            Expression::BinaryOp {
                left: l1,
                op: BinaryOperator::Gt,
                right: r1,
            },
            Expression::BinaryOp {
                left: l2,
                op: BinaryOperator::Lt,
                right: r2,
            },
        ) = (left, right)
        {
            if l1 == l2 {
                if let (Expression::Literal(val1), Expression::Literal(val2)) =
                    (r1.as_ref(), r2.as_ref())
                {
                    return self.compare_values(val1, val2).unwrap_or(0) >= 0;
                }
            }
        }
        false
    }

    /// Helper: Compare two values
    fn compare_values(&self, left: &Value, right: &Value) -> Result<i32, LogicalPlanError> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(if l < r {
                -1
            } else if l > r {
                1
            } else {
                0
            }),
            _ => Err(LogicalPlanError::InternalError(
                "Cannot compare values".to_string(),
            )),
        }
    }

    /// Helper: Create a new plan with different children
    fn with_new_children(
        &self,
        plan: LogicalPlan,
        children: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match plan {
            LogicalPlan::TableScan(node) => {
                if !children.is_empty() {
                    return Err(LogicalPlanError::InternalError(
                        "TableScan should not have children".to_string(),
                    ));
                }
                Ok(LogicalPlan::TableScan(node))
            }
            LogicalPlan::Projection(mut node) => {
                if children.len() != 1 {
                    return Err(LogicalPlanError::InternalError(
                        "Projection should have exactly one child".to_string(),
                    ));
                }
                node.input = Box::new(children.into_iter().next().unwrap());
                Ok(LogicalPlan::Projection(node))
            }
            LogicalPlan::Filter(mut node) => {
                if children.len() != 1 {
                    return Err(LogicalPlanError::InternalError(
                        "Filter should have exactly one child".to_string(),
                    ));
                }
                node.input = Box::new(children.into_iter().next().unwrap());
                Ok(LogicalPlan::Filter(node))
            }
            _ => Ok(plan), // Simplified - you'd need to handle all plan types
        }
    }
}
