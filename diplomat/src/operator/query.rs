use std::collections::HashMap;

use shared_types::DataType;
use sqlparser::ast::{
    Expr, GroupByExpr, Join, JoinConstraint, JoinOperator, LimitClause, OrderByExpr, OrderByKind,
    Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, TableFactor, TableWithJoins,
    Value as SqlValue,
};

use crate::{
    common::LogicalPlanError,
    expression::Expression,
    logical_plan::{
        AggregateNode, DistinctNode, FilterNode, JoinNode, LimitNode, LogicalPlan, ProjectionNode,
        SortNode, SubqueryNode, TableScanNode, UnionNode, ValuesNode,
    },
    types::{ColumnDef, JoinType, LogicalSchema, SortExpr, SortOrder, TableRef},
    utils::{
        expr_to_column_name, expr_to_logical_expr, has_aggregates, is_aggregate_expr,
        object_name_to_string, values_to_plan,
    },
};

pub struct QueryPlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl QueryPlan {
    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    pub fn generate(&mut self, query: &Query) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = self.set_expr_to_plan(&query.body)?;

        // Apply ORDER BY
        if let Some(order_by) = &query.order_by {
            match &order_by.kind {
                OrderByKind::Expressions(exprs) => {
                    plan = self.apply_order_by(plan, exprs)?;
                }
                _ => {
                    return Err(LogicalPlanError::UnsupportedOperation(
                        "Unsupported ORDER BY kind".to_string(),
                    ));
                }
            }
        }

        // Apply LIMIT and OFFSET
        if let Some(limit_clause) = &query.limit_clause {
            plan = self.apply_limit_clause(plan, limit_clause)?;
        }

        Ok(plan)
    }

    /// Convert a set expression to a logical plan
    pub fn set_expr_to_plan(
        &mut self,
        set_expr: &SetExpr,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match set_expr {
            SetExpr::Select(select) => self.select_to_plan(select),
            SetExpr::Query(query) => self.generate(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_plan = self.set_expr_to_plan(left)?;
                let right_plan = self.set_expr_to_plan(right)?;

                match op {
                    SetOperator::Union => {
                        let all = matches!(set_quantifier, SetQuantifier::All);
                        let schema = left_plan.schema().clone();
                        Ok(LogicalPlan::Union(UnionNode {
                            left: Box::new(left_plan),
                            right: Box::new(right_plan),
                            all,
                            schema,
                            statistics: crate::types::PlanStatistics::unknown(),
                        }))
                    }
                    _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                        "Unsupported set operation: {:?}",
                        op
                    ))),
                }
            }
            SetExpr::Values(values) => values_to_plan(values),
            _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                "Unsupported set expression: {:?}",
                set_expr
            ))),
        }
    }

    /// Convert a SELECT statement to a logical plan
    fn select_to_plan(&mut self, select: &Select) -> Result<LogicalPlan, LogicalPlanError> {
        // Start with FROM clause
        let mut plan = if select.from.is_empty() {
            // SELECT without FROM - create a values node with a single row
            LogicalPlan::Values(ValuesNode {
                values: vec![vec![]],
                schema: LogicalSchema::empty(),
                statistics: crate::types::PlanStatistics::with_row_count(1),
            })
        } else {
            self.from_to_plan(&select.from)?
        };

        // Apply WHERE clause
        if let Some(selection) = &select.selection {
            let predicate = expr_to_logical_expr(selection)?;
            plan = LogicalPlan::Filter(FilterNode {
                predicate,
                input: Box::new(plan),
                statistics: crate::types::PlanStatistics::unknown(),
            });
        }

        // Apply GROUP BY and aggregates
        let has_group_by = match &select.group_by {
            GroupByExpr::All(with_modifier) => false,
            GroupByExpr::Expressions(exprs, with_modifier) => !exprs.is_empty(),
        };

        if has_group_by || has_aggregates(&select.projection) {
            plan = self.apply_group_by(plan, &select.group_by, &select.projection)?;
        }

        // Apply HAVING clause
        if let Some(having) = &select.having {
            let predicate = expr_to_logical_expr(having)?;
            plan = LogicalPlan::Filter(FilterNode {
                predicate,
                input: Box::new(plan),
                statistics: crate::types::PlanStatistics::unknown(),
            });
        }

        // Apply SELECT (projection)
        plan = self.apply_projection(plan, &select.projection)?;

        // Apply DISTINCT
        if select.distinct.is_some() {
            plan = LogicalPlan::Distinct(DistinctNode {
                input: Box::new(plan),
                statistics: crate::types::PlanStatistics::unknown(),
            });
        }

        Ok(plan)
    }

    /// Convert FROM clause to a logical plan
    pub fn from_to_plan(
        &mut self,
        from: &[TableWithJoins],
    ) -> Result<LogicalPlan, LogicalPlanError> {
        if from.is_empty() {
            return Err(LogicalPlanError::SqlParseError(
                "FROM clause cannot be empty".to_string(),
            ));
        }

        let mut plan = self.table_factor_to_plan(&from[0].relation)?;

        // Apply joins
        for join in &from[0].joins {
            plan = self.apply_join(plan, join)?;
        }

        // Handle multiple tables in FROM (implicit cross joins)
        for table_with_joins in &from[1..] {
            let right_plan = self.table_factor_to_plan(&table_with_joins.relation)?;

            // Create cross join
            let left_schema = plan.schema();
            let right_schema = right_plan.schema();
            let mut combined_columns = left_schema.columns.clone();
            combined_columns.extend(right_schema.columns.clone());

            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: JoinType::Cross,
                join_constraint: None,
                schema: LogicalSchema::new(combined_columns),
                statistics: crate::types::PlanStatistics::unknown(),
            });

            // Apply joins for this table
            for join in &table_with_joins.joins {
                plan = self.apply_join(plan, join)?;
            }
        }

        Ok(plan)
    }

    /// Convert a table factor to a logical plan
    fn table_factor_to_plan(
        &mut self,
        table_factor: &TableFactor,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_to_string(name);
                let table_ref = if let Some(alias) = alias {
                    TableRef::with_alias(table_name.clone(), alias.name.value.clone())
                } else {
                    TableRef::new(table_name.clone())
                };

                // Get schema from registered schemas or create a default one
                let schema = self
                    .table_schemas
                    .get(&table_name)
                    .cloned()
                    .unwrap_or_else(|| {
                        // Create a default schema with unknown columns
                        LogicalSchema::new(vec![ColumnDef::with_table(
                            "*",
                            DataType::String,
                            table_ref.effective_name().to_string(),
                        )])
                    });

                Ok(LogicalPlan::TableScan(TableScanNode {
                    table: table_ref,
                    schema,
                    projected_columns: None,
                    filters: vec![],
                    statistics: crate::types::PlanStatistics::unknown(),
                }))
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                let subquery_plan = self.generate(subquery)?;

                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());

                Ok(LogicalPlan::Subquery(SubqueryNode {
                    subquery: Box::new(subquery_plan),
                    alias: Some(alias_name),
                    statistics: crate::types::PlanStatistics::unknown(),
                }))
            }
            _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                "Unsupported table factor: {:?}",
                table_factor
            ))),
        }
    }

    /// Apply a join to the current plan
    fn apply_join(
        &mut self,
        left_plan: LogicalPlan,
        join: &Join,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let right_plan = self.table_factor_to_plan(&join.relation)?;

        let join_type = match join.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::Full,
            JoinOperator::CrossJoin => JoinType::Cross,
            _ => {
                return Err(LogicalPlanError::UnsupportedOperation(format!(
                    "Unsupported join type: {:?}",
                    join.join_operator
                )));
            }
        };

        let join_constraint = match &join.join_operator {
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint) => {
                match constraint {
                    JoinConstraint::On(expr) => Some(expr_to_logical_expr(expr)?),
                    JoinConstraint::Using(columns) => None, // TODO: Implement using style join
                    JoinConstraint::Natural => None,        // TODO: Implement natural join logic
                    JoinConstraint::None => None,
                }
            }
            JoinOperator::CrossJoin => None,
            _ => None,
        };

        // Combine schemas
        let left_schema = left_plan.schema();
        let right_schema = right_plan.schema();
        let mut combined_columns = left_schema.columns.clone();
        combined_columns.extend(right_schema.columns.clone());

        Ok(LogicalPlan::Join(JoinNode {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            join_type,
            join_constraint,
            schema: LogicalSchema::new(combined_columns),
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }

    /// Apply projection to the plan
    fn apply_projection(
        &mut self,
        plan: LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let mut expressions = Vec::new();
        let mut schema_columns = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let logical_expr = expr_to_logical_expr(expr)?;
                    let column_name = expr_to_column_name(expr);
                    expressions.push(logical_expr);
                    schema_columns.push(ColumnDef::new(column_name, DataType::String)); // TODO: Infer type
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let logical_expr = expr_to_logical_expr(expr)?;
                    expressions.push(logical_expr);
                    schema_columns.push(ColumnDef::new(&alias.value, DataType::String)); // TODO: Infer type
                }
                SelectItem::Wildcard(_) => {
                    expressions.push(Expression::wildcard());
                    schema_columns.extend(plan.schema().columns.clone());
                }
                _ => {
                    // QualifiedWildcard is not yet  supported e.g. `alias.*` or even `schema.table.*`
                }
            }
        }

        Ok(LogicalPlan::Projection(ProjectionNode {
            expressions,
            input: Box::new(plan),
            schema: LogicalSchema::new(schema_columns),
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }

    /// Apply GROUP BY and aggregates
    fn apply_group_by(
        &mut self,
        plan: LogicalPlan,
        group_by: &GroupByExpr,
        projection: &[SelectItem],
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let mut group_expr = Vec::new();

        match group_by {
            GroupByExpr::All(all) => {
                // GROUP BY ALL - not commonly supported, treat as no grouping
            }
            GroupByExpr::Expressions(exprs, modifier) => {
                for expr in exprs {
                    group_expr.push(expr_to_logical_expr(expr)?);
                }
            }
        }

        let mut aggr_expr = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if is_aggregate_expr(expr) {
                        aggr_expr.push(expr_to_logical_expr(expr)?);
                    }
                }
                _ => {}
            }
        }

        // Create schema for aggregate result
        let mut schema_columns = Vec::new();

        // Add group by columns
        for (i, _) in group_expr.iter().enumerate() {
            schema_columns.push(ColumnDef::new(format!("group_{}", i), DataType::String));
        }

        // Add aggregate columns
        for (i, _) in aggr_expr.iter().enumerate() {
            schema_columns.push(ColumnDef::new(format!("aggr_{}", i), DataType::String));
        }

        Ok(LogicalPlan::Aggregate(AggregateNode {
            group_expr,
            aggr_expr,
            input: Box::new(plan),
            schema: LogicalSchema::new(schema_columns),
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }

    /// Apply ORDER BY
    fn apply_order_by(
        &mut self,
        plan: LogicalPlan,
        order_by: &[OrderByExpr],
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let mut expressions = Vec::new();

        for order_expr in order_by {
            let expr = expr_to_logical_expr(&order_expr.expr)?;

            // one of the expressions' fields has a field of the same name: `options.`
            let order = if order_expr.options.asc.unwrap_or(true) {
                SortOrder::Ascending
            } else {
                SortOrder::Descending
            };

            let mut sort_expr = SortExpr::new(expr, order);
            if order_expr.options.nulls_first.unwrap_or(false) {
                sort_expr = sort_expr.nulls_first();
            }

            expressions.push(sort_expr);
        }

        Ok(LogicalPlan::Sort(SortNode {
            expressions,
            input: Box::new(plan),
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }

    /// Apply LIMIT clause
    fn apply_limit_clause(
        &mut self,
        plan: LogicalPlan,
        limit_clause: &LimitClause,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let (fetch, skip) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by: _,
            } => {
                // Extract limit value
                let fetch_value = if let Some(limit_expr) = limit {
                    match limit_expr {
                        Expr::Value(value) => match &value.value {
                            SqlValue::Number(n, _) => Some(n.parse::<usize>().map_err(|_| {
                                LogicalPlanError::SqlParseError("Invalid LIMIT value".to_string())
                            })?),
                            _ => {
                                return Err(LogicalPlanError::UnsupportedOperation(
                                    "LIMIT must be a number".to_string(),
                                ));
                            }
                        },
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "LIMIT must be a number".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };

                // Extract offset value
                let skip_value = if let Some(offset_val) = offset {
                    match &offset_val.value {
                        Expr::Value(value) => match &value.value {
                            SqlValue::Number(n, _) => Some(n.parse::<usize>().map_err(|_| {
                                LogicalPlanError::SqlParseError("Invalid OFFSET value".to_string())
                            })?),
                            _ => {
                                return Err(LogicalPlanError::UnsupportedOperation(
                                    "OFFSET must be a number".to_string(),
                                ));
                            }
                        },
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "OFFSET must be a number".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };

                (fetch_value, skip_value)
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                // Extract limit value
                let fetch_value = match limit {
                    Expr::Value(value) => match &value.value {
                        SqlValue::Number(n, _) => n.parse::<usize>().map_err(|_| {
                            LogicalPlanError::SqlParseError("Invalid LIMIT value".to_string())
                        })?,
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "LIMIT must be a number".to_string(),
                            ));
                        }
                    },
                    _ => {
                        return Err(LogicalPlanError::UnsupportedOperation(
                            "LIMIT must be a number".to_string(),
                        ));
                    }
                };

                // Extract offset value
                let skip_value = match offset {
                    Expr::Value(value) => match &value.value {
                        SqlValue::Number(n, _) => n.parse::<usize>().map_err(|_| {
                            LogicalPlanError::SqlParseError("Invalid OFFSET value".to_string())
                        })?,
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "OFFSET must be a number".to_string(),
                            ));
                        }
                    },
                    _ => {
                        return Err(LogicalPlanError::UnsupportedOperation(
                            "OFFSET must be a number".to_string(),
                        ));
                    }
                };

                (Some(fetch_value), Some(skip_value))
            }
        };

        Ok(LogicalPlan::Limit(LimitNode {
            skip,
            fetch,
            input: Box::new(plan),
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }
}
