use std::collections::HashMap;

use shared_types::DataType;
use sqlparser::ast::{Assignment, AssignmentTarget, Expr, TableFactor, TableWithJoins};

use crate::{
    common::LogicalPlanError,
    logical_plan::{LogicalPlan, UpdateAssignment, UpdateNode},
    operator::query::QueryPlan,
    types::{ColumnDef, LogicalSchema, TableRef},
    utils::{expr_to_logical_expr, object_name_to_string},
};

pub struct UpdatePlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl UpdatePlan {
    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    /// Convert UPDATE statement to logical plan
    pub fn generate(
        &mut self,
        table: &TableWithJoins,
        assignments: &[Assignment],
        from: &Option<Vec<TableWithJoins>>,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let mut query = QueryPlan::new(self.table_schemas.clone());

        // Get table name from the first table in the update
        let table_ref = match &table.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_to_string(name);
                if let Some(alias) = alias {
                    TableRef::with_alias(table_name, alias.name.value.clone())
                } else {
                    TableRef::new(table_name)
                }
            }
            _ => {
                return Err(LogicalPlanError::UnsupportedOperation(
                    "UPDATE with subquery not supported".to_string(),
                ));
            }
        };

        let mut update_assignments = Vec::new();
        for assignment in assignments {
            let column = match &assignment.target {
                AssignmentTarget::ColumnName(name) => object_name_to_string(name),
                _ => {
                    return Err(LogicalPlanError::UnsupportedOperation(
                        "Complex assignment targets not supported".to_string(),
                    ));
                }
            };

            let value = expr_to_logical_expr(&assignment.value)?;
            update_assignments.push(UpdateAssignment { column, value });
        }

        let filter = if let Some(selection) = selection {
            Some(expr_to_logical_expr(selection)?)
        } else {
            None
        };

        let from_plan = if let Some(from_tables) = from {
            Some(Box::new(query.from_to_plan(from_tables)?))
        } else {
            None
        };

        let schema = LogicalSchema::new(vec![ColumnDef::new("rows_affected", DataType::Integer)]);

        Ok(LogicalPlan::Update(UpdateNode {
            table: table_ref,
            assignments: update_assignments,
            filter,
            from: from_plan,
            schema,
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }
}
