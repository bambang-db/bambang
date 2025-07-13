use std::collections::HashMap;

use shared_types::DataType;
use sqlparser::ast::{Expr, ObjectName, TableFactor, TableWithJoins};

use crate::{common::LogicalPlanError, logical_plan::{DeleteNode, LogicalPlan}, types::{ColumnDef, LogicalSchema, TableRef}, utils::{expr_to_logical_expr, object_name_to_string}};

pub struct DeletePlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl DeletePlan {

    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    /// Convert DELETE statement to logical plan
    pub fn generate(
        &self,
        _tables: &[ObjectName],
        from: &[TableWithJoins],
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        if from.is_empty() {
            return Err(LogicalPlanError::SqlParseError(
                "DELETE must specify FROM table".to_string(),
            ));
        }

        // Get the first table as the target
        let table_ref = match &from[0].relation {
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
                    "DELETE with subquery not supported".to_string(),
                ));
            }
        };

        let filter = if let Some(selection) = selection {
            Some(expr_to_logical_expr(selection)?)
        } else {
            None
        };

        let schema = LogicalSchema::new(vec![ColumnDef::new("rows_affected", DataType::Integer)]);

        Ok(LogicalPlan::Delete(DeleteNode {
            table: table_ref,
            filter,
            schema,
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }
}
