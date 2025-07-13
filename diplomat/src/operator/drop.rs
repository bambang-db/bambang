use std::collections::HashMap;

use shared_types::DataType;
use sqlparser::ast::{ObjectName, ObjectType};

use crate::{
    common::LogicalPlanError,
    logical_plan::{DropTableNode, LogicalPlan},
    types::{ColumnDef, LogicalSchema, TableRef},
    utils::object_name_to_string,
};

pub struct DropPlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl DropPlan {
    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    /// Convert DROP statement to logical plan
    pub fn drop_table(
        &self,
        object_type: &ObjectType,
        names: &[ObjectName],
        if_exists: bool,
        cascade: bool,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match object_type {
            ObjectType::Table => {
                let tables = names
                    .iter()
                    .map(|name| TableRef::new(object_name_to_string(name)))
                    .collect();

                let schema =
                    LogicalSchema::new(vec![ColumnDef::new("tables_dropped", DataType::Integer)]);

                Ok(LogicalPlan::DropTable(DropTableNode {
                    tables,
                    if_exists,
                    cascade,
                    schema,
                    statistics: crate::types::PlanStatistics::unknown(),
                }))
            }
            _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                "Unsupported object type: {:?}",
                object_type
            ))),
        }
    }
}
