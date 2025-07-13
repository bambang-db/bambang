use std::collections::HashMap;

use shared_types::DataType;
use sqlparser::ast::{Ident, ObjectName, Query};

use crate::{
    common::LogicalPlanError,
    logical_plan::{InsertNode, InsertSource, LogicalPlan},
    operator::query::QueryPlan,
    types::{ColumnDef, LogicalSchema, TableRef},
    utils::object_name_to_string,
};

pub struct InsertPlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl InsertPlan {
    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    pub fn generate(
        &mut self,
        table_name: &ObjectName,
        columns: &Option<Vec<Ident>>,
        source: &Query,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let table = TableRef::new(object_name_to_string(table_name));
        let mut query = QueryPlan::new(self.table_schemas.clone());

        let column_names = columns
            .as_ref()
            .map(|cols| cols.iter().map(|col| col.value.clone()).collect());

        let source_plan = query.generate(source)?;

        let schema = LogicalSchema::new(vec![ColumnDef::new("rows_affected", DataType::Integer)]);

        Ok(LogicalPlan::Insert(InsertNode {
            table,
            columns: column_names,
            source: InsertSource::Query(Box::new(source_plan)),
            schema,
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }
}
