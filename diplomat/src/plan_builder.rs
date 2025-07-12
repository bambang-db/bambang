use sqlparser::ast::Statement;

use crate::{common::LogicalPlanError, logical_plan::LogicalPlan};

pub struct PlanBuilder {}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate(&mut self, statement: &Statement) -> Result<LogicalPlan, LogicalPlanError> {
        match statement {
            Statement::Query(query) => {
                todo!("Implement query")
            }
            Statement::Insert(insert) => {
                todo!("Implement insert")
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            } => {
                todo!("Implement update")
            }
            Statement::Delete(delete) => {
                todo!("Implement delete")
            }
            Statement::CreateTable(table) => {
                todo!("Implement create table")
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => {
                todo!("Implement drop table")
            }
            _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                "Unsupported statement: {:?}",
                statement
            ))),
        }
    }

    fn query_to_plan() {}

    fn insert_to_plan() {}

    fn update_to_plan() {}

    fn delete_to_plan() {}

    fn create_table_to_plan() {}

    fn drop_to_plan() {}
}
