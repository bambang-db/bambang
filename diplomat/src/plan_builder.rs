use crate::logical_plan::*;
use crate::operator::create::CreatePlan;
use crate::operator::delete::DeletePlan;
use crate::operator::drop::DropPlan;
use crate::operator::insert::InsertPlan;
use crate::operator::update::UpdatePlan;
use crate::types::LogicalSchema;
use crate::{common::LogicalPlanError, operator::query::QueryPlan};
use sqlparser::ast::{self, Statement, TableObject};
use std::collections::HashMap;

pub struct PlanBuilder {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {
            table_schemas: HashMap::new(),
        }
    }

    /// Add a table schema for validation
    pub fn with_table_schema(mut self, table_name: String, schema: LogicalSchema) -> Self {
        self.table_schemas.insert(table_name, schema);
        self
    }

    pub fn generate(&mut self, statement: &Statement) -> Result<LogicalPlan, LogicalPlanError> {
        match statement {
            Statement::Query(query) => {
                let mut builder = QueryPlan::new(self.table_schemas.clone());
                builder.generate(&query)
            }
            Statement::Insert(insert) => {
                let mut builder: InsertPlan = InsertPlan::new(self.table_schemas.clone());
                if let Some(source) = &insert.source {
                    match &insert.table {
                        TableObject::TableName(table_name) => {
                            return builder.generate(
                                table_name,
                                &Some(insert.columns.clone()),
                                source,
                            );
                        }
                        _ => {
                            return Err(LogicalPlanError::UnsupportedOperation(
                                "Complex table objects not supported".to_string(),
                            ));
                        }
                    };
                } else {
                    Err(LogicalPlanError::UnsupportedOperation(
                        "INSERT without source not supported".to_string(),
                    ))
                }
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning: _,
                or: _,
            } => {
                let mut builder = UpdatePlan::new(self.table_schemas.clone());

                // Convert from UpdateTableFromKind to Vec<TableWithJoins> if needed
                let from_tables = match from {
                    Some(ast::UpdateTableFromKind::AfterSet(tables)) => Some(tables.clone()),
                    Some(ast::UpdateTableFromKind::BeforeSet(tables)) => Some(tables.clone()),
                    _ => None,
                };

                builder.generate(table, assignments, &from_tables, selection)
            }
            Statement::Delete(delete) => {
                let builder = DeletePlan::new(self.table_schemas.clone());

                // Extract tables from FromTable
                let tables = match &delete.from {
                    ast::FromTable::WithFromKeyword(tables) => tables,
                    ast::FromTable::WithoutKeyword(tables) => tables,
                };

                builder.generate(&delete.tables, tables, &delete.selection)
            }
            Statement::CreateTable(table) => {
                let builder = CreatePlan::new(self.table_schemas.clone());
                builder.create_table(
                    &table.name,
                    &table.columns,
                    &table.constraints,
                    table.if_not_exists,
                )
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict: _,
                purge: _,
                temporary: _,
                table: _,
            } => {
                let builder = DropPlan::new(self.table_schemas.clone());
                builder.drop_table(object_type, names, *if_exists, *cascade)
            }
            _ => Err(LogicalPlanError::UnsupportedOperation(format!(
                "Unsupported statement: {:?}",
                statement
            ))),
        }
    }
}
