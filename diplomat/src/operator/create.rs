use std::collections::HashMap;

use shared_types::DataType as LogicalDataType;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, ObjectName, TableConstraint};

use crate::{
    common::LogicalPlanError,
    logical_plan::{ColumnDefinition, CreateTableNode, LogicalPlan},
    types::{ColumnDef, LogicalSchema, TableRef},
    utils::{expr_to_logical_expr, object_name_to_string, sql_data_type_to_data_type},
};

pub struct CreatePlan {
    table_schemas: HashMap<String, LogicalSchema>,
}

impl CreatePlan {
    pub fn new(table_schemas: HashMap<String, LogicalSchema>) -> Self {
        Self {
            table_schemas: table_schemas,
        }
    }

    /// Convert CREATE TABLE statement to logical plan
    pub fn create_table(
        &self,
        name: &ObjectName,
        columns: &[SQLColumnDef],
        constraints: &[TableConstraint],
        if_not_exists: bool,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let table = TableRef::new(object_name_to_string(name));

        let mut column_defs = Vec::new();
        for col in columns {
            let data_type = sql_data_type_to_data_type(&col.data_type)?;
            let mut column_def = ColumnDefinition {
                name: col.name.value.clone(),
                data_type,
                nullable: true,
                default: None,
                primary_key: false,
                unique: false,
                auto_increment: false,
            };

            // Process column options
            for option in &col.options {
                match &option.option {
                    ColumnOption::NotNull => column_def.nullable = false,
                    ColumnOption::Null => column_def.nullable = true,
                    ColumnOption::Default(expr) => {
                        column_def.default = Some(expr_to_logical_expr(expr)?);
                    }
                    ColumnOption::Unique { is_primary, .. } => {
                        if *is_primary {
                            column_def.primary_key = true;
                            column_def.nullable = false;
                        } else {
                            column_def.unique = true;
                        }
                    }
                    _ => {} // Ignore other options for now
                }
            }

            column_defs.push(column_def);
        }

        let mut table_constraints = Vec::new();
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    name,
                    index_name,
                    index_type_display,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                    nulls_distinct,
                } => {
                    let column_names = columns.iter().map(|c| c.value.clone()).collect();

                    table_constraints.push(crate::logical_plan::TableConstraint::Unique {
                        columns: column_names,
                    });
                }
                TableConstraint::PrimaryKey {
                    name,
                    index_name,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                } => {
                    let column_names = columns.iter().map(|c| c.value.clone()).collect();
                    table_constraints.push(crate::logical_plan::TableConstraint::PrimaryKey {
                        columns: column_names,
                    });
                }
                TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    let column_names = columns.iter().map(|c| c.value.clone()).collect();
                    let foreign_table_name = object_name_to_string(foreign_table);
                    let foreign_column_names =
                        referred_columns.iter().map(|c| c.value.clone()).collect();
                    table_constraints.push(crate::logical_plan::TableConstraint::ForeignKey {
                        columns: column_names,
                        foreign_table: foreign_table_name,
                        foreign_columns: foreign_column_names,
                    });
                }
                TableConstraint::Check { expr, .. } => {
                    let check_expr = expr_to_logical_expr(expr)?;
                    table_constraints
                        .push(crate::logical_plan::TableConstraint::Check { expr: check_expr });
                }
                _ => {} // Ignore other constraints for now
            }
        }

        let schema = LogicalSchema::new(vec![ColumnDef::new(
            "table_created",
            LogicalDataType::Boolean,
        )]);

        Ok(LogicalPlan::CreateTable(CreateTableNode {
            table,
            columns: column_defs,
            constraints: table_constraints,
            if_not_exists,
            schema,
            statistics: crate::types::PlanStatistics::unknown(),
        }))
    }
}
