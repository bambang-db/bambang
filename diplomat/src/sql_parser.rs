use sqlparser::{dialect::Dialect, parser::Parser};

use crate::{common::LogicalPlanError, logical_plan::LogicalPlan, plan_builder::PlanBuilder};

pub struct SQLParser {
    dialect: Box<dyn Dialect>,
    builder: PlanBuilder,
}

impl SQLParser {
    pub fn new(dialect: Box<dyn Dialect>) -> Self {
        Self {
            dialect: dialect,
            builder: PlanBuilder::new(),
        }
    }

    pub fn parse(&mut self, sql: &str) -> Result<LogicalPlan, LogicalPlanError> {
        let statements = Parser::parse_sql(self.dialect.as_ref(), sql)?;

        if statements.is_empty() {
            return Err(LogicalPlanError::SqlParseError(
                "No statements found".to_string(),
            ));
        }

        if statements.len() > 1 {
            return Err(LogicalPlanError::UnsupportedOperation(
                "Multiple statements not supported".to_string(),
            ));
        }

        let stmt = &statements[0];
        let plan = self
            .builder
            .generate(stmt)
            .expect("Logical plan generation fail");

        Ok(plan)
    }
}
