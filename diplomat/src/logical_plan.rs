use std::fmt;

use serde::{Deserialize, Serialize};
use shared_types::DataType;

use crate::{
    common::LogicalPlanError,
    expression::Expression,
    types::{JoinType, LogicalSchema, PlanStatistics, SortExpr, TableRef},
};

/// Trait for logical plan nodes
pub trait LogicalPlanNode: fmt::Debug + Clone {
    /// Get the schema produced by this node
    fn schema(&self) -> &LogicalSchema;

    /// Get the children of this node
    fn children(&self) -> Vec<&LogicalPlan>;

    /// Get mutable references to children
    fn children_mut(&mut self) -> Vec<&mut LogicalPlan>;

    /// Create a new node with different children
    fn with_new_children(
        &self,
        children: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan, LogicalPlanError>;

    /// Get statistics for this node
    fn statistics(&self) -> &PlanStatistics;

    /// Validate this node
    fn validate(&self) -> Result<(), LogicalPlanError>;

    /// Get a human-readable description
    fn description(&self) -> String;
}

/// Main logical plan enum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Table scan operation
    TableScan(TableScanNode),

    /// Projection operation
    Projection(ProjectionNode),

    /// Filter operation
    Filter(FilterNode),

    /// Join operation
    Join(JoinNode),

    /// Aggregate operation
    Aggregate(AggregateNode),

    /// Sort operation
    Sort(SortNode),

    /// Limit operation
    Limit(LimitNode),

    /// Insert operation
    Insert(InsertNode),

    /// Update operation
    Update(UpdateNode),

    /// Delete operation
    Delete(DeleteNode),

    /// Create table operation
    CreateTable(CreateTableNode),

    /// Drop table operation
    DropTable(DropTableNode),

    /// Union operation
    Union(UnionNode),

    /// Distinct operation
    Distinct(DistinctNode),

    /// Values operation (for VALUES clause)
    Values(ValuesNode),

    /// Subquery operation
    Subquery(SubqueryNode),
}

/// Table scan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableScanNode {
    pub table: TableRef,
    pub schema: LogicalSchema,
    pub projected_columns: Option<Vec<String>>,
    pub filters: Vec<Expression>,
    pub statistics: PlanStatistics,
}

/// Projection node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionNode {
    pub expressions: Vec<Expression>,
    pub input: Box<LogicalPlan>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Filter node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterNode {
    pub predicate: Expression,
    pub input: Box<LogicalPlan>,
    pub statistics: PlanStatistics,
}

/// Join node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub join_constraint: Option<Expression>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Aggregate node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateNode {
    pub group_expr: Vec<Expression>,
    pub aggr_expr: Vec<Expression>,
    pub input: Box<LogicalPlan>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Sort node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortNode {
    pub expressions: Vec<SortExpr>,
    pub input: Box<LogicalPlan>,
    pub statistics: PlanStatistics,
}

/// Limit node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitNode {
    pub skip: Option<usize>,
    pub fetch: Option<usize>,
    pub input: Box<LogicalPlan>,
    pub statistics: PlanStatistics,
}

/// Insert node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertNode {
    pub table: TableRef,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Insert source
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),
    Query(Box<LogicalPlan>),
}

/// Update node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateNode {
    pub table: TableRef,
    pub assignments: Vec<UpdateAssignment>,
    pub filter: Option<Expression>,
    pub from: Option<Box<LogicalPlan>>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Update assignment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateAssignment {
    pub column: String,
    pub value: Expression,
}

/// Delete node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteNode {
    pub table: TableRef,
    pub filter: Option<Expression>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Create table node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableNode {
    pub table: TableRef,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Column definition for CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expression>,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
}

/// Table constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        foreign_table: String,
        foreign_columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    Check {
        expr: Expression,
    },
}

/// Drop table node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableNode {
    pub tables: Vec<TableRef>,
    pub if_exists: bool,
    pub cascade: bool,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Union node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnionNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub all: bool, // true for UNION ALL, false for UNION
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Distinct node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistinctNode {
    pub input: Box<LogicalPlan>,
    pub statistics: PlanStatistics,
}

/// Values node (for VALUES clause)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValuesNode {
    pub values: Vec<Vec<Expression>>,
    pub schema: LogicalSchema,
    pub statistics: PlanStatistics,
}

/// Subquery node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubqueryNode {
    pub subquery: Box<LogicalPlan>,
    pub alias: Option<String>,
    pub statistics: PlanStatistics,
}

// Implementation of LogicalPlanNode for each node type
impl LogicalPlanNode for TableScanNode {
    fn schema(&self) -> &LogicalSchema {
        &self.schema
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn children_mut(&mut self) -> Vec<&mut LogicalPlan> {
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        if !children.is_empty() {
            return Err(LogicalPlanError::InternalError(
                "TableScan should not have children".to_string(),
            ));
        }
        Ok(LogicalPlan::TableScan(self.clone()))
    }

    fn statistics(&self) -> &PlanStatistics {
        &self.statistics
    }

    fn validate(&self) -> Result<(), LogicalPlanError> {
        if self.table.name.is_empty() {
            return Err(LogicalPlanError::ValidationError(
                "Table name cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    fn description(&self) -> String {
        format!("TableScan: {}", self.table.name)
    }
}

impl LogicalPlanNode for ProjectionNode {
    fn schema(&self) -> &LogicalSchema {
        &self.schema
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn children_mut(&mut self) -> Vec<&mut LogicalPlan> {
        vec![&mut self.input]
    }

    fn with_new_children(
        &self,
        mut children: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        if children.len() != 1 {
            return Err(LogicalPlanError::InternalError(
                "Projection should have exactly one child".to_string(),
            ));
        }
        let mut new_node = self.clone();
        new_node.input = Box::new(children.remove(0));
        Ok(LogicalPlan::Projection(new_node))
    }

    fn statistics(&self) -> &PlanStatistics {
        &self.statistics
    }

    fn validate(&self) -> Result<(), LogicalPlanError> {
        if self.expressions.is_empty() {
            return Err(LogicalPlanError::ValidationError(
                "Projection must have at least one expression".to_string(),
            ));
        }
        Ok(())
    }

    fn description(&self) -> String {
        format!("Projection: {} expressions", self.expressions.len())
    }
}

impl LogicalPlanNode for FilterNode {
    fn schema(&self) -> &LogicalSchema {
        self.input.schema()
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn children_mut(&mut self) -> Vec<&mut LogicalPlan> {
        vec![&mut self.input]
    }

    fn with_new_children(
        &self,
        mut children: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        if children.len() != 1 {
            return Err(LogicalPlanError::InternalError(
                "Filter should have exactly one child".to_string(),
            ));
        }
        let mut new_node = self.clone();
        new_node.input = Box::new(children.remove(0));
        Ok(LogicalPlan::Filter(new_node))
    }

    fn statistics(&self) -> &PlanStatistics {
        &self.statistics
    }

    fn validate(&self) -> Result<(), LogicalPlanError> {
        // Validate that predicate references valid columns
        Ok(())
    }

    fn description(&self) -> String {
        format!("Filter: {}", self.predicate)
    }
}

// Helper methods for LogicalPlan
impl LogicalPlan {
    /// Get the schema of this plan
    pub fn schema(&self) -> &LogicalSchema {
        match self {
            LogicalPlan::TableScan(node) => node.schema(),
            LogicalPlan::Projection(node) => node.schema(),
            LogicalPlan::Filter(node) => node.schema(),
            LogicalPlan::Join(node) => &node.schema,
            LogicalPlan::Aggregate(node) => &node.schema,
            LogicalPlan::Sort(node) => node.input.schema(),
            LogicalPlan::Limit(node) => node.input.schema(),
            LogicalPlan::Insert(node) => &node.schema,
            LogicalPlan::Update(node) => &node.schema,
            LogicalPlan::Delete(node) => &node.schema,
            LogicalPlan::CreateTable(node) => &node.schema,
            LogicalPlan::DropTable(node) => &node.schema,
            LogicalPlan::Union(node) => &node.schema,
            LogicalPlan::Distinct(node) => node.input.schema(),
            LogicalPlan::Values(node) => &node.schema,
            LogicalPlan::Subquery(node) => node.subquery.schema(),
        }
    }

    /// Get the children of this plan
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::Projection(node) => vec![&node.input],
            LogicalPlan::Filter(node) => vec![&node.input],
            LogicalPlan::Join(node) => vec![&node.left, &node.right],
            LogicalPlan::Aggregate(node) => vec![&node.input],
            LogicalPlan::Sort(node) => vec![&node.input],
            LogicalPlan::Limit(node) => vec![&node.input],
            LogicalPlan::Insert(node) => match &node.source {
                InsertSource::Query(plan) => vec![plan],
                InsertSource::Values(_) => vec![],
            },
            LogicalPlan::Update(node) => {
                if let Some(from) = &node.from {
                    vec![from]
                } else {
                    vec![]
                }
            }
            LogicalPlan::Delete(_) => vec![],
            LogicalPlan::CreateTable(_) => vec![],
            LogicalPlan::DropTable(_) => vec![],
            LogicalPlan::Union(node) => vec![&node.left, &node.right],
            LogicalPlan::Distinct(node) => vec![&node.input],
            LogicalPlan::Values(_) => vec![],
            LogicalPlan::Subquery(node) => vec![&node.subquery],
        }
    }

    /// Get a human-readable description of this plan
    pub fn description(&self) -> String {
        match self {
            LogicalPlan::TableScan(node) => node.description(),
            LogicalPlan::Projection(node) => node.description(),
            LogicalPlan::Filter(node) => node.description(),
            LogicalPlan::Join(node) => format!("Join: {:?}", node.join_type),
            LogicalPlan::Aggregate(node) => format!(
                "Aggregate: {} groups, {} aggregates",
                node.group_expr.len(),
                node.aggr_expr.len()
            ),
            LogicalPlan::Sort(node) => format!("Sort: {} expressions", node.expressions.len()),
            LogicalPlan::Limit(node) => {
                format!("Limit: skip={:?}, fetch={:?}", node.skip, node.fetch)
            }
            LogicalPlan::Insert(node) => format!("Insert: {}", node.table.name),
            LogicalPlan::Update(node) => format!("Update: {}", node.table.name),
            LogicalPlan::Delete(node) => format!("Delete: {}", node.table.name),
            LogicalPlan::CreateTable(node) => format!("CreateTable: {}", node.table.name),
            LogicalPlan::DropTable(node) => format!("DropTable: {} tables", node.tables.len()),
            LogicalPlan::Union(node) => format!("Union: all={}", node.all),
            LogicalPlan::Distinct(_) => "Distinct".to_string(),
            LogicalPlan::Values(node) => format!("Values: {} rows", node.values.len()),
            LogicalPlan::Subquery(_) => "Subquery".to_string(),
        }
    }

    /// Validate this plan
    pub fn validate(&self) -> Result<(), LogicalPlanError> {
        match self {
            LogicalPlan::TableScan(node) => node.validate(),
            LogicalPlan::Projection(node) => node.validate(),
            LogicalPlan::Filter(node) => node.validate(),
            // Add validation for other node types as needed
            _ => Ok(()),
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}
