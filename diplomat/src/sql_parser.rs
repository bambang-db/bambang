use sqlparser::{dialect::Dialect, parser::Parser};

use crate::common::SQLParserError;

pub struct SQLParser {
    dialect: Box<dyn Dialect>,
}

impl SQLParser {
    pub fn new(dialect: Box<dyn Dialect>) -> Self {
        Self { dialect: dialect }
    }

    pub fn parse(&self, sql: &str) -> Result<(), SQLParserError> {
        let ast = Parser::parse_sql(self.dialect.as_ref(), sql).unwrap();

        if ast.is_empty() {
            return Err(SQLParserError::InvalidStatement(
                "Statements Not Found".to_string(),
            ));
        }

        let stmt = ast[0].clone();

        println!("{:#?}", stmt);

        Ok(())
    }
}
