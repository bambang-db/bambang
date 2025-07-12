use diplomat::sql_parser::SQLParser;
use sqlparser::dialect::GenericDialect;

#[tokio::main]
async fn main() {
    let mut parser = SQLParser::new(Box::new(GenericDialect {}));
    parser
        .parse("SELECT id, name, age FROM users WHERE age > 18 ORDER BY name LIMIT 10")
        .expect("Failed to parse query");
}
