use diplomat::{optimizer::Optimizer, sql_parser::SQLParser};
use sqlparser::dialect::GenericDialect;

#[tokio::main]
async fn main() {
    let mut parser = SQLParser::new(Box::new(GenericDialect {})); // bring your own dialect, postgresql, mysql, etc
    let optimizer = Optimizer::new();

    let query = r"
        SELECT o.order_id, c.customer_name, p.product_name, o.quantity * 2 + 1 as calculated_qty
        FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
        WHERE o.order_date > '2023-01-01'
            AND o.quantity > 5 * 2
            AND o.quantity > 8
            AND c.status = 'active'
            AND p.price > 100 * 3 - 50;
    ";

    let unoptimized = parser.parse(query).expect("Failed to parse query");

    let optimized = optimizer
        .optimize(unoptimized)
        .expect("Failed to optimize plan");

    println!("{:#?}", &optimized);
}
