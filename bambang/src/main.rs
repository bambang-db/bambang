use matan::{
    manager::CatalogManager,
    schema::{Column, DataType, Schema},
};

#[tokio::main]
async fn main() {
    let mut manager = CatalogManager::new("test.catalog", "test_db".to_string()).unwrap();

    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            nullable: false,
            primary_key: false,
        },
    ];

    let schema = Schema::new(columns);

    manager
        .create_table("users".to_string(), schema, "users.db".to_string())
        .unwrap();

    let user_schema = manager.get_schema("users").unwrap();

    println!("{:#?}", user_schema);
}
