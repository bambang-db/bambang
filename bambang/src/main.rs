use bindereh::{
    manager::Manager,
    operator::{
        insert::InsertOperation,
        join::{HashJoinOperation, JoinCondition, JoinType},
        scan::ScanOperation,
    },
    page::Page,
};
use shared_types::{Column, DataType, Predicate, Row, ScanOptions, Schema, Value};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

// SSB Standard Scale Factors
const SCALE_FACTOR: i32 = 1; // SF1 = 1, SF10 = 10, SF100 = 100

// SSB Scaled-Down Row Counts (maintains proportional relationships)
const LINEORDER_ROWS: u64 = 600_000; // 600k rows (10% of 6M) - still large enough for join testing
const DATE_ROWS: u64 = 2_556; // Keep full date dimension - essential for time-based queries
const CUSTOMER_ROWS: u64 = 3_000; // 3k customers (10% of 30k) - maintains customer distribution
const SUPPLIER_ROWS: u64 = 200; // 200 suppliers (10% of 2k) - sufficient for supplier analysis
const PART_ROWS: u64 = 20_000; // 20k parts (10% of 200k) - adequate part variety

async fn setup_lineorder_table(
    manager: Arc<Manager>,
) -> Result<(u64, Schema), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Column {
            name: "lo_orderkey".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_linenumber".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_custkey".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_partkey".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_suppkey".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_orderdate".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_orderpriority".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_shippriority".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_quantity".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_extendedprice".to_string(),
            data_type: DataType::Float,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_ordtotalprice".to_string(),
            data_type: DataType::Float,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_discount".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_revenue".to_string(),
            data_type: DataType::Float,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_supplycost".to_string(),
            data_type: DataType::Float,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_tax".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_commitdate".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "lo_shipmode".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
    ]);

    let mut root_page_id = manager.allocate_page().await;
    let root_node = Page {
        page_id: root_page_id,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: true,
    };
    manager.write_page(&root_node).await?;
    manager.register_leaf_page(root_page_id).await?;

    let insert_op = InsertOperation::new(manager.clone());
    let batch_size = 10_000;
    let total_rows = LINEORDER_ROWS * SCALE_FACTOR as u64;

    println!("Generating {} LINEORDER rows...", total_rows);

    for batch_start in (0..total_rows).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size as u64, total_rows);
        let mut batch_rows = Vec::new();

        for i in batch_start..batch_end {
            let order_key = (i / 7) + 1; // Average 7 line items per order
            let line_number = (i % 7) + 1;
            let cust_key = (i % CUSTOMER_ROWS) + 1;
            let part_key = (i % PART_ROWS) + 1;
            let supp_key = (i % SUPPLIER_ROWS) + 1;

            // Date distribution: 1992-1998 (7 years)
            let days_since_1992 = i % 2556;
            let date_key = 19920101 + days_since_1992 as i64;

            let quantity = 1 + (i % 50);
            let extended_price = 901.0 + (i as f64 % 104949.0);
            let discount = 0 + (i % 11);
            let tax = 0 + (i % 9);
            let revenue = extended_price * (100.0 - discount as f64) / 100.0;
            let supply_cost = 1.0 + (i as f64 % extended_price * 0.6);

            batch_rows.push(Row {
                id: i + 1,
                data: vec![
                    Value::Integer(order_key as i64),   // lo_orderkey
                    Value::Integer(line_number as i64), // lo_linenumber
                    Value::Integer(cust_key as i64),    // lo_custkey
                    Value::Integer(part_key as i64),    // lo_partkey
                    Value::Integer(supp_key as i64),    // lo_suppkey
                    Value::Integer(date_key),           // lo_orderdate
                    Value::Integer(1 + (i % 5) as i64), // lo_orderpriority
                    Value::Integer(0),                  // lo_shippriority
                    Value::Integer(quantity as i64),    // lo_quantity
                    Value::Float(extended_price),       // lo_extendedprice
                    Value::Float(extended_price * 4.0), // lo_ordtotalprice
                    Value::Integer(discount as i64),    // lo_discount
                    Value::Float(revenue),              // lo_revenue
                    Value::Float(supply_cost),          // lo_supplycost
                    Value::Integer(tax as i64),         // lo_tax
                    Value::Integer(date_key + 30),      // lo_commitdate
                    Value::Integer(1 + (i % 7) as i64), // lo_shipmode
                ],
            });
        }

        let insert_result = insert_op.execute_batch(batch_rows, root_page_id).await?;
        if let Some(new_root) = insert_result.new_root_id {
            root_page_id = new_root;
        }

        if batch_start % 100_000 == 0 {
            println!("Inserted {} / {} LINEORDER rows", batch_start, total_rows);
        }
    }

    println!("Completed LINEORDER table with {} rows", total_rows);
    Ok((root_page_id, schema))
}

async fn setup_dates_table(
    manager: Arc<Manager>,
) -> Result<(u64, Schema), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Column {
            name: "d_datekey".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
        },
        Column {
            name: "d_date".to_string(),
            data_type: DataType::Integer, // Using integer for simplicity
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_dayofweek".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_month".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_year".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_yearmonthnum".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_yearmonth".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_daynuminweek".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_daynuminmonth".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_daynuminyear".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_monthnuminyear".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_weeknuminyear".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_sellingseason".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_lastdayinweekfl".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_lastdayinmonthfl".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_holidayfl".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
        Column {
            name: "d_weekdayfl".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: false,
        },
    ]);

    let mut root_page_id = manager.allocate_page().await;
    let root_node = Page {
        page_id: root_page_id,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: true,
    };
    manager.write_page(&root_node).await?;
    manager.register_leaf_page(root_page_id).await?;

    let insert_op = InsertOperation::new(manager.clone());
    let mut batch_rows = Vec::new();
    let mut row_id = 1;

    println!("Generating {} DATE rows...", DATE_ROWS);

    // Generate dates from 1992-01-01 to 1998-12-31
    for year in 1992..=1998 {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        let mut day_in_year = 1;

        for month in 1..=12 {
            let days_in_month = get_days_in_month(month, year);

            for day in 1..=days_in_month {
                let date_key = year * 10000 + month * 100 + day;
                let year_month_num = year * 100 + month;
                let week_num = ((day_in_year - 1) / 7) + 1;
                let day_of_week = ((day_in_year - 1) % 7) + 1;

                batch_rows.push(Row {
                    id: row_id,
                    data: vec![
                        Value::Integer(date_key as i64),                          // d_datekey
                        Value::Integer(date_key as i64),                          // d_date
                        Value::Integer(day_of_week),                              // d_dayofweek
                        Value::Integer(month),                                    // d_month
                        Value::Integer(year),                                     // d_year
                        Value::Integer(year_month_num),                           // d_yearmonthnum
                        Value::Integer(year_month_num),                           // d_yearmonth
                        Value::Integer(day_of_week),                              // d_daynuminweek
                        Value::Integer(day),                                      // d_daynuminmonth
                        Value::Integer(day_in_year),                              // d_daynuminyear
                        Value::Integer(month),             // d_monthnuminyear
                        Value::Integer(week_num),          // d_weeknuminyear
                        Value::Integer(get_season(month)), // d_sellingseason
                        Value::Integer(if day_of_week == 7 { 1 } else { 0 }), // d_lastdayinweekfl
                        Value::Integer(if day == days_in_month { 1 } else { 0 }), // d_lastdayinmonthfl
                        Value::Integer(0),                                        // d_holidayfl
                        Value::Integer(if day_of_week <= 5 { 1 } else { 0 }),     // d_weekdayfl
                    ],
                });

                row_id += 1;
                day_in_year += 1;
            }
        }
    }

    let insert_result = insert_op.execute_batch(batch_rows, root_page_id).await?;
    if let Some(new_root) = insert_result.new_root_id {
        root_page_id = new_root;
    }

    println!("Completed DATE table with {} rows", DATE_ROWS);
    Ok((root_page_id, schema))
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn get_days_in_month(month: i64, year: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn get_season(month: i64) -> i64 {
    match month {
        12 | 1 | 2 => 1,  // Winter
        3 | 4 | 5 => 2,   // Spring
        6 | 7 | 8 => 3,   // Summer
        9 | 10 | 11 => 4, // Fall
        _ => 1,
    }
}

fn get_memory_usage() -> u64 {
    if cfg!(target_os = "windows") {
        if let Ok(output) = Command::new("tasklist")
            .args(&[
                "/FI",
                "PID eq",
                &std::process::id().to_string(),
                "/FO",
                "CSV",
            ])
            .output()
        {
            if let Ok(output_str) = String::from_utf8(output.stdout) {
                if let Some(line) = output_str.lines().nth(1) {
                    if let Some(mem_str) = line.split(',').nth(4) {
                        let mem_str = mem_str.trim_matches('"').replace(",", "");
                        if let Ok(mem_kb) = mem_str.parse::<u64>() {
                            return mem_kb * 1024;
                        }
                    }
                }
            }
        }
    }
    0
}

fn get_cpu_usage() -> f64 {
    if cfg!(target_os = "windows") {
        if let Ok(output) = Command::new("wmic")
            .args(&[
                "process",
                "where",
                &format!("ProcessId={}", std::process::id()),
                "get",
                "PageFileUsage,WorkingSetSize",
            ])
            .output()
        {
            if let Ok(_) = String::from_utf8(output.stdout) {
                return 0.0;
            }
        }
    }
    0.0
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let initial_memory = get_memory_usage();

    println!(
        "=== SSB Standard Benchmark (Scale Factor {}) ===",
        SCALE_FACTOR
    );
    println!("Expected data volumes:");
    println!("  LINEORDER: {} rows", LINEORDER_ROWS * SCALE_FACTOR as u64);
    println!("  DATE: {} rows", DATE_ROWS);
    println!("Initial Memory: {} MB", initial_memory / 1024 / 1024);

    let setup_start = Instant::now();
    // Increase buffer pool size significantly for better caching
    // 16384 pages * 32KB = 512MB buffer pool (vs previous 32MB)
    let lineorder_manager = Arc::new(Manager::new("lineorder.db", 16384).await?);
    let dates_manager = Arc::new(Manager::new("dates.db", 4096).await?);

    let (lineorder_root, lineorder_schema) =
        setup_lineorder_table(lineorder_manager.clone()).await?;
    let (dates_root, dates_schema) = setup_dates_table(dates_manager.clone()).await?;
    let setup_time = setup_start.elapsed();
    println!("Table Setup Time: {:.2}s", setup_time.as_secs_f64());

    // SSB Q1.1: SELECT sum(lo_extendedprice*lo_discount) as revenue
    //           FROM lineorder, dates
    //           WHERE lo_orderdate = d_datekey
    //           AND d_year = 1993
    //           AND lo_discount between 1 and 3
    //           AND lo_quantity < 25;

    let scan_op_lineorder = ScanOperation::new(lineorder_manager.clone(), 2);
    let scan_op_dates = ScanOperation::new(dates_manager.clone(), 2);

    let dates_scan_options = ScanOptions {
        schema: Some(dates_schema.clone()),
        predicate: Some(Predicate::ColumnEquals {
            column: "d_year".to_string(),
            value: Value::Integer(1993),
        }),
        parallel: true,
        ..Default::default()
    };

    let lineorder_scan_options = ScanOptions {
        schema: Some(lineorder_schema.clone()),
        predicate: Some(Predicate::And(
            Box::new(Predicate::And(
                Box::new(Predicate::ColumnGreaterThanOrEqual {
                    column: "lo_discount".to_string(),
                    value: Value::Integer(1),
                }),
                Box::new(Predicate::ColumnLessThanOrEqual {
                    column: "lo_discount".to_string(),
                    value: Value::Integer(3),
                }),
            )),
            Box::new(Predicate::ColumnLessThan {
                column: "lo_quantity".to_string(),
                value: Value::Integer(25),
            }),
        )),
        parallel: true,
        ..Default::default()
    };

    let scan_start = Instant::now();
    let dates_result = scan_op_dates
        .execute(dates_root, dates_scan_options)
        .await?;
    let lineorder_result = scan_op_lineorder
        .execute(lineorder_root, lineorder_scan_options)
        .await?;
    let scan_time = scan_start.elapsed();

    println!("\n=== SSB Q1.1 Execution ===");
    println!("Scan Results:");
    println!(
        "  Dates: {} rows scanned, {} pages read",
        dates_result.total_scanned, dates_result.pages_read
    );
    println!(
        "  Lineorder: {} rows scanned, {} pages read",
        lineorder_result.total_scanned, lineorder_result.pages_read
    );
    println!("Scan Time: {:.2}ms", scan_time.as_secs_f64() * 1000.0);

    let join_condition = vec![JoinCondition {
        left_column: "lo_orderdate".to_string(),
        right_column: "d_datekey".to_string(),
    }];

    let join_op =
        HashJoinOperation::new(lineorder_manager.clone(), JoinType::Inner, join_condition);

    let join_start = Instant::now();
    let join_result = join_op
        .execute(
            lineorder_result.rows,
            dates_result.rows,
            &lineorder_schema,
            &dates_schema,
        )
        .await?;
    let join_time = join_start.elapsed();

    println!("Join Results:");
    println!(
        "  Left rows: {}, Right rows: {}, Output rows: {}",
        join_result.left_rows_processed, join_result.right_rows_processed, join_result.output_rows
    );
    println!("Join Time: {:.2}ms", join_time.as_secs_f64() * 1000.0);

    // Calculate revenue: sum(lo_extendedprice * lo_discount/100)
    let agg_start = Instant::now();
    let revenue_sum: f64 = join_result
        .rows
        .iter()
        .filter_map(|row| {
            if let (Value::Float(extended_price), Value::Integer(discount)) =
                (&row.data[9], &row.data[11])
            {
                Some(extended_price * (*discount as f64) / 100.0)
            } else {
                None
            }
        })
        .sum();
    let agg_time = agg_start.elapsed();

    let total_time = total_start.elapsed();
    let final_memory = get_memory_usage();
    let memory_used = if final_memory > initial_memory {
        final_memory - initial_memory
    } else {
        0
    };

    println!("Aggregation Time: {:.2}ms", agg_time.as_secs_f64() * 1000.0);
    println!("\n=== Performance Metrics ===");
    println!(
        "Total Query Time: {:.2}ms",
        total_time.as_secs_f64() * 1000.0
    );
    println!("Memory Used: {} MB", memory_used / 1024 / 1024);
    println!(
        "Throughput: {:.2} rows/sec",
        join_result.output_rows as f64 / total_time.as_secs_f64()
    );
    println!("SSB Q1.1 Revenue: {:.2}", revenue_sum);

    Ok(())
}
