use std::sync::Arc;
use std::time::Instant;

use bindereh::{executor::Executor, manager::Manager, page::Page};
use shared_types::{Column, DataType, Row, ScanOptions, Schema, Value};

#[tokio::main]
async fn main() {
    println!("🚀 Starting Parallel vs Sequential Scan Benchmark...");

    let manager = Arc::new(Manager::new("parallel_benchmark.db", 1024).unwrap());
    let schema = Schema::new(vec![Column {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        primary_key: true,
    }]);

    // Create initial root node
    let mut current_root_page_id = manager.allocate_page().await;
    println!("Initial root page ID: {}", current_root_page_id);

    let root_node = Page {
        page_id: current_root_page_id,
        is_leaf: true,
        parent_page_id: None,
        keys: vec![],
        values: vec![],
        child_page_ids: vec![],
        next_leaf_page_id: None,
        is_dirty: true,
    };

    manager.write_page(&root_node).await.unwrap();

    // Register the initial leaf page in the registry
    manager
        .register_leaf_page(current_root_page_id)
        .await
        .unwrap();

    let executor = Executor::new(manager.clone(), current_root_page_id, 2); // 4 workers for parallel

    // Insert test data
    println!("\n📝 Inserting test data...");
    let total_insertions = 50_000; // Larger dataset for better parallel performance comparison
    let start_insert = Instant::now();

    for i in 1..=total_insertions {
        let row = Row {
            id: i,
            data: vec![Value::Integer(i as i64)],
        };

        let new_root_page_id = executor.insert(row).await.unwrap();
        if new_root_page_id != current_root_page_id {
            current_root_page_id = new_root_page_id;
        }

        if i % 10000 == 0 {
            println!("  Inserted {} rows", i);
        }
    }

    let insert_duration = start_insert.elapsed();
    println!(
        "✅ Insertion completed in {:.2}ms",
        insert_duration.as_secs_f64() * 1000.0
    );

    // Test scenarios
    let test_scenarios = vec![("1001 rows", 1001), ("Full table scan", total_insertions)];

    println!("\n🔍 Starting scan performance comparison...");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    SCAN PERFORMANCE COMPARISON               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    for (scenario_name, limit) in test_scenarios {
        println!("\n📊 Testing: {}", scenario_name);

        // Sequential scan
        let sequential_options = ScanOptions {
            limit: if limit == total_insertions {
                None
            } else {
                Some(limit as usize)
            },
            schema: Some(schema.clone()),
            projection: Some(vec!["id".to_string()]),
            parallel: false,
            ..Default::default()
        };

        let start_sequential = Instant::now();
        let sequential_result = executor.scan(sequential_options).await.unwrap();
        let sequential_duration = start_sequential.elapsed();

        // Parallel scan
        let parallel_options = ScanOptions {
            limit: if limit == total_insertions {
                None
            } else {
                Some(limit as usize)
            },
            schema: Some(schema.clone()),
            projection: Some(vec!["name".to_string(), "id".to_string()]),
            parallel: true,
            ..Default::default()
        };

        let start_parallel = Instant::now();
        let parallel_result = executor.scan(parallel_options).await.unwrap();
        let parallel_duration = start_parallel.elapsed();

        // Results validation
        assert_eq!(
            sequential_result.rows.len(),
            parallel_result.rows.len(),
            "Row count mismatch between sequential and parallel scans"
        );

        // Performance comparison
        let sequential_ms = sequential_duration.as_secs_f64() * 1000.0;
        let parallel_ms = parallel_duration.as_secs_f64() * 1000.0;
        let speedup = sequential_ms / parallel_ms;
        let improvement = ((sequential_ms - parallel_ms) / sequential_ms) * 100.0;

        println!("  📈 Results:");
        println!("    Rows returned: {}", sequential_result.rows.len());
        println!(
            "    Pages read (sequential): {}",
            sequential_result.pages_read
        );
        println!("    Pages read (parallel): {}", parallel_result.pages_read);
        println!("    Sequential time: {:.2}ms", sequential_ms);
        println!("    Parallel time: {:.2}ms", parallel_ms);

        if speedup > 1.0 {
            println!(
                "    🚀 Speedup: {:.2}x ({:.1}% faster)",
                speedup, improvement
            );
        } else {
            println!(
                "    ⚠️  Slowdown: {:.2}x ({:.1}% slower)",
                1.0 / speedup,
                -improvement
            );
        }

        // Throughput comparison
        let sequential_throughput =
            sequential_result.rows.len() as f64 / sequential_duration.as_secs_f64();
        let parallel_throughput =
            parallel_result.rows.len() as f64 / parallel_duration.as_secs_f64();

        println!(
            "    Sequential throughput: {:.2} rows/sec",
            sequential_throughput
        );
        println!(
            "    Parallel throughput: {:.2} rows/sec",
            parallel_throughput
        );
    }

    // Summary
    println!("\n🎯 BENCHMARK SUMMARY");
    println!("Database File: parallel_benchmark.db");
    println!("Total Records: {}", total_insertions);
    println!("Page Size: 128 bytes");
    println!("Worker Threads: 4");

    // println!("\n💡 Key Insights:");
    // println!("  • Parallel scanning shows benefits for larger datasets");
    // println!("  • Worker pool distributes page reads across multiple tasks");
    // println!("  • Results are identical between sequential and parallel modes");
    // println!("  • Predicate evaluation logic remains unchanged");

    println!("\n🏁 Parallel benchmark completed successfully!");
}
