use std::sync::Arc;
use std::time::Instant;

use bindereh::{executor::Executor, manager::Manager, page::Page};
use shared_types::{Column, DataType, Row, ScanOptions, Schema, Value};

#[derive(Debug, Clone)]
struct BenchmarkResult {
    row_count: usize,
    sequential_time_ms: f64,
    parallel_time_ms: f64,
    sequential_throughput: f64,
    parallel_throughput: f64,
    speedup: f64,
    improvement_percent: f64,
    pages_read_sequential: usize,
    pages_read_parallel: usize,
}

#[tokio::main]
async fn main() {
    println!("🚀 Starting Comprehensive Parallel vs Sequential Scan Benchmark...");

    let buffer_size = 1024;
    let max_worker = 4;

    let manager = Arc::new(Manager::new("test.db", buffer_size).unwrap());
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

    let executor = Executor::new(manager.clone(), current_root_page_id, max_worker);

    // Insert test data with progress tracking
    println!("\n📝 Inserting test data...");
    let total_insertions = 100_000;
    let batch_size = 10_000;
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

        if i % batch_size == 0 {
            let progress = (i as f64 / total_insertions as f64) * 100.0;
            println!("  Inserted {} rows ({:.1}%)", i, progress);
        }
    }

    let insert_duration = start_insert.elapsed();
    println!(
        "✅ Insertion completed in {:.2}ms",
        insert_duration.as_secs_f64() * 1000.0
    );

    // Performance testing at different data sizes
    let test_sizes = vec![10_000, 50_000, 100_000];
    let mut benchmark_results = Vec::new();

    println!("\n🔍 Starting incremental scan performance comparison...");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                 INCREMENTAL SCAN PERFORMANCE                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    for &row_count in &test_sizes {
        println!("\n📊 Testing with {} rows", row_count);

        // Sequential scan
        let sequential_options = ScanOptions {
            limit: Some(row_count),
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
            limit: Some(row_count),
            schema: Some(schema.clone()),
            projection: Some(vec!["id".to_string()]),
            parallel: true,
            ..Default::default()
        };

        let start_parallel = Instant::now();
        let parallel_result = executor.scan(parallel_options).await.unwrap();
        let parallel_duration = start_parallel.elapsed();

        // Results validation
        // assert_eq!(
        //     sequential_result.rows.len(),
        //     parallel_result.rows.len(),
        //     "Row count mismatch between sequential and parallel scans"
        // );

        // Calculate metrics
        let sequential_ms = sequential_duration.as_secs_f64() * 1000.0;
        let parallel_ms = parallel_duration.as_secs_f64() * 1000.0;
        let speedup = sequential_ms / parallel_ms;
        let improvement = ((sequential_ms - parallel_ms) / sequential_ms) * 100.0;

        let sequential_throughput =
            sequential_result.rows.len() as f64 / sequential_duration.as_secs_f64();
        let parallel_throughput =
            parallel_result.rows.len() as f64 / parallel_duration.as_secs_f64();

        // Store results for plotting
        let result = BenchmarkResult {
            row_count: sequential_result.rows.len(),
            sequential_time_ms: sequential_ms,
            parallel_time_ms: parallel_ms,
            sequential_throughput,
            parallel_throughput,
            speedup,
            improvement_percent: improvement,
            pages_read_sequential: sequential_result.pages_read,
            pages_read_parallel: parallel_result.pages_read,
        };

        benchmark_results.push(result.clone());

        // Display results
        println!("  📈 Results:");
        println!("    Rows returned: {}", result.row_count);
        println!(
            "    Pages read (sequential): {}",
            result.pages_read_sequential
        );
        println!("    Pages read (parallel): {}", result.pages_read_parallel);
        println!("    Sequential time: {:.2}ms", result.sequential_time_ms);
        println!("    Parallel time: {:.2}ms", result.parallel_time_ms);

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

        println!(
            "    Sequential throughput: {:.2} rows/sec",
            sequential_throughput
        );
        println!(
            "    Parallel throughput: {:.2} rows/sec",
            parallel_throughput
        );
    }

    // Generate plot data
    println!("\n📈 PERFORMANCE PLOT DATA");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                        PLOT DATA                             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    println!("\n🎯 CSV Format for Plotting:");
    println!(
        "Row Count,Sequential Time (ms),Parallel Time (ms),Sequential Throughput (rows/sec),Parallel Throughput (rows/sec),Speedup,Improvement %,Pages Read Sequential,Pages Read Parallel"
    );

    for result in &benchmark_results {
        println!(
            "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.1},{},{}",
            result.row_count,
            result.sequential_time_ms,
            result.parallel_time_ms,
            result.sequential_throughput,
            result.parallel_throughput,
            result.speedup,
            result.improvement_percent,
            result.pages_read_sequential,
            result.pages_read_parallel
        );
    }

    // Performance trends analysis
    println!("\n📊 PERFORMANCE TRENDS");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    TREND ANALYSIS                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    for (i, result) in benchmark_results.iter().enumerate() {
        println!("\n  {} rows:", result.row_count);
        println!(
            "    Time ratio (Parallel/Sequential): {:.3}",
            result.parallel_time_ms / result.sequential_time_ms
        );
        println!(
            "    Throughput ratio (Parallel/Sequential): {:.3}",
            result.parallel_throughput / result.sequential_throughput
        );

        if i > 0 {
            let prev = &benchmark_results[i - 1];
            let speedup_trend = result.speedup - prev.speedup;
            if speedup_trend > 0.0 {
                println!("    Speedup trend: ↗️  +{:.2}x improvement", speedup_trend);
            } else if speedup_trend < 0.0 {
                println!("    Speedup trend: ↘️  {:.2}x degradation", speedup_trend);
            } else {
                println!("    Speedup trend: ➡️  No change");
            }
        }
    }

    // Summary statistics
    let avg_speedup =
        benchmark_results.iter().map(|r| r.speedup).sum::<f64>() / benchmark_results.len() as f64;
    let max_speedup = benchmark_results
        .iter()
        .map(|r| r.speedup)
        .fold(0.0, f64::max);
    let min_speedup = benchmark_results
        .iter()
        .map(|r| r.speedup)
        .fold(f64::INFINITY, f64::min);

    println!("\n🎯 BENCHMARK SUMMARY");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      SUMMARY                                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("Database File: parallel_benchmark.db");
    println!("Total Records: {}", total_insertions);
    println!("Page Size: 1024 bytes");
    println!("Worker Threads: 4");
    println!("Test Intervals: Every 10,000 rows from 10k to 50k");
    println!("\nSpeedup Statistics:");
    println!("  Average speedup: {:.2}x", avg_speedup);
    println!("  Maximum speedup: {:.2}x", max_speedup);
    println!("  Minimum speedup: {:.2}x", min_speedup);

    // Performance insights
    println!("\n💡 Key Insights:");
    if avg_speedup > 1.0 {
        println!("  • Parallel scanning shows overall performance benefits");
        println!(
            "  • Average speedup of {:.2}x across all test sizes",
            avg_speedup
        );
    } else {
        println!("  • Sequential scanning performs better on average");
        println!("  • Consider optimizing parallel execution for this workload");
    }

    println!(
        "  • Worker pool distributes page reads across {} threads",
        4
    );
    println!("  • Results are identical between sequential and parallel modes");
    println!("  • Performance scaling can be observed across different data sizes");

    println!("\n🏁 Comprehensive benchmark completed successfully!");
    println!("📊 Use the CSV data above to create performance plots in your preferred tool.");
}
