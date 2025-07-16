Please fix all the think i provided below, make sure it will be factual and correct, if i mention the paper make sure it
is correct that paper contain that statement. And give review like you are my professor i targeting this journal
submitted to IEEE / ACM / VLDB, give the brutal review for it. also you can introduce the better idea, or any suggestion 
that might be important

## SQLite & Duck-DB

i read paper about sqlite past, present and future, on that paper basically just  mention about sqlite and the new competitor
duck db, on that paper it showed that duck db perform better on SSB benchmark query (OLAP), because, duck-db using 
columnar based storage instead of row-based, that advantage might perform better on aggregation or any analytical workload,
not like sqlit, using a b-tree, so dont have advantage like that. 

Otherwise on TATP benchmark sqlite perform way better than duck-db, why? because sqlite using b-tree and it is optimized
for OLTP back-then as what sqlite purposed. based on that paper sqlite, sqlite seems keeping them self as OLTP rather being a
general-purpose one.

## The idea

So, basically the idea is i want to build a thing that fit with OLTP, yes it is still row-based but also can be good at 
OLAP. So, b+ tree is the candidate, b+ tree just a variance of b tree that add sibling pointer on the leaf page / leaf node, 
so i think the OLTP  performance will not degrade so much, also we keep to use the row based one. But take look at what OLAP 
really means, maybe in my mind it can be divided on two section, first is the usage of columnar, and second is how they 
operate the data, the  parallel scan, the vectorized query, the optimization of read data from disk, that's it all. So, b+ 
tree might be fit becauase it had sibling pointer, so might we can parallel, vectorized, etc..

## The journey

So, i start to building that storage engine, so basically the idea to parallel is, we collecting all leaf_page_id
on a b+ tree, then store it on an array, then spawn N-worker given designated leaf_page_id the worker need to read, 
so basically the pseudo-code will be like this  : 

```
-- sequential scanning

let leaf_page_id = get_leafmost_leaf_page_id();

while leaf_page_id != null {
    let page = read_page(leaf_page_id);
    // do something with page
    leaf_page_id = page.next_leaf_page_id;
}
```

```
-- parallel sequential scanning

fn collect_all_leaf_page_id() -> vec<u64> {
    let result = vec::new();
    let leaf_page_id = get_leafmost_leaf_page_id();

    result.push(leaf_page_id);

    while leaf_page_id != null {
        let page = read_page(leaf_page_id);
        result.push(page.next_leaf_page_id)
    }
}

let payloads = collect_all_leaf_page_id();

// divide payloads, and scan worker
```

as we can see, this techniques very un-optimal because it need to traverse all leaf_page just to get the id, then just 
spawn worker, and so on. so how we can parallel that read process, if we still depending on collecting all leaf_page_id
'sequential-ly', so the idea is to introduce leaf page registry, basically it's a dedicated file / space that store all
leaf_page_id, it's look better, but not yet even better than the sequential one, why?

## The parallel problem

The problem with parallel read is when we need to collect all result from N-worker, yes, sync overhead, but we already solve
that  problem by introducing non-blocking result collecting, also some techniques such-as read-ahead, and buffer pool 
improvement playing a role, here the result : 

-- Unoptimized (not yet using read-ahead & non blocking result collecting)
📊 Testing with 10000 rows
  📈 Results:
    Rows returned: 10000
    Pages read (sequential): 5000
    Pages read (parallel): 5003
    Sequential time: 435.58ms
    Parallel time: 420.92ms
    🚀 Speedup: 1.03x (3.4% faster)
    Sequential throughput: 22957.73 rows/sec
    Parallel throughput: 23757.32 rows/sec

📊 Testing with 50000 rows
  📈 Results:
    Rows returned: 50000
    Pages read (sequential): 25000
    Pages read (parallel): 25003
    Sequential time: 2129.78ms
    Parallel time: 2529.67ms
    ⚠️  Slowdown: 1.19x (18.8% slower)
    Sequential throughput: 23476.63 rows/sec
    Parallel throughput: 19765.41 rows/sec

📊 Testing with 100000 rows
  📈 Results:
    Rows returned: 100000
    Pages read (sequential): 49999
    Pages read (parallel): 49999
    Sequential time: 3531.81ms
    Parallel time: 4999.61ms
    ⚠️  Slowdown: 1.42x (41.6% slower)
    Sequential throughput: 28314.09 rows/sec
    Parallel throughput: 20001.57 rows/sec

-- Optimized (using read-ahead and prefetch techniques)
📊 Testing with 10000 rows
✅ all_leaf_page_ids 0.30ms -> read from leaf page registry
✅ join_set.spawn 0.09ms -> spawning worker
✅ collect_result 31.71ms -> collecting result
  📈 Results:
    Rows returned: 10000
    Pages read (sequential): 5000
    Pages read (parallel): 5003
    Sequential time: 124.38ms
    Parallel time: 74.32ms
    🚀 Speedup: 1.67x (40.3% faster)
    Sequential throughput: 80400.33 rows/sec
    Parallel throughput: 134562.15 rows/sec

📊 Testing with 50000 rows
✅ all_leaf_page_ids 0.21ms
✅ join_set.spawn 0.14ms
✅ collect_result 134.31ms
  📈 Results:
    Rows returned: 50000
    Pages read (sequential): 25000
    Pages read (parallel): 25003
    Sequential time: 849.32ms
    Parallel time: 135.23ms
    🚀 Speedup: 6.28x (84.1% faster)
    Sequential throughput: 58870.56 rows/sec
    Parallel throughput: 369733.61 rows/sec

📊 Testing with 100000 rows
✅ all_leaf_page_ids 0.22ms
✅ join_set.spawn 0.12ms
✅ collect_result 256.28ms
  📈 Results:
    Rows returned: 100000
    Pages read (sequential): 49999
    Pages read (parallel): 49999
    Sequential time: 1721.70ms
    Parallel time: 257.39ms
    🚀 Speedup: 6.69x (85.1% faster)
    Sequential throughput: 58082.14 rows/sec
    Parallel throughput: 388516.24 rows/sec