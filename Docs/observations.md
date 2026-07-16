# Observations — Physical Plan Analysis

This document records what to look for in `explain(True)` output for each technique.

---

## 1. Broadcast Join

**Baseline plan signal:**

```
SortMergeJoin [product_id], [product_id], Inner
:- Exchange hashpartitioning(product_id, 200)   ← shuffle on orders
+- Exchange hashpartitioning(product_id, 200)   ← shuffle on products
```

**Optimized plan signal:**

```
BroadcastHashJoin [product_id], [product_id], Inner, BuildRight
+- BroadcastExchange HashedRelationBroadcastMode   ← products broadcast, no shuffle
```

**Why it's faster:** Two network shuffles eliminated. Products (5K rows) sent to
every executor in memory instead of both tables being redistributed by key.

---

## 2. Predicate Pushdown

**Baseline plan signal:**

```
FileScan parquet
  PushedFilters: []
  DictionaryFilters: []
```

**Optimized plan signal:**

```
FileScan parquet
  PushedFilters: [IsNotNull(customer_id), EqualTo(customer_id,100)]
  DictionaryFilters: [(customer_id = 100)]
```

**Why it matters:** Parquet stores column data using dictionary encoding with
min/max statistics per row group. When the filter is a direct equality on the raw
column, the Parquet reader skips entire row groups before data enters Spark.
Wrapping the column in `abs()` prevents this — Spark must read all data first.

---

## 3. Column Pruning

**Plan signal:**

```
FileScan parquet
  ReadSchema: struct<product_id:int,total_amount:int>
```

Only 2 of 9 columns appear in ReadSchema. Catalyst automatically determined
which columns are needed and pruned the rest at the file reader level.

---

## 4. Partition Pruning

**Baseline plan signal:**

```
FileScan parquet
  PartitionFilters: []   ← no pruning, all partitions scanned
  DataFilters: [year(order_date) = 2025]
```

**Optimized plan signal:**

```
FileScan parquet
  PartitionFilters: [isnotnull(order_year), (order_year = 2025)]   ← pruning active
```

**Why it's faster:** The orders dataset is stored as `order_year=2023/`,
`order_year=2024/`, `order_year=2025/` directories. Filtering on `order_year`
directly tells Spark to open only the matching directory — all other year
directories are never opened. Filtering on `year(order_date)` requires Spark
to open all directories and evaluate the function on every row.

---

## 5. Data Skew + Salting

**Key distribution (proof of skew):**

```
customer_id=1 → 199,861 rows
customer_id=2 → 200,049 rows
customer_id=3 → 199,812 rows
customer_id=47379 → 14 rows
```

3 keys hold 60% of 1M rows. During `Exchange hashpartitioning(customer_id)`,
all rows with the same key route to the same partition → one partition 20,000x
larger than others.

**After salting — key distribution:**

```
1_0 → ~20,000 rows    1_1 → ~20,000 rows    ...    1_9 → ~20,000 rows
2_0 → ~20,000 rows    ...
```

Hot key rows spread evenly across 10 partitions per original key.

**AQE limitation:** AQE skew join handler only activates during joins, not
`groupBy`. For groupBy skew, salting is the only solution.

---

## 6. Repartition vs Coalesce

**Repartition plan signal:**

```
Exchange RoundRobinPartitioning(20), REPARTITION_BY_NUM   ← full shuffle
```

**Coalesce plan signal:**

```
Coalesce 2   ← no Exchange node, no shuffle
```

**Repartition by Column — ENSURE_REQUIREMENTS vs REPARTITION_BY_NUM:**
Both baseline and explicit repartition-by-column show identical Exchange nodes —
the only difference is the tag. `ENSURE_REQUIREMENTS` = Spark chose the shuffle.
`REPARTITION_BY_NUM` = explicit repartition was executed. Real benefit of
repartition-by-column appears in multi-join pipelines where same pre-partitioned
DataFrame is reused with `cache()`.

---

## 7. Cache vs Persist

Without caching, 3 actions on the same DataFrame = 3 full Parquet scans.
With `cache()` / `persist()`, materialisation happens once on first action,
all subsequent actions read from memory.

**Key difference — storage levels:**
| Level | RAM | Disk | Use When |
|---|---|---|---|
| MEMORY_ONLY | YES | NO | Small DFs |
| MEMORY_AND_DISK | YES | YES | Default (same as cache()) |
| DISK_ONLY | NO | YES | Too large for RAM |

Always call `unpersist()` after use to free executor memory.

---

## 8. UDF vs Built-in Function

**UDF plan signal:**

```
Project [pythonUDF0#... AS category]   ← black box, Catalyst cannot optimize
```

**Built-in plan signal:**

```
Project [CASE WHEN (total_amount < 1000) THEN Low ...  AS category]
```

**Why 82% faster:** Python UDFs serialize each row from JVM → Python process
via Pickle, execute the function, deserialize back — row by row. Built-in
`when()/otherwise()` executes entirely in the JVM with whole-stage code generation.
Zero serialization overhead.
