# PySpark Performance Optimization

A hands-on notebook demonstrating **8 Spark optimization techniques** with physical execution plan analysis, timing comparisons, and engine-level explanations built on a synthetic 1M-row e-commerce dataset.

Built to demonstrate practical Spark knowledge beyond API familiarity.

---

## Dataset

Synthetic e-commerce data generated using `Generate_Dataset.ipynb`:

| Table | Type | Rows |
|-------|------|------|
| Customers | Dimension | 100,000 |
| Products | Dimension | 5,000 |
| Orders | Fact (partitioned by year/month) | 1,000,000 |

---

## Optimization Techniques

### 1. Broadcast Join
**Problem:** SortMergeJoin shuffles both tables across the network.  
**Fix:** Broadcast the small Products table (5K rows) to every executor.  
**Result:** ↓ 44% execution time (3.28s → 1.85s)


**Plan signal:** `SortMergeJoin` with two `Exchange` nodes → `BroadcastHashJoin` with one `BroadcastExchange`

---

### 2. Predicate Pushdown
**Problem:** Wrapping a column in a function (`abs(customer_id)`) prevents Spark from pushing the filter to the Parquet reader.  
**Fix:** Filter directly on the raw column value.  
**Result:** `DictionaryFilters` active → row groups skipped at the Parquet reader level before data enters Spark.



**Plan signal:** `PushedFilters: []` and `DictionaryFilters: []` → `PushedFilters: [IsNotNull, EqualTo]` and `DictionaryFilters: [(customer_id=100)]`

---

### 3. Column Pruning
**Problem:** Spark reads all columns from Parquet even when only 2 of 9 are needed.  
**Fix:** Catalyst Optimizer handles this automatically — no code change needed.  
**Result:** `ReadSchema` contains only `product_id` and `total_amount` instead of all 9 columns.



**Plan signal:** `ReadSchema: struct<product_id:int, total_amount:int>`

---

### 4. Partition Pruning
**Problem:** Filtering on `year(order_date)` forces a full dataset scan — Spark cannot use the partition directory structure.  
**Fix:** Filter directly on the partition column `order_year`.  
**Result:** ↓ 44% execution time (1.24s → 0.69s)



**Plan signal:** `PartitionFilters: []` → `PartitionFilters: [isnotnull(order_year), (order_year=2025)]`

---

### 5. Data Skew + Salting
**Problem:** 3 customer IDs hold 60% of 1M rows. During `hashpartitioning`, all hot-key rows land on the same partition — one executor handles 200K rows while others handle ~10.  
**Fix:** Salting — append a random number to the key, run partial aggregation, strip the salt, run final aggregation.  
**Result:** Hot keys split from 200,000 rows/partition to ~20,000 rows/partition across 10 partitions.



> **Note:** Salting shows its timing benefit on multi-node clusters. On single-node Colab, the proof is in the key distribution output, not wall-clock time.

**Key rule:** AQE handles join skew automatically but does **not** handle groupBy skew. Salting is the only solution for groupBy skew.

---

### 6. Repartition vs Coalesce
**Problem:** Using `repartition()` when reducing partitions introduces an unnecessary full shuffle.  
**Fix:** Use `coalesce()` to merge partitions locally without network transfer.


| | Repartition (Round-Robin) | Repartition (By Column) | Coalesce |
|---|---|---|---|
| Full Shuffle | Yes | Yes | No |
| Can Increase Partitions | Yes | Yes | No |
| Physical Plan | `RoundRobinPartitioning` | `hashpartitioning(col)` | `Coalesce` node |
| Use When | Rebalance skewed data | Pre-partition before multi-joins | Reduce output file count |

**Repartition by Column:** `repartition(N, col("key"))` co-locates data for downstream joins. Real benefit appears in multi-join pipelines where the same pre-partitioned DataFrame is reused with `cache()`.



---

### 7. Cache vs Persist
**Problem:** 3 actions on the same DataFrame = 3 full Parquet scans.  
**Fix:** `cache()` or `persist()` materialises the DataFrame once; all subsequent actions read from memory.  
**Result:** ↓ ~70% on repeated actions



| | cache() | persist(MEMORY_AND_DISK) |
|---|---|---|
| Storage Level | MEMORY_AND_DISK (default) | User-defined |
| Custom Level | No | Yes |
| When to Use | Quick reuse | Need explicit storage control |

Always call `unpersist()` after use to free executor memory.

---

### 8. Python UDF vs Built-in Function
**Problem:** Python UDFs serialize each row JVM → Python → JVM via Pickle — row by row.  
**Fix:** Replace with built-in `when()/otherwise()` which executes natively in the JVM with Catalyst optimization.  
**Result:** ↓ 82% execution time (11.18s → 1.96s)



**Plan signal:** `pythonUDF` node (black box, no optimization) → `CASE WHEN` inside a `Project` node

| | Python UDF | Pandas UDF | Built-in Function |
|---|---|---|---|
| Execution | Row-by-row | Arrow batch | JVM native |
| Catalyst Optimization | No | No | Yes |
| Use When | Avoid | Custom logic only | Always prefer |

---

## Results Summary

| # | Technique | Baseline | Optimized | Improvement |
|---|-----------|----------|-----------|-------------|
| 1 | Broadcast Join | 3.28s | 1.85s | ↓ 44% |
| 2 | Predicate Pushdown | 0.55s | 1.02s | Plan-level |
| 3 | Column Pruning | — | — | Catalyst auto |
| 4 | Partition Pruning | 1.24s | 0.69s | ↓ 44% |
| 5 | Data Skew (Salting) | 1.71s | — | Distribution fix |
| 6 | Repartition vs Coalesce | 2.18s | 0.57s | No extra shuffle |
| 7 | Cache vs Persist | ~5.4s | ~1.6s | ↓ 70% |
| 8 | UDF vs Built-in | 11.18s | 1.96s | ↓ 82% |



---

## How to Run

### Option 1 — Google Colab (recommended)
1. Upload `notebooks/` to Colab
2. Run `Generate_Dataset.ipynb` first to create the data, ensure the dataset is created in the desired path.
3. Run the Spark_Optimization notebook. Note - check the paths where the dataset is created for reading into a dataframe.

### Option 2 — Local
```bash
pip install -r requirements.txt
jupyter notebook notebooks/Spark_Optimization_Techniques.ipynb
```

---

## Repository Structure

```
spark-optimization-techniques/
├── README.md
├── requirements.txt
├── notebooks/
│   ├── Generate_Dataset.ipynb        # Synthetic data generation
│   └── Spark_Optimization_Techniques.ipynb  # Main optimization notebook
├── data/                             # Generated by Generate_Dataset.ipynb
│   ├── customers/
│   ├── products/
│   └── orders/
├── images/                           # Execution plan visualizations
├── docs/
│   ├── execution_times.md            # Full timing results and notes
│   ├── observations.md               # Physical plan analysis per technique
│   └── optimization_summary.md      # Decision guide and quick reference

```

---

## Key Concepts Demonstrated

- Reading and interpreting `explain(True)` physical plans
- Difference between `ENSURE_REQUIREMENTS` and `REPARTITION_BY_NUM` Exchange tags
- Why AQE handles join skew but not groupBy skew
- When salting helps and when it doesn't (single-node vs cluster)
- `coalesce()` vs `repartition()` — when the shuffle cost matters
- Python UDF serialization overhead vs JVM-native execution
- Storage level trade-offs in `cache()` vs `persist()`

---

## Tech Stack

- PySpark 3.5
- Python 3.10+
- Google Colab / Jupyter
- Parquet (columnar storage)
- Faker (synthetic data generation)
