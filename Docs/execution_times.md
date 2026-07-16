# Execution Times

All benchmarks run on Google Colab (single-node local Spark, `local[*]`).
Dataset: 1M orders, 100K customers, 5K products.

> **Note:** Single-node timings reflect I/O and serialization costs, not distributed
> parallelism. Techniques like data skew salting and repartition-by-column show their
> full benefit on multi-node clusters.

---

## Results Summary

| # | Technique | Baseline | Optimized | Improvement |
|---|-----------|----------|-----------|-------------|
| 1 | Broadcast Join | 3.28s | 1.85s | **↓ 44%** |
| 2 | Predicate Pushdown | 0.55s | 1.02s | Plan-level only* |
| 3 | Column Pruning | — | — | Catalyst auto |
| 4 | Partition Pruning | 1.24s | 0.69s | **↓ 44%** |
| 5 | Data Skew (Salting) | 1.71s | 5.24s | Plan-level only* |
| 6 | Repartition vs Coalesce | 0.58s | 0.57s (coalesce) | No extra shuffle |
| 7 | Cache vs Persist | ~5.4s (3 actions) | ~1.6s (3 actions) | **↓ 70%** |
| 8 | UDF vs Built-in | 11.18s | 1.96s | **↓ 82%** |

---

## Notes on Specific Techniques

### Predicate Pushdown (*)
The baseline (`abs(customer_id)==100`) ran faster (0.55s) than the optimized
(`customer_id==100`, 1.02s) due to the specific nature of equality filtering on a
small result set. The key difference is in the execution plan:
- **Baseline:** `PushedFilters: []`, `DictionaryFilters: []`
- **Optimized:** `PushedFilters: [IsNotNull, EqualTo]`, `DictionaryFilters: [(customer_id=100)]`

Predicate pushdown reduces I/O at the Parquet reader level. On larger result sets
or repeated scans the timing benefit becomes visible.

### Data Skew / Salting (*)
Salting appears slower on Colab (5.24s vs 1.71s) because Colab is single-node —
all partitions run sequentially regardless of distribution. The benefit is
**parallelism across executors** which only applies on multi-node clusters.

Proof of correctness is in the key distribution:
- Before salting: customer_id 1,2,3 each hold ~200,000 rows
- After salting: each hot key splits into 10 sub-keys of ~20,000 rows each

### Column Pruning
Handled automatically by Catalyst — no explicit code needed.
Physical plan shows `ReadSchema: struct<product_id:int, total_amount:int>`
instead of all 9 columns, confirming only required columns are read from Parquet.
