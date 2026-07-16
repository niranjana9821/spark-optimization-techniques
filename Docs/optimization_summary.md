# Optimization Summary

## Decision Guide — Which Technique to Apply When

| Symptom                                        | Likely Cause                                       | Technique               |
| ---------------------------------------------- | -------------------------------------------------- | ----------------------- |
| Join stage is slow                             | Full shuffle on both sides                         | Broadcast Join          |
| Filter reads more data than expected           | Function wrapping column prevents pushdown         | Predicate Pushdown      |
| Reading all columns when only a few needed     | SELECT \* or unused columns in lineage             | Column Pruning (auto)   |
| Full dataset scanned despite date filter       | Filtering on derived expression, not partition col | Partition Pruning       |
| One task in stage takes 10x longer than others | Data skew on a high-frequency key                  | Salting                 |
| Many small output files                        | Too many partitions at write time                  | Coalesce before write   |
| Skewed data before join                        | Hot key in join column                             | AQE skew join / Salting |
| Same DataFrame computed multiple times         | No caching between actions                         | Cache / Persist         |
| Custom Python logic slowing down pipeline      | Python UDF with row-by-row serialization           | Pandas UDF or Built-in  |

---

## Reading explain() — Quick Reference

| Node                                 | Meaning                     | Good or Bad                            |
| ------------------------------------ | --------------------------- | -------------------------------------- |
| `Exchange hashpartitioning(col, N)`  | Shuffle by column           | Necessary evil — reduce data before it |
| `Exchange RoundRobinPartitioning(N)` | Explicit repartition()      | Only if needed                         |
| `BroadcastHashJoin`                  | Small table broadcast       | No shuffle on small side               |
| `SortMergeJoin`                      | Both sides shuffled         | Use when neither side fits in memory   |
| `Coalesce N`                         | Partition merge, no shuffle | Good for output file reduction         |
| `PartitionFilters: [col = val]`      | Partition pruning active    | Directories skipped                    |
| `PartitionFilters: []`               | No pruning                  | Check filter column                    |
| `PushedFilters: [...]`               | Predicate pushdown active   | Row groups skipped                     |
| `DictionaryFilters: [...]`           | Dictionary-level skip       | Best case for equality filters         |
| `ReadSchema: struct<c1,c2>`          | Column pruning active       | Unused columns not read                |
| `pythonUDF`                          | Python UDF in plan          | Replace with built-in if possible      |
| `AdaptiveSparkPlan`                  | AQE enabled                 | Plan may change at runtime             |

---

## AQE — What It Handles and What It Doesn't

| Scenario                     | AQE Handles?             | Manual Fix Needed           |
| ---------------------------- | ------------------------ | --------------------------- |
| Join skew                    | Auto (Spark 3.x)         | None if AQE enabled         |
| GroupBy skew                 | No                       | Salting                     |
| Too many shuffle partitions  | Auto coalesces           | None                        |
| Broadcast threshold exceeded | Can switch join strategy | None                        |
| Predicate pushdown           | Not AQE's job            | Filter on raw columns       |
| Partition pruning            | Not AQE's job            | Filter on partition columns |

---

## Environment Note

All timings in this repo were collected on Google Colab (single-node `local[*]`).
Techniques that depend on inter-executor parallelism (data skew salting,
repartition-by-column) show their full benefit on multi-node clusters.
The correctness of each optimization is verified through `explain()` physical plans,
not solely through wall-clock timing.
