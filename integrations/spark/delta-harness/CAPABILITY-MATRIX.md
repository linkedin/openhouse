# Delta harness capability matrix

This matrix summarizes only behavior present in the executable catalog in the
current checkout. It is an inventory of tested product contracts, not a roadmap.
See [TEST-COVERAGE.md](TEST-COVERAGE.md) for the assertions behind each row.

## Product behavior

| Capability | Cases | Formats | Observable contract |
|------------|------:|---------|---------------------|
| Scalar values | 10 | Parquet, ORC | Scalar values, nulls, special floating-point values, numeric boundaries, Unicode, and empty strings read back with their intended semantics. |
| Reads and mutations | 12 | Parquet, ORC | Projection, append, overwrite, predicate delete, predicate update, and merge upsert produce exact rows and snapshot counts. |
| Rejected DML | 12 | Parquet, ORC | Invalid delete, update, insert, and merge statements fail with the expected diagnostics; the rejected cardinality-violation merge also preserves table state. |
| **Complete catalog** | **34** | **Parquet, ORC** | **34 passed, 0 skipped, 0 failed against embedded OpenHouse.** |

## Framework checks

Thirteen Spark-free JUnit tests cover the execution machinery that makes catalog
results repeatable.

| Area | Behavior checked |
|------|------------------|
| Catalog identity | Contribution order and the exact 34 case IDs are stable, and every ID is unique. |
| Runtime configuration | Data sources are late-bound, and configured parallelism accepts only positive integers. |
| Retry boundary | Only transient failures during Spark-session creation are retried. Failures after a case starts are terminal. |
| Failure classification | Transient connection failures are narrowly classified, and cause traversal terminates for cyclic chains. |
| Table lifecycle | Cleanup runs only for owned tables, and body failures retain precedence over cleanup failures. |
| Table identity | Concurrent generation and counter resets do not produce duplicate table names. |
| Seed generation | Core seed SQL and date rollover are deterministic. |
| Published artifact | Gradle `check` requires portable `Plan` and `Runner` classes and rejects embedded launcher and server classes. |
