# Independent database metadata

Database metadata has its own identity and lifecycle. The HTTP handler translates request and
response objects to the HouseTables service models. Existing table operations remain independent
of these records.

## Storage

The property table is a JPA collection owned by the versioned database row. The foreign key
prevents orphan properties, and service transactions commit the owner and collection together.
Optimistic version checks reject competing updates and stale deletes. A failed transaction
rolls back property removals as well as additions.

Property-key queries are not a requirement. A single serialized property document is a viable
alternative that could reduce collection round trips, while still requiring transactional
read-modify-write and version checks. This implementation retains the relational collection
and additive DDL. It does not claim that property-level queries require this layout.

Keys are case-sensitive UTF-8 bytes. Empty keys, spaces, accents, and supplementary Unicode
characters round-trip. Create, update, and removal requests reject unpaired UTF-16 surrogates
through bean validation before persistence.

## Local MySQL validation

Use an isolated MySQL 8.0 schema containing synthetic data only. Apply
`ddl/0000__baseline.sql`, `ddl/0001__add_entity_type_to_user_table_row.sql`, and
`ddl/0002__add_database_metadata.sql` unchanged before running either task. Both tasks disable
automatic schema creation and Spring SQL initialization.

```bash
./gradlew :services:housetables:databaseMysqlTest \
  -PdatabaseMysqlUrl='jdbc:mysql://127.0.0.1:33316/feedback?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The service suite covers exact keys, normalized uniqueness, sparse updates, competing writers,
stale deletion, storage-failure rollback, and table independence. The tests use the dedicated
local root account with an empty password, so this task is unsuitable for shared environments.

Run the workload task against a fresh schema:

```bash
./gradlew :services:housetables:databaseMysqlWorkload \
  -PdatabaseMysqlUrl='jdbc:mysql://127.0.0.1:33316/feedback_workload?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The workload retains its synthetic fixture. It measures 10, 100, and 1,000 properties with one
and four writers, plus generated occupancy queries over 10,000 views and 20,000 unrelated
entities. Each configuration warms up once, then records three runs of 100 operations.
`WORKLOAD` rows contain workload name, concurrency, run, successes, conflicts, statement count,
median milliseconds, p95 milliseconds, attempts per second, and successful operations per
second. `SQL_CAPTURE` rows contain the actual Hibernate SQL, including pagination and counts.

Compare identical baseline and candidate configurations. The declared local investigation
criteria allow at most 25% higher median-of-run p95 latency and require at least 80% of baseline
throughput. Unstable baselines make a timing comparison inconclusive. These are local
investigation criteria, not production SLOs. Statement counts and persistence assertions remain
deterministic checks. Occupancy is a policy check whose filtering and count work scales with
namespace size; returning one row does not limit all database work to one row.

## Local measurements on November 6, 2026

The original implementation and corrective changes used the same harness, MySQL 8.0.45
container, production-recorded DDL, fixtures, and connection settings. The following
median-of-three-run values come from a sequential pair without concurrent Gradle builds.
Throughput counts attempted operations, including checked concurrency conflicts; the raw
workload output also reports successful throughput.

| Workload | Concurrency | Baseline p95 ms | Candidate p95 ms | Baseline attempts/s | Candidate attempts/s |
|---|---:|---:|---:|---:|---:|
| 10 properties | 1 | 11.84 | 7.17 | 136.81 | 182.12 |
| 10 properties | 4 | 7.78 | 8.28 | 653.49 | 611.51 |
| 100 properties | 1 | 9.16 | 6.64 | 181.75 | 200.29 |
| 100 properties | 4 | 7.40 | 8.60 | 670.41 | 655.87 |
| 1,000 properties | 1 | 14.15 | 7.60 | 118.37 | 169.70 |
| 1,000 properties | 4 | 9.84 | 11.42 | 526.06 | 481.88 |
| 10,000 views | 1 | 63.17 | 61.35 | 17.21 | 17.14 |
| 10,000 views | 4 | 84.47 | 83.17 | 51.20 | 51.33 |

All numerical median comparisons meet the declared local criteria. Sparse-update timing remains
inconclusive because some baseline p95 runs varied by up to 81%, which prevents attributing
the apparent improvements to the changes. Deterministic sparse-update checks passed: omitted
properties survive, successful uncontended updates use five statements, and competing writers
produce checked conflicts rather than lost updates.

The quiet occupancy comparison meets both timing criteria with baseline p95 variation below
5%. An earlier pair overlapped other builds and exceeded the four-reader p95 criterion by
reporting a 28.97% increase; it is retained as a failed initial measurement, not counted as a
pass. Both pairs used three statements per views-only check. Captured generated SQL and
`EXPLAIN ANALYZE` show indexed access through `idx_user_table_upper_db_table`, with 10,000
target-namespace candidates visited by each page/count query and no cross-database scan.
These measurements establish local application behavior, not production capacity.
