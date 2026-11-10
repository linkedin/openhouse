# Independent database metadata

Database properties live as one JSON document in `catalog_databases`, alongside the database's
immutable UUID, canonical lowercase name, and version. Every metadata read needs the
whole map, so a single row matches the access pattern. A sparse update rewrites the document;
its storage cost grows with the map size.

## Storage

The document uses a JPA converter and a `LONGTEXT` column, with the same representation in
MySQL and H2. JSON preserves property spelling independently of database collation.
The lowercase name has a unique index; the UUID is the primary key. Requests accept mixed
case, and metadata responses return the lowercase name. Original capitalization is not stored.

Sparse updates read the row, preserve unspecified keys, assign a replacement map, and commit
within a transaction. Optimistic version checks reject competing updates and stale deletes,
including empty updates. A failed transaction rolls back the whole document.
The HTTP handler translates wire objects to service models and checked failures to HTTP errors.
Deleting database metadata leaves table rows and files untouched.

Create, update, and removal keys must contain well-formed Unicode and at most 512 UTF-16 code
units. Empty keys, spaces, accents, case differences, and supplementary characters round-trip.
Unpaired surrogates and overlength keys return HTTP 400 before persistence.

## Local MySQL validation

The service tests exercise persistence through Spring and Hibernate on H2 and local MySQL.
They cover exact strings, UUID stability, sparse updates, optimistic conflicts and rollback,
empty maps, and table independence. HTTP tests cover request validation and error responses.

Current-schema MySQL plans use `uq_database_name` for normalized-name reads and `PRIMARY`
for UUID reads and versioned updates/deletes. The read plans return one row.
Write plans were checked with `EXPLAIN` only; those statements were not executed.

Use an isolated MySQL 8.0 schema containing synthetic data only. Apply the
[baseline DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0000__baseline.sql),
[entity-type DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0001__add_entity_type_to_user_table_row.sql), and
[database DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_database_metadata.sql)
to a new schema before running either task. Both tasks disable automatic schema creation and
Spring SQL initialization. The database DDL describes this undeployed feature's initial schema,
not a migration for an existing deployment.

```bash
./gradlew :services:housetables:databaseMysqlTest \
  -PdatabaseMysqlUrl='jdbc:mysql://127.0.0.1:33316/revision_document_service?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The task uses a local root account with an empty password and cleans its test records.
It is unsuitable for shared environments.

Run the workload task against a fresh schema:

```bash
./gradlew :services:housetables:databaseMysqlWorkload \
  -PdatabaseMysqlUrl='jdbc:mysql://127.0.0.1:33316/revision_document_workload?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The [workload harness](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/src/test/java/com/linkedin/openhouse/housetables/e2e/database/DatabaseMysqlWorkloadTest.java)
retains its synthetic fixture and captures generated SQL, successful operations, conflicts,
statement counts, and timing samples. It exercises maps of 10, 100, and 1,000 properties and
table/view occupancy queries. A one-row occupancy response can still require filtering and
counting every entity in that namespace.

### Occupancy regression comparison

The workload prints `OCCUPANCY_P95` for one and four readers. Each value is the
median of three warmed runs' p95 latencies, in milliseconds; each run makes 100
table-then-view occupancy checks. The fixture has 10,000 views in the requested
namespace and 20,000 in another namespace. Capture a baseline with
`databaseMysqlWorkload`, then compare a candidate with
`databaseMysqlOccupancyRegression` on a fresh schema using the same DDL, fixture,
MySQL version, hardware, and concurrency. Keep competing workloads off that host.

The regression task requires both measured baseline values and an explicit
maximum regression percentage. It fails if either reader configuration exceeds
`baseline * (1 + percentage / 100)`, even when the SQL statement count is unchanged.
The ordinary workload task only records measurements.

A local MySQL 8.0 baseline measured 64.583 ms with one reader and 88.952 ms with
four readers. For this local smoke check, a 50% allowance catches large slowdowns
while allowing host scheduling variation. This is a local regression criterion,
not a production latency target; recapture the baseline and choose the allowance
for the environment being evaluated.

```bash
./gradlew :services:housetables:databaseMysqlOccupancyRegression \
  -PdatabaseMysqlUrl='jdbc:mysql://127.0.0.1:33316/occupancy_candidate?useSSL=false&allowPublicKeyRetrieval=true' \
  -PdatabaseOccupancyBaselineSingleP95Millis=64.583 \
  -PdatabaseOccupancyBaselineConcurrentP95Millis=88.952 \
  -PdatabaseOccupancyMaxRegressionPercent=50 \
  -x CopyGitHooksTask
```

Historical measurements of the child-table implementation do not validate this document schema.
Those sparse-update timing comparisons were inconclusive because the baseline varied.
The current checks establish local correctness and query access paths, not a performance
improvement or production capacity.
