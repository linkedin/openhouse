# Namespace metadata administration

Namespace metadata stores optional defaults for consumers that explicitly request effective
table properties. Ordinary table operations remain independent of these records. Explicit
table properties take precedence over defaults.

Administrators manage defaults through the public namespace API. Creating, updating, and
dropping metadata require SYSTEM_ADMIN; metadata reads use GET_TABLE_METADATA. The internal
House Tables API persists the metadata and is intended for service callers.

## Names and property changes

Names resolve case-insensitively and retain their insertion spelling. Creating `Finance`
and then requesting `FINANCE` returns the same identity and the name `Finance`. A duplicate
create with another spelling fails without replacing the existing record. Existing lowercase
records retain their spelling.

Updates set the supplied keys and remove the requested keys while retaining all other
properties. Property names are case-sensitive. A namespace keeps its UUID across updates;
dropping and recreating it creates a new UUID. Concurrent edits can conflict, including
an empty update. After a conflict, read the current properties and decide whether the intended
edit still applies before submitting it again.

Create, update, and removal keys must contain well-formed Unicode and at most 512 UTF-16 code
units. Empty keys, spaces, accents, case differences, and supplementary characters round-trip.
Unpaired surrogates and overlength keys return HTTP 400 before persistence.

## Deletion and recovery

Deleting namespace metadata leaves table metadata and files intact. The public API rejects
drop while a table or view occupies the namespace. Table creation and metadata deletion are
independent, so a concurrent table create can succeed even when a namespace drop succeeds.
A namespace record is not a mandatory parent of a table.

A lost response does not establish whether a mutation committed. Read the metadata and
reconcile its identity and properties with the intended change before retrying. In particular,
a repeated create can encounter an existing record and a repeated delete can encounter an
absent record after the original request succeeded.

## Local MySQL validation

The service tests exercise persistence through Spring and Hibernate on H2 and local MySQL.
They cover exact strings, UUID stability, sparse updates, optimistic conflicts and rollback,
empty maps, and table independence. HTTP tests cover request validation and error responses.

Current-schema MySQL plans use `uq_namespace_name` for case-insensitive name reads and `PRIMARY`
for UUID reads and versioned updates/deletes. The read plans return one row.
Write plans were checked with `EXPLAIN` only; those statements were not executed.

Use an isolated MySQL 8.0 schema containing synthetic data only. Apply the
[baseline DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0000__baseline.sql),
[entity-type DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0001__add_entity_type_to_user_table_row.sql), and
[namespace DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_namespace_metadata.sql)
to a new schema before running either task. Both tasks disable automatic schema creation and
Spring SQL initialization. The namespace DDL describes this undeployed feature's initial schema,
not a migration for an existing deployment.

```bash
./gradlew :services:housetables:namespaceMysqlTest \
  -PnamespaceMysqlUrl='jdbc:mysql://127.0.0.1:33316/revision_document_service?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The task uses a local root account with an empty password and cleans its test records.
It is unsuitable for shared environments.

Run the workload task against a fresh schema:

```bash
./gradlew :services:housetables:namespaceMysqlWorkload \
  -PnamespaceMysqlUrl='jdbc:mysql://127.0.0.1:33316/revision_document_workload?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The [workload harness](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/src/test/java/com/linkedin/openhouse/housetables/e2e/namespace/NamespaceMysqlWorkloadTest.java)
retains its synthetic fixture and captures generated SQL, successful operations, conflicts,
statement counts, and timing samples. It exercises maps of 10, 100, and 1,000 properties and
table/view occupancy queries. A one-row occupancy response can still require filtering and
counting every entity in that namespace.

### Occupancy regression comparison

The workload prints `OCCUPANCY_P95` for one and four readers. Each value is the
median of three warmed runs' p95 latencies, in milliseconds; each run makes 100
table-then-view occupancy checks. The fixture has 10,000 views in the requested
namespace and 20,000 in another namespace. Capture a baseline with
`namespaceMysqlWorkload`, then compare a candidate with
`namespaceMysqlOccupancyRegression` on a fresh schema using the same DDL, fixture,
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
./gradlew :services:housetables:namespaceMysqlOccupancyRegression \
  -PnamespaceMysqlUrl='jdbc:mysql://127.0.0.1:33316/occupancy_candidate?useSSL=false&allowPublicKeyRetrieval=true' \
  -PnamespaceOccupancyBaselineSingleP95Millis=64.583 \
  -PnamespaceOccupancyBaselineConcurrentP95Millis=88.952 \
  -PnamespaceOccupancyMaxRegressionPercent=50 \
  -x CopyGitHooksTask
```

Historical measurements of the child-table implementation do not validate this document schema.
Those sparse-update timing comparisons were inconclusive because the baseline varied.
The current checks establish local correctness and query access paths, not a performance
improvement or production capacity.
