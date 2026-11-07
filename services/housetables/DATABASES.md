# Independent database metadata

Database properties live as one JSON document in `catalog_databases`, alongside the database's
immutable UUID, logical name, normalized name, and version. Every metadata read needs the
whole map, so a single row matches the access pattern. A sparse update rewrites the document;
its storage cost grows with the map size.

## Storage

The document uses a JPA converter and a `LONGTEXT` column, with the same representation in
MySQL and H2. JSON preserves property spelling independently of database collation.
The normalized name has a unique index; the UUID is the primary key.

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

Historical measurements of the child-table implementation do not validate this document schema.
Those sparse-update timing comparisons were inconclusive because the baseline varied.
The current checks establish local correctness and query access paths, not a performance
improvement or production capacity.
