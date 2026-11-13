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
create with another spelling fails without replacing the existing record.

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

Before deployment, use the local tests to verify exact property strings, retained name spelling,
UUID stability, sparse updates, concurrent conflicts, rollback, and table independence.

Current-schema MySQL plans use `uq_namespace_name` for case-insensitive name reads and `PRIMARY`
for UUID reads and versioned updates/deletes. The read plans return one row.
Write plans were checked with `EXPLAIN` only; those statements were not executed.

Use an isolated MySQL 8.0 schema containing synthetic data only. Apply the
[baseline DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0000__baseline.sql),
[entity-type DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0001__add_entity_type_to_user_table_row.sql), and
[namespace DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_namespace_metadata.sql)
to a new schema before running the validation task. The task disables automatic schema creation and
Spring SQL initialization. The namespace DDL describes this undeployed feature's initial schema,
not a migration for an existing deployment.

```bash
./gradlew :services:housetables:namespaceMysqlTest \
  -PnamespaceMysqlUrl='jdbc:mysql://127.0.0.1:33316/revision_document_service?useSSL=false&allowPublicKeyRetrieval=true' \
  -x CopyGitHooksTask
```

The task uses a local root account with an empty password and cleans its test records.
It is unsuitable for shared environments.

These checks establish local correctness and query access paths. They do not measure production
capacity. A sparse update rewrites the property document, so storage work grows with the map size.
The separately available workload tasks are optional performance investigations, not prerequisites
for using namespace defaults.
