# Optimizer Schema Migrations

The Optimizer Service creates and changes its MySQL tables with reviewed, committed
[Flyway](https://documentation.red-gate.com/fd) migrations, which it applies when it starts. JPA
entities declare the schema the application needs, not every object the database may retain during
a staged rollout. [Atlas](https://atlasgo.io) can draft migration SQL explicitly; builds and CI only
verify the committed migrations and never generate SQL into source. The tables can share a database
with HouseTables' tables. Migrations may only target the four optimizer tables (`table_operations`,
`table_operations_history`, `table_stats`, `table_stats_history`) and `optimizer_schema_history`;
HouseTables' production schema and data must stay untouched.

| Path | Role |
|------|------|
| `libs/optimizer/optimizer-common/src/main/java/com/linkedin/openhouse/optimizer/db/` | The application's mapped columns and required indexes, declared with `@Column` and `@Table(indexes = ...)`. |
| `libs/optimizer/optimizer-common/src/main/resources/db/migration/` | The migrations, and Atlas's checksums of them in `atlas.sum`. They ship in the optimizer-common jar; only the service runs them. |
| `libs/optimizer/optimizer-common/atlas.hcl` | Atlas's configuration: the entities' DDL, the migrations, and the MySQL image Atlas compares them on. |
| `build/optimizer/entity-schema.sql` | The entities' DDL, written by `:services:optimizer:exportEntitySchema` independently of tests and migrations. Not committed. |
| `OptimizerDatabaseConfiguration` (optimizer-common) | Uses the same explicit table/column naming in the service, analyzer and scheduler. Name each column in `@Column(name = ...)`. |

## Tests

The optimizer repository, service, and scheduler integration tests run on MySQL 8.0.46, the release
production runs, in Docker, so start Docker before you run them. The test modules include Flyway on
their test runtime classpath. `MySqlContainerInitializer` (optimizer-common test fixtures) gives each
Spring test context a new [MySQL container](https://java.testcontainers.org/modules/databases/mysql/),
and Flyway migrates it as the service does at startup. Add it to a `@SpringBootTest`:

```java
@SpringBootTest
@ContextConfiguration(initializers = MySqlContainerInitializer.class)
class MyRepositoryTest { ... }
```

`exportEntitySchema` boots the service against H2 with the MySQL dialect and database actions and
Flyway disabled. Hibernate writes the entities' DDL under `build/`, even when a migration is missing.

`check`, and therefore `build`, runs `:services:optimizer:verifyMigrations`. Gradle downloads pinned
Atlas Community 1.3.0 and verifies its SHA-256; no separate install, account or paid features are
required. Atlas's native `schema diff` replays the committed Flyway migration directory on throwaway
MySQL and compares it with Hibernate's exported DDL. Gradle fails if Atlas reports SQL to bring the
migrations into agreement with the entities. A missing column or index therefore fails the required
build. Verification never writes migration SQL or `atlas.sum` into source.

The `verify` environment uses Atlas's built-in diff policy to skip drops of retained tables, columns
and indexes. This permits staged objects the current entities don't map. Other mapped definitions
must agree, including a `VARCHAR`'s width: even a wider migrated column is drift until the entity
declares that width. The authoring environment does not skip drops; review its proposed SQL.

This is schema-drift verification, not a general compatibility checker. Skipped extra objects can
still break writes: for example, a mandatory column without a default or an extra unique index.
It does not prove stored-data preservation, old/new application compatibility or production lock
duration. Review and exercise those properties for each migration; there are no handwritten
column/type/index comparison rules in this repository.

`OptimizerSchemaMigrationsTest` separately starts the actual service on empty MySQL and beside
HouseTables' real schema and seeded rows. Flyway applies the committed migrations and Hibernate
validates the entity mappings. Both starts must create the same optimizer tables, and HouseTables'
definitions and rows must be unchanged. Standalone tests do not invoke Atlas or generation.

GitHub's PR workflow runs `./gradlew clean build` for PRs into `main` and the migration stack's
base branch, `chbush/optimizer-flyway-atlas`. Remove the stack-only branch entry from
`.github/workflows/pr-validations.yml` after the base merges and the remaining PR targets `main`.

## Changing the Schema

You need Docker for Atlas's throwaway MySQL. Gradle installs the pinned, checksum-verified
[Atlas Community](https://atlasgo.io/community-edition) CLI (Apache 2.0) automatically. Generation is
an explicit authoring command; `build` and `check` only run the native read-only diff described above.

1. Edit the entities in `com.linkedin.openhouse.optimizer.db`.
2. Draft a migration such as `add_job_attempts`:
   ```bash
   ./gradlew :services:optimizer:generateMigration -PmigrationName=add_job_attempts
   ```
   No failing test is needed. Hibernate exports the entities independently; Atlas replays the
   migrations and DDL on throwaway MySQL and writes a candidate
   `V<UTC timestamp>__add_job_attempts.sql` plus `atlas.sum`. The command succeeds after generation or
   when there are no changes. Gradle removes only newly generated undo scripts and rehashes: the
   service never runs undo, and Flyway Community cannot apply it. Omitting `migrationName` uses
   `sync_entities`.

3. Review the SQL:
   - A desired-state diff is a draft, not a rollout plan. Remove proposed drops for columns or
     indexes still needed by older releases; SQL migrations may intentionally retain schema that
     the current entities no longer map. Write backfills and rename transitions deliberately.
   - Prefer one DDL statement per migration. MySQL commits each DDL statement on its own, so a
     migration that fails partway keeps its earlier statements, and the service won't start until
     someone sorts them out by hand ([Recovering from a Failed Migration](#recovering-from-a-failed-migration)).
   - Keep the previous release working on the new schema. Rolling back the service doesn't undo
     migrations, so add a column before code uses it, stop using it before a migration drops it, and
     give a new `NOT NULL` column a default.
   - Startup waits for the migration, so a long `ALTER TABLE` on a large table delays readiness and
     has to fit the deployment's startup probe ([What Happens at Startup](#what-happens-at-startup)).
4. Run verification and the tests:
   ```bash
   ./gradlew :services:optimizer:check :libs:optimizer:optimizer-common:test
   ```
5. Commit the entity change, the migration, and `atlas.sum` together.

Atlas writes DDL only. Write a migration that it can't, such as one that copies data, by hand: add
the file, then run `./gradlew :services:optimizer:hashMigrations` to update `atlas.sum`. The read-only
CI check accepts handwritten or generated SQL equally; it checks the result, not command history.

The timestamp columns declare `columnDefinition = "TIMESTAMP(6)"`, the type their migration created;
for an `Instant`, Hibernate's DDL would say `DATETIME(6)`.

If another change merged a migration first, `atlas.sum` conflicts when you rebase. Delete your
migration, take `atlas.sum` from the base branch, and run generation again so that yours gets a later
version. Don't keep a migration that sorts before the base branch's newest: Flyway won't apply it to
a database that already has the newer one, and the service stops starting there. The tests can't
catch it, because they migrate databases that have none of the migrations.

```
Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 20261020000000.
```

Never edit a migration that has shipped: Flyway compares every applied migration's checksum at
startup and refuses to start on a mismatch. Fix it with a new migration instead. After editing one
that hasn't shipped, run `./gradlew :services:optimizer:hashMigrations`.

## What Happens at Startup

Spring Boot runs Flyway before JPA starts. Hibernate then validates the mapped tables, columns, and
types (`spring.jpa.hibernate.ddl-auto=validate`) before the service accepts requests; it does not
change the schema. Validation does not check stored values, indexes, or compatibility with other
application versions. Flyway records each applied migration in `optimizer_schema_history` and applies
the pending ones in version order. Replicas that start together take turns through a MySQL named lock,
so each migration runs once.

The service can share HouseTables' database. Its MySQL user can read and write HouseTables' tables,
so Flyway sees them: it records a baseline at version 0 in the nonempty database, then applies every
migration, since each has a higher version. Flyway doesn't check whose the other tables are: pointed
at the wrong database, the service adds its tables there.

The migrations may only create and change the optimizer tables and `optimizer_schema_history`.
Neither Flyway nor the MySQL account enforces that: Flyway runs whatever SQL a migration holds, and
the user can write HouseTables' tables. Review each migration so that it doesn't reference
HouseTables' tables, and rely on the preservation test ([Tests](#tests)) as a check.

On the first rollout, HouseTables' tables may exist and the optimizer's tables must not. Flyway
doesn't adopt or diff optimizer tables that exist without history: the initial migration fails on
the existing table and the service doesn't start. Any reset of such tables needs a separate,
explicit approval of what happens to their data, and never includes HouseTables' tables.

The HTTP port opens only once the migrations finish, so the liveness probe fails for as long as one
runs. Give the deployment a startup probe that allows for the slowest migration: Kubernetes holds off
the liveness probe until the startup probe passes. A pod killed mid-migration leaves its statement
running in MySQL, and the service may not start again until someone recovers it by hand
([Recovering from a Failed Migration](#recovering-from-a-failed-migration)).

Flyway's `clean`, which drops every table in the database, HouseTables' included, is disabled. That
guards only against `clean`, not against migration SQL that touches other tables.

Flyway ignores applied migrations newer than the newest one it knows. A rolled-back release can
still start if the newer schema satisfies its entity mappings; migrations must preserve compatibility
with the previous release.

The analyzer and scheduler apps don't migrate. Hibernate checks at their startup that the tables
match their entities (`spring.jpa.hibernate.ddl-auto=validate`), so an app ahead of the service's
schema exits before it reads or writes anything. Roll out the service before the apps.

```
SchemaManagementException: Schema-validation: missing column [attempts] in table [table_operations]
```

Validation is a packaged default for the optimizer service and apps, not a setting for other services.
Rebuild and redeploy to adopt it. Spring Boot external configuration, such as
`SPRING_JPA_HIBERNATE_DDL_AUTO`, can override the default. OpenAPI generation alone disables both
Flyway and validation because its spec-only H2 database has no tables.

The service runs Flyway 8.5.13, the release Spring Boot 2.7 is built against. It supports MySQL up
to 8.0; on a later MySQL it logs that support hasn't been tested.

## Recovering from a Failed Migration

A migration fails when a statement errors, or when the service is killed while it runs: by a probe,
a rollout, or a node drain. The service then exits at every start, with one of:

```
Migration of schema `oh_db` to version "20261020000000 - add job attempts" failed!
Detected failed migration to version 20261020000000 (add job attempts).
```

MySQL commits each DDL statement on its own, and keeps running a statement whose client was killed,
so the database can hold all, some, or none of the migration's changes. Flyway records the migration
as failed (`success = 0` in `optimizer_schema_history`) when a statement errors, including when the
start after a kill runs the migration again into its own half-applied change. Flyway's `repair`,
which the service doesn't run, only deletes that row, so the next start runs into the same error.

Recover by hand, as a MySQL admin connected to the optimizer's database:

1. Make sure none of the migration's statements is still running; one blocked by another
   transaction shows `Waiting for table metadata lock`. Wait for it to finish, or stop it: MySQL
   rolls back a statement it stops.
   ```sql
   SELECT id, time, state, info FROM performance_schema.processlist
   WHERE db = DATABASE() AND command = 'Query' AND id <> CONNECTION_ID();
   KILL <id>;
   ```
2. Find the failed migration:
   ```sql
   SELECT version, script FROM optimizer_schema_history WHERE success = 0;
   ```
   No row means no start has failed on it yet. Restart the service: it runs the migration again,
   and records it as failed if the change is already there.
3. Check which of the script's statements took effect, with `SHOW CREATE TABLE`. The script is in
   `libs/optimizer/optimizer-common/src/main/resources/db/migration/` at the release that ran it.
4. Make the history match the database:
   - All of them: mark the migration applied.
     ```sql
     UPDATE optimizer_schema_history SET success = 1 WHERE version = '<version>';
     ```
   - None: delete the row, so that the next start runs the migration again.
     ```sql
     DELETE FROM optimizer_schema_history WHERE version = '<version>';
     ```
   - Some: run the rest by hand and mark the migration applied, or revert them and delete the row.
5. Restart the service.

## Database Privileges

The service's MySQL user needs at least these privileges on the optimizer's tables. It can also
read and write HouseTables' tables in a shared database, so the account's privileges are not a
safety boundary: migration review and the preservation test are.

| Tables | Privileges |
|--------|------------|
| `table_operations`, `table_operations_history`, `table_stats`, `table_stats_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, and `DROP` once a migration drops a table |
| `optimizer_schema_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `INDEX`: Flyway creates the table, then its index with `CREATE INDEX`. Without `INDEX`, it keeps the table and goes on without the index. |

Grant these before the service first starts; the optimizer tables don't exist until then. Atlas
writes index changes on existing tables as `ALTER TABLE`, which doesn't need `INDEX`; write the
ones you write by hand the same way.
