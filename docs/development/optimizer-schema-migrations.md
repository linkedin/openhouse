# Optimizer Schema Migrations

The Optimizer Service creates and changes its MySQL tables with
[Flyway](https://documentation.red-gate.com/fd) migrations, which it applies when it starts. The JPA
entities declare the tables, and [Atlas](https://atlasgo.io) writes the migration that gets a
database there. The tables can share a database with HouseTables' tables. The migrations' only
targets are the four optimizer tables (`table_operations`, `table_operations_history`, `table_stats`,
`table_stats_history`) and `optimizer_schema_history`. HouseTables' production schema and data must
stay untouched.

| Path | Role |
|------|------|
| `libs/optimizer/optimizer-common/src/main/java/com/linkedin/openhouse/optimizer/db/` | The entities, with their columns in `@Column` and their indexes in `@Table(indexes = ...)`. They declare the tables. |
| `libs/optimizer/optimizer-common/src/main/resources/db/migration/` | The migrations, and Atlas's checksums of them in `atlas.sum`. They ship in the optimizer-common jar; only the service runs them. |
| `libs/optimizer/optimizer-common/atlas.hcl` | Atlas's configuration: the entities' DDL, the migrations, and the MySQL image Atlas compares them on. |
| `build/optimizer/entity-schema.sql` | The entities' DDL, written by `:services:optimizer:exportEntitySchema` without migrations or validation. Not committed. |
| `OptimizerDatabaseConfiguration` (optimizer-common) | Maps the entities to the table and column names their annotations give, in the service and both apps, as in the entities' DDL. Hibernate doesn't turn camelCase into snake_case, so name each column in `@Column(name = ...)`. |

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

`exportEntitySchema` boots the service against an in-memory H2 database with the MySQL dialect and
both Flyway and database schema actions disabled. Hibernate writes the entities' DDL without needing
Docker or matching migrations. The service test task depends on this export.

`OptimizerSchemaMigrationsTest` starts the service on an empty MySQL database: Flyway applies every
migration and Hibernate validates the entities against the tables. The test runs the exported DDL on
another empty database and fails unless both hold the same tables, indexes included, so an entity
change without its migration fails the required build. It also starts the service on a database
holding the real HouseTables schema with seeded rows. Both must end up with the same optimizer
tables, and HouseTables' table definitions and row contents must be identical before and after.

## Changing the Schema

### Automatic Generation in Pull Requests

Edit the entities and open or update a PR. The **Optimizer Schema Migrations** workflow runs
`./gradlew :services:optimizer:generateMigration`. Gradle exports the entities' DDL and invokes the
pinned Atlas Community CLI against a disposable MySQL database. If the committed migrations already
match, it writes nothing. Otherwise it generates a new `V<UTC timestamp>__sync_entities.sql`, removes
Atlas's undo script, and updates `atlas.sum`; existing versioned migrations stay untouched.

For a branch in this repository, a separate publishing job uses
[`git-auto-commit-action`](https://github.com/stefanzweifel/git-auto-commit-action) to commit
only the new migration and `atlas.sum` to the PR branch. Generation has a read-only token; publishing
executes no PR code and accepts only those two files as data. It pushes without force, so a newer
branch head rejects the stale commit. It explicitly dispatches **Pull Request Validations** after
the bot commit, because a push made with `GITHUB_TOKEN` does not start those checks automatically.
The required build runs the schema comparison on the committed result, not an uncommitted fix.
`build` and `test` do not generate migrations: their schema comparison must still reject entity
changes whose migrations have not been committed.

Review the generated SQL before approving the new head. The bot never approves or merges, and its
commit dismisses earlier approvals under the repository's rules. A rename can be generated as a
drop and add; backfills and compatibility with the previous release still need human judgment.
Migration failures or invalid checksums fail generation rather than being ignored.

For a fork PR, the upstream token cannot push to the fork. The workflow instead uploads an
`optimizer-migration-<PR number>` artifact: copy its two files into
`libs/optimizer/optimizer-common/src/main/resources/db/migration/` and commit them to the fork. The
required build continues to reject drift until those files are committed. This is the exception to
zero-command authoring; the automation does not request contributors' write credentials.

The automatic publisher uses only `GITHUB_TOKEN` with job-scoped `contents: write` and
`actions: write`; no Atlas account or paid features are needed. Both workflow files, including the
`workflow_dispatch` trigger on `pr-validations.yml`, must be on the default branch before the bot can
dispatch validation. Until then, use the local command below to commit a migration yourself.

### Local Generation and SQL Review

You need Docker, for the throwaway MySQL that Atlas compares on, and the
[Atlas Community](https://atlasgo.io/community-edition) CLI (Apache 2.0):

```bash
curl -sSf https://atlasgo.sh | ATLAS_VERSION=v1.3.0 sh -s -- --community
```

1. Edit the entities in `com.linkedin.openhouse.optimizer.db`.
2. Generate the migration, such as `add_job_attempts`:
   ```bash
   ./gradlew :services:optimizer:generateMigration -PmigrationName=add_job_attempts
   ```
   This task depends on `exportEntitySchema` and runs Atlas's `migrate diff`, which validates the
   existing checksums before generating SQL. Gradle removes only newly generated undo scripts,
   rehashes the directory, and copies the new migration and `atlas.sum` to
   `build/optimizer/generated-migrations/` for the PR workflow. Without schema changes, it produces
   no artifact. No passing migration test is needed to export the DDL. To export DDL alone, run
   `./gradlew :services:optimizer:exportEntitySchema`.
3. Review the SQL:
   - Prefer one DDL statement per migration. MySQL commits each DDL statement on its own, so a
     migration that fails partway keeps its earlier statements, and the service won't start until
     someone sorts them out by hand ([Recovering from a Failed Migration](#recovering-from-a-failed-migration)).
   - Keep the previous release working on the new schema. Rolling back the service doesn't undo
     migrations, so add a column before code uses it, stop using it before a migration drops it, and
     give a new `NOT NULL` column a default.
   - Startup waits for the migration, so a long `ALTER TABLE` on a large table delays readiness and
     has to fit the deployment's startup probe ([What Happens at Startup](#what-happens-at-startup)).
4. Run the tests:
   ```bash
   ./gradlew :libs:optimizer:optimizer-common:test :services:optimizer:test
   ```
5. Commit the entity change, the migration, and `atlas.sum` together.

Atlas writes DDL only. Write a migration that it can't, such as one that copies data, by hand: add
the file, then run `atlas migrate hash --env local` to update `atlas.sum`. The test still checks
that the tables end up as the entities declare them.

The timestamp columns declare `columnDefinition = "TIMESTAMP(6)"`, the type their migration created;
for an `Instant`, Hibernate's DDL would say `DATETIME(6)`.

If another change merged a migration first, `atlas.sum` conflicts when you rebase. Delete your
migration, take `atlas.sum` from the base branch, and rerun local generation or update the PR so the
workflow generates a later version. Don't keep a migration that sorts before the base branch's
newest: Flyway won't apply it to a database that already has the newer one, and the service stops
starting there. The tests can't catch it, because they migrate databases that have none of the
migrations.

```
Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 20261020000000.
```

Never edit a migration that has shipped: Flyway compares every applied migration's checksum at
startup and refuses to start on a mismatch. Fix it with a new migration instead. After editing one
that hasn't shipped, run `atlas migrate hash --env local`.

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
