# Optimizer Schema Migrations

The Optimizer Service creates and changes its MySQL tables with
[Flyway](https://documentation.red-gate.com/fd) migrations, which it applies when it starts. The JPA
entities declare the tables, and [Atlas](https://atlasgo.io) writes the migration that gets a
database there. The tables can share a database with HouseTables' tables.

| Path | Role |
|------|------|
| `libs/optimizer/optimizer-common/src/main/java/com/linkedin/openhouse/optimizer/db/` | The entities, with their columns in `@Column` and their indexes in `@Table(indexes = ...)`. They declare the tables. |
| `libs/optimizer/optimizer-common/src/main/resources/db/migration/` | The migrations, and Atlas's checksums of them in `atlas.sum`. They ship in the optimizer-common jar; only the service runs them. |
| `libs/optimizer/optimizer-common/atlas.hcl` | Atlas's configuration: the entities' DDL, the migrations, and the MySQL image Atlas compares them on. |
| `build/optimizer/entity-schema.sql` | The entities' DDL, which Hibernate writes when `OptimizerSchemaMigrationsTest` runs. Not committed. |
| `OptimizerDatabaseConfiguration` (optimizer-common) | Maps the entities to the table and column names their annotations give, in the service and both apps, as in the entities' DDL. Hibernate doesn't turn camelCase into snake_case, so name each column in `@Column(name = ...)`. |

## Tests

The repository and service tests run on MySQL 8.0.46, the release production runs, in Docker, so
start Docker before you run them. `MySqlContainerInitializer` (optimizer-common test fixtures) gives
each Spring test context a new [MySQL container](https://java.testcontainers.org/modules/databases/mysql/),
and Flyway migrates it as the service does at startup. Add it to a `@SpringBootTest`:

```java
@SpringBootTest
@ContextConfiguration(initializers = MySqlContainerInitializer.class)
class MyRepositoryTest { ... }
```

`OptimizerSchemaMigrationsTest` starts the service on an empty database: Flyway applies every
migration, and Hibernate writes the entities' DDL and validates the entities against the tables. The
test runs the DDL on another empty database and fails unless both hold the same tables, indexes
included, so an entity change without its migration fails the build. It also starts the service on
a database that already holds HouseTables' tables, as the database it shares with HouseTables does,
and checks that it ends up with the same tables and keeps HouseTables' tables and rows.

## Changing the Schema

You need Docker, for the throwaway MySQL that Atlas compares on, and the
[Atlas Community](https://atlasgo.io/community-edition) CLI (Apache 2.0):

```bash
curl -sSf https://atlasgo.sh | sh -s -- --community
```

1. Edit the entities in `com.linkedin.openhouse.optimizer.db`.
2. Write their DDL, then generate the migration, such as `add_job_attempts`:
   ```bash
   ./gradlew :services:optimizer:test --tests '*OptimizerSchemaMigrationsTest'
   cd libs/optimizer/optimizer-common
   atlas migrate diff --env local add_job_attempts
   ```
   The test fails, as no migration makes the change yet, but Hibernate writes the entities' DDL
   first. Atlas applies the migrations and that DDL to a throwaway MySQL, writes the difference to
   `V<UTC timestamp>__add_job_attempts.sql`, and adds the file to `atlas.sum`. Delete the
   `U<UTC timestamp>__add_job_attempts.sql` undo script it writes too: undo needs Flyway Teams, and
   the service never runs it.
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
migration, take `atlas.sum` from the base branch, and run step 2 again, so that yours gets a later
version. Don't keep a migration that sorts before the base branch's newest: Flyway won't apply it to
a database that already has the newer one, and the service stops starting there. The tests can't
catch it, because they migrate databases that have none of the migrations.

```
Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 20261020000000.
```

Never edit a migration that has shipped: Flyway compares every applied migration's checksum at
startup and refuses to start on a mismatch. Fix it with a new migration instead. After editing one
that hasn't shipped, run `atlas migrate hash --env local`.

## What Happens at Startup

Spring Boot runs Flyway before JPA starts and before the HTTP port opens. Flyway records each
applied migration in `optimizer_schema_history` and applies the pending ones in version order.
Replicas that start together take turns through a MySQL named lock, so each migration runs once.

The service can share HouseTables' database. A MySQL user granted only the optimizer's tables, as
[Database Privileges](#database-privileges) allows, sees none of HouseTables' tables, so Flyway
finds the database empty and applies every migration. A user granted the whole database sees them,
and Flyway first records a baseline at version 0; it still applies every migration, since each has
a higher version. Flyway doesn't check whose the other tables are: pointed at the wrong database,
the service adds its tables there.

The HTTP port opens only once the migrations finish, so the liveness probe fails for as long as one
runs. Give the deployment a startup probe that allows for the slowest migration: Kubernetes holds off
the liveness probe until the startup probe passes. A pod killed mid-migration leaves its statement
running in MySQL, and the service may not start again until someone recovers it by hand
([Recovering from a Failed Migration](#recovering-from-a-failed-migration)).

Flyway's `clean`, which drops every table in the database, HouseTables' included, is disabled.

Flyway ignores applied migrations newer than the newest one it knows, so a rolled-back release
still starts against the newer schema.

The analyzer and scheduler apps don't migrate. Hibernate checks at their startup that the tables
match their entities (`spring.jpa.hibernate.ddl-auto=validate`), so an app ahead of the service's
schema exits before it reads or writes anything. Roll out the service before the apps.

```
SchemaManagementException: Schema-validation: missing column [attempts] in table [table_operations]
```

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

The service's MySQL user needs these privileges, which a database it shares can grant on the
optimizer's tables alone:

| Tables | Privileges |
|--------|------------|
| `table_operations`, `table_operations_history`, `table_stats`, `table_stats_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, and `DROP` once a migration drops a table |
| `optimizer_schema_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `INDEX`: Flyway creates the table, then its index with `CREATE INDEX`. Without `INDEX`, it keeps the table and goes on without the index. |

MySQL accepts a grant on a table that doesn't exist yet if the grant includes `CREATE`, so grant them
all before the service first starts. Atlas writes index changes on existing tables as `ALTER TABLE`,
which doesn't need `INDEX`; write the ones you write by hand the same way.
