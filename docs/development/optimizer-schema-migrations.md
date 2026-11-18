# Optimizer Schema Migrations

The Optimizer Service runs [Flyway](https://documentation.red-gate.com/fd) migrations when it
starts. These migrations create and update its MySQL tables.

The optimizer can share a database with HouseTables. Migrations may only change these tables:

- `table_operations`
- `table_operations_history`
- `table_stats`
- `table_stats_history`
- `optimizer_schema_history`

Do not change HouseTables' schema or data.

| Path | Role |
|------|------|
| `libs/optimizer/optimizer-common/src/main/java/com/linkedin/openhouse/optimizer/db/` | Defines the columns with `@Column` and the indexes with `@Table(indexes = ...)`. These entities map the tables the migrations create. |
| `libs/optimizer/optimizer-common/src/main/resources/db/migration/` | Contains the migrations. These ship in the optimizer-common jar. Only the service runs them. |

## Tests

The optimizer integration tests run on MySQL 8.0.46 in Docker. Production uses the same MySQL
release. Start Docker before running the repository tests or the service and scheduler integration
tests.

The test modules include Flyway at runtime. `MySqlContainerInitializer` starts a new
[MySQL container](https://java.testcontainers.org/modules/databases/mysql/) for each Spring test
context. Flyway applies the migrations just as it does when the service starts. The initializer
lives in the optimizer-common test fixtures. Add it to a `@SpringBootTest`:

```java
@SpringBootTest
@ContextConfiguration(initializers = MySqlContainerInitializer.class)
class MyRepositoryTest { ... }
```

`OptimizerSchemaMigrationsTest` starts the service as a MySQL user that can access only the four
optimizer tables and `optimizer_schema_history`. A migration that reads or changes any other table
fails, and so does the startup. This includes `DROP TABLE IF EXISTS` on a table that doesn't exist.
The test starts the service on an empty database, and on one that holds the real HouseTables schema
with seeded rows. Both must end up with the same optimizer tables, and HouseTables' table
definitions and rows must not change.

The test grants access through its own `OPTIMIZER_TABLES` list, not one derived from the
migrations. Add each new optimizer table to it. Never replace it with a database-wide grant.

This check runs before deployment. It does not isolate production tables. It cannot cover SQL
paths that depend on production-only data or configuration. It cannot protect a deployment that
skips the checked migrations. Production credentials are unchanged. Review the SQL and require
`:services:optimizer:test` to pass before deploying.

## Changing the Schema

1. Write the migration in `libs/optimizer/optimizer-common/src/main/resources/db/migration/`. Name
   it `V<UTC timestamp>__<description>.sql`. An example is `V20261020000000__add_job_attempts.sql`.
   Flyway applies migrations in version order. A timestamp keeps two branches from taking the same
   version.
2. Edit the entities in `com.linkedin.openhouse.optimizer.db` to match.
3. Review the SQL:
   - Prefer one DDL statement per migration. MySQL commits each statement separately. A failure
     leaves earlier statements in place. The service will not start until someone fixes the
     partial migration by hand. See [Recovering from a Failed Migration](#recovering-from-a-failed-migration).
   - Keep the previous release working on the new schema. Rolling back the service does not undo
     migrations. Add a column before code uses it. Stop using a column before dropping it. Give a
     new `NOT NULL` column a default.
   - The service waits for migrations before it becomes ready. A long `ALTER TABLE` can delay
     startup on a large table. Set the startup probe to allow enough time for it to finish.
     See [What Happens at Startup](#what-happens-at-startup).
4. Run the tests:
   ```bash
   ./gradlew :libs:optimizer:optimizer-common:test :services:optimizer:test
   ```
5. Commit the entity change and the migration together.

Another migration may merge before yours. Give yours a new timestamp when you rebase. Your
migration must sort after the base branch's newest migration. Flyway will not apply an older
migration to a database that already has the newer one. The service will fail to start there.
The tests cannot catch this ordering problem because they start with no migrations applied.

```
Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 20261020000000.
```

Never edit a migration that has shipped. Flyway checks each applied migration's checksum at startup.
The service refuses to start if a checksum has changed. Add a new migration to fix a shipped one.

## What Happens at Startup

Spring Boot runs Flyway before JPA starts. Flyway applies pending migrations in version order.
It records each applied migration in `optimizer_schema_history`. A MySQL named lock makes replicas
take turns so that each migration runs once. The HTTP port opens after the migrations finish.

The service can share HouseTables' database. Its MySQL user can read and write HouseTables' tables.
Flyway sees those tables and records a version 0 baseline in the nonempty database. This marks the
starting point before any optimizer migration. Flyway then applies every migration because their
versions are higher than 0. Check the database URL before deployment. Flyway does not check which
service owns the existing tables. The optimizer will add its tables even in the wrong database.

Migrations may only change the optimizer tables and `optimizer_schema_history`. Neither Flyway nor
the current shared production account enforces this limit. Flyway runs whatever SQL the migration
contains. The account can write to HouseTables' tables. Check each migration for references to
HouseTables' tables. The [tests](#tests) run the migrations as a user limited to the optimizer
tables, so a migration that touches another table fails there. These test grants do not change
production permissions.

HouseTables' tables may exist before the first rollout. The optimizer tables must not exist yet.
Flyway does not adopt existing optimizer tables that have no migration history. The initial
migration fails if they exist. The service then refuses to start. Get separate approval before
resetting those tables. The approval must cover what happens to their data. Never include
HouseTables' tables in a reset.

Liveness probes fail while a migration runs. Set a startup probe that allows enough time for the
slowest migration. Kubernetes waits for that probe to pass before running liveness probes. Killing
the pod does not stop its SQL statement in MySQL. Manual recovery may be needed before the service
can start again. See [Recovering from a Failed Migration](#recovering-from-a-failed-migration).

Flyway's `clean` command is disabled. It would drop every table in the database including
HouseTables' tables. Disabling `clean` does not stop migration SQL from changing other tables.

Flyway ignores applied migrations that are newer than the release knows about. A rolled-back
release still starts against the newer schema.

The analyzer and scheduler do not run migrations. They use the tables the service migrated. Roll
out the service first.

The service runs Flyway 8.5.13. Spring Boot 2.7 is built against this release. It supports MySQL
through 8.0. On later MySQL releases it logs a warning that support has not been tested.

## Recovering from a Failed Migration

A failed SQL statement can leave a migration incomplete. A probe can kill the service during a
migration. A rollout or node drain can do the same. The service may then fail at every start with
one of these errors:

```
Migration of schema `oh_db` to version "20261020000000 - add job attempts" failed!
Detected failed migration to version 20261020000000 (add job attempts).
```

MySQL commits each DDL statement separately. It keeps running a statement after its client is
killed. The database may contain all or some or none of the migration's changes.

Flyway records a failed statement with `success = 0` in `optimizer_schema_history`. After a kill
the next start may retry a statement that already took effect. That retry also records a failure.
The service does not run Flyway's `repair` command. Removing the failed history row with `repair`
does not undo database changes. The next start can hit the same error.

Connect to the optimizer database as a MySQL admin. Recover by hand:

1. Make sure none of the migration's statements is still running. A statement blocked by another
   transaction shows `Waiting for table metadata lock`. Wait for it to finish or stop it. MySQL
   rolls back a statement that it stops.
   ```sql
   SELECT id, time, state, info FROM performance_schema.processlist
   WHERE db = DATABASE() AND command = 'Query' AND id <> CONNECTION_ID();
   KILL <id>;
   ```
2. Find the failed migration:
   ```sql
   SELECT version, script FROM optimizer_schema_history WHERE success = 0;
   ```
   No row means no start has recorded a failure yet. Restart the service. It retries the
   migration and records a failure if the change is already there.
3. Use `SHOW CREATE TABLE` to check which statements took effect. Find the script under
   `libs/optimizer/optimizer-common/src/main/resources/db/migration/` in the release that ran it.
4. Make the history match the database:
   - All statements took effect: mark the migration applied.
     ```sql
     UPDATE optimizer_schema_history SET success = 1 WHERE version = '<version>';
     ```
   - No statements took effect: delete the row. The next start runs the migration again.
     ```sql
     DELETE FROM optimizer_schema_history WHERE version = '<version>';
     ```
   - Some statements took effect: run the rest by hand and mark the migration applied.
     Alternatively undo the changes that took effect and delete the row.
5. Restart the service.

## Database Privileges

In production the optimizer should use its own MySQL user. Follow these rules for that user:

- Grant access to the exact optimizer table names.
- Give no access to HouseTables. This includes `SELECT`.
- Do not use a database-wide or global grant. Either one removes this limit.
- Check grants inherited through roles too. They must not give access to other tables.

Table-level grants do not cancel broader grants. Any broader grant still gives access.

If migrations run separately from the service:

- Give DDL privileges only to the migration user.
- Give the runtime user only the DML privileges it needs.

This production change is not part of the current rollout. The restricted test user has these
table-level grants. The shared production account can still access HouseTables:

| Tables | Privileges |
|--------|------------|
| `table_operations`, `table_operations_history`, `table_stats`, `table_stats_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`, `INDEX` |
| `optimizer_schema_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `INDEX`. Flyway creates the table and then adds its index with `CREATE INDEX`. Without `INDEX` it keeps the table but skips the index. |

Grant these privileges before the service first starts. MySQL allows table-level grants before the
named tables exist. This includes `CREATE`. In production grant only the privileges that the
deployed migrations need. `INDEX` is needed for standalone `CREATE INDEX` and `DROP INDEX`. Index
changes inside `ALTER TABLE` need `ALTER` instead.
