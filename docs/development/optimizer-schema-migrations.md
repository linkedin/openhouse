# Optimizer Schema Migrations

The Optimizer Service creates and changes its MySQL tables with
[Flyway](https://documentation.red-gate.com/fd) migrations, which it applies when it starts. The
tables can share a database with HouseTables' tables. The migrations' only targets are the four
optimizer tables (`table_operations`, `table_operations_history`, `table_stats`,
`table_stats_history`) and `optimizer_schema_history`. HouseTables' production schema and data must
stay untouched.

| Path | Role |
|------|------|
| `libs/optimizer/optimizer-common/src/main/java/com/linkedin/openhouse/optimizer/db/` | The entities, with their columns in `@Column` and their indexes in `@Table(indexes = ...)`. They map the tables the migrations create. |
| `libs/optimizer/optimizer-common/src/main/resources/db/migration/` | The migrations. They ship in the optimizer-common jar; only the service runs them. |

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

`OptimizerSchemaMigrationsTest` starts the service on an empty database, and on one that holds the
real HouseTables schema with seeded rows. Both must end up with the same optimizer tables, and
HouseTables' table definitions and row contents must be identical before and after.

## Changing the Schema

1. Write the migration in `libs/optimizer/optimizer-common/src/main/resources/db/migration/`, named
   `V<UTC timestamp>__<description>.sql`, such as `V20261020000000__add_job_attempts.sql`. Flyway
   applies migrations in version order; a timestamp keeps two branches from taking the same version.
2. Edit the entities in `com.linkedin.openhouse.optimizer.db` to match.
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
5. Commit the entity change and the migration together.

If another change merged a migration with a later version first, give yours a new timestamp when you
rebase. Don't keep a migration that sorts before the base branch's newest: Flyway won't apply it to
a database that already has the newer one, and the service stops starting there. Fresh-database
tests don't catch this:

```
Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 20261020000000.
```

CI compares migrations against the PR's fetched base commit, including for stacked PRs. Once a SQL
migration is in that base, it cannot be edited, deleted, or renamed. New versions must sort after
the base's newest version; duplicate versions are rejected. This is deliberately stricter than
Flyway's checksum check at startup: merged migrations are immutable even before deployment. Add a
new migration to change the schema. `atlas.sum` can change to include the new file.

Run the same guard locally against your fetched base ref (`linkedin/main` for a PR against `main`):

```bash
python3 scripts/python/check_optimizer_migration_history.py --base linkedin/main
```

The guard includes untracked migration files and fails if the base ref is unavailable. Rebase before
merging when another migration lands first; the check only knows the base commit it is given.

## What Happens at Startup

Spring Boot runs Flyway before JPA starts and before the HTTP port opens. Flyway records each
applied migration in `optimizer_schema_history` and applies the pending ones in version order.
Replicas that start together take turns through a MySQL named lock, so each migration runs once.

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

Flyway ignores applied migrations newer than the newest one it knows, so a rolled-back release
still starts against the newer schema.

The analyzer and scheduler apps don't migrate: they use the tables the service migrated. Roll out
the service before the apps.

The service runs Flyway 8.5.13, the release Spring Boot 2.7 is built against. It supports MySQL up
to 8.0; on a later MySQL it logs that support hasn't been tested.

## Recovering from a Failed Migration

A migration fails when a statement errors, or when the service is killed while it runs: by a probe,
a rollout, or a node drain. The service then exits at every start, with one of:

```
Migration of schema `oh_db` to version "20261020000000 - add job attempts" failed!
Detected failed migration to version 20261020000000 (add job attempts).
```

MySQL commits each DDL statement on its own, so a failed migration leaves every statement that ran
before the failure in place; Flyway doesn't roll them back. A killed client doesn't tell you what ran:
the statement it was running may still be running, may have finished, or may have been undone, and a
killed pod may never have written the failed row. The database can therefore hold all, some, or none
of the migration's changes whatever the history table says. A missing `success = 0` row doesn't mean
nothing changed, and restarting the service to make Flyway record one isn't a recovery step: it
reruns the migration against the half-applied schema.

Flyway's `repair` doesn't undo the changes either. In 8.5.13 it:

- deletes failed rows (`success = 0`) from `optimizer_schema_history`;
- rewrites the checksum, description, and type of each applied row to match the migration files it
  is given;
- can mark applied rows whose migration files it can't find as deleted;
- leaves every table, column, index, and row alone.

The second and third are why `repair` is only safe against the exact migration files that produced
the history. Pointed at an edited or older migration set, it records the edit as applied without
running it, and hides a missing migration. Never use it to get past a checksum mismatch or a missing
migration; those need a new migration or a reviewed decision.

Recover as a MySQL admin, in this order:

1. **Stop the writers.** Scale the service to zero replicas, so that no replica can start Flyway
   again and take the migration lock, and pause the analyzer and scheduler apps and anything else
   writing the optimizer tables. Rolling the service back doesn't help: every release with this
   migration fails the same way. Don't start recovery while a replica may still be restarting.
2. **Find out what is running.** A statement from the killed migration can still be running in
   MySQL, such as one blocked on `Waiting for table metadata lock`. Let it finish; stopping it with
   `KILL` isn't guaranteed to leave the schema as it was.
   ```sql
   SELECT id, time, state, info FROM performance_schema.processlist
   WHERE db = DATABASE() AND command = 'Query' AND id <> CONNECTION_ID();
   ```
3. **Take a current backup, and have a tested restore of the last one from before the deploy.** The
   current backup holds the half-applied state and the data written since. Check that the restore
   works before you rely on it.
4. **Inspect the real state.** Read the history table and the actual schema:
   ```sql
   SELECT installed_rank, version, description, checksum, success
   FROM optimizer_schema_history ORDER BY installed_rank;
   SHOW CREATE TABLE <table>;  -- for each table the migration touches
   ```
   Compare against the script of the failed version, in
   `libs/optimizer/optimizer-common/src/main/resources/db/migration/` at the release that ran it,
   one statement at a time. Find why the statement failed, in the database's error message: usually
   something in the environment, such as rows that break a new unique index, or an object that
   already exists.
5. **Fix the external cause, with the data's owners.** Don't change the migration file: the history
   guard forbids editing any merged migration, and a recorded checksum has to stay valid. Remove or
   correct the conflicting data or object only after review, and only after the backup. If the
   script itself is wrong, such as invalid SQL, no environment change makes it pass: that takes a
   reviewed, exceptional recovery decided case by case, not the replay below.
6. **Revert what the migration already did, and only that.** Undo each statement that took effect,
   in reverse order, and only after checking with `SHOW CREATE TABLE` that the object is the one
   the script created: don't drop a column or index on a guess, since an earlier migration or a
   later hand change may own it. Anything that is not purely additive, such as a dropped or
   rewritten column or table, can't be reverted here: restore it from the backup and tell the data's
   owners. The goal is exactly the state the script was written for, so that the unchanged script
   runs again from its first statement. Don't edit `optimizer_schema_history` by hand.
7. **Check the release, preflight, repair, migrate.** Run Flyway 8.5.13 against that release's
   complete migration directory: every file in `db/migration/` of the release that ran the failed
   migration (check out its tag or commit and record it), unedited, with the same history table and
   baseline settings the service uses. Don't assume a Flyway CLI is installed. This runs it from
   the pinned image, in the checkout, with the password read from the shell's environment rather
   than the command line. Run the preflight first; don't proceed automatically to `repair`:
   ```bash
   export FLYWAY_PASSWORD   # the admin's password, set without echoing it or writing it to a file
   flyway() {  # flyway <command>
     docker run --rm \
       -e FLYWAY_URL='jdbc:mysql://<host>:3306/<optimizer database>' \
       -e FLYWAY_USER='<admin user>' -e FLYWAY_PASSWORD \
       -e FLYWAY_TABLE=optimizer_schema_history \
       -e FLYWAY_BASELINE_ON_MIGRATE=true -e FLYWAY_BASELINE_VERSION=0 \
       -e FLYWAY_CLEAN_DISABLED=true \
       -e FLYWAY_LOCATIONS=filesystem:/flyway/sql \
       -v "$PWD/libs/optimizer/optimizer-common/src/main/resources/db/migration:/flyway/sql:ro" \
       flyway/flyway:8.5.13 "$@"
   }
   flyway info
   flyway -ignoreFutureMigrations=false -ignoreMigrationPatterns='*:pending' -outputType=json validate
   ```
   `info` doesn't compare checksums. Inspect `validationSuccessful` and every entry in
   `invalidMigrations`: **JSON validation exits 0 even when validation fails in 8.5.13.** Continue
   only if validation succeeds (there may be no failed history row), or the only invalid entry is
   the failed version with `FAILED_VERSIONED_MIGRATION`. A checksum mismatch, a missing migration,
   or another failed version means stop; never `repair` through it. The explicit options allow
   pending versions from this release but reject applied versions absent from the checkout,
   including newer ones that Flyway ignores by default. These are recovery-only options, not
   changes to the service configuration.

   After that review, remove the failed row, check that the version is pending, and validate again:
   ```bash
   flyway repair
   flyway info
   flyway -ignoreFutureMigrations=false -ignoreMigrationPatterns='*:pending' validate
   ```
   That text-mode `validate` must exit 0. Only then replay the unchanged migration and validate
   with no pending versions ignored:
   ```bash
   flyway migrate
   flyway -ignoreFutureMigrations=false validate
   ```
8. **Verify before the restart.** `validate` passes, `SHOW CREATE TABLE` for each table the
   migration touched shows the intended final schema, and the rows are intact: compare row values
   and key invariants against the backup (checksums of the rows, or a diff of the primary keys and
   key columns), not just row counts. Then scale the service up to one replica and watch it start
   before resuming the other replicas, the analyzer, and the scheduler.

A crash can also leave no failed row with the whole migration already applied, because every
statement finished before the kill. The replay above can't handle that: running the script again
fails on the objects that exist, and the history can't be corrected with `repair`. Flyway
Community 8.5.13 has no supported command to record a migration as applied without running it; a
paid edition has one (`skipExecutingMigrations`), which this runbook doesn't cover. Don't edit the
history by hand or guess. Keep the writers stopped and have a DBA review the actual schema against
the script and decide the reconciliation, in writing, before anything else is run. If a migration
drops or rewrites data, only the backup can undo it.

## Database Privileges

The service's MySQL user needs at least these privileges on the optimizer's tables. It can also
read and write HouseTables' tables in a shared database, so the account's privileges are not a
safety boundary: migration review and the preservation test are.

| Tables | Privileges |
|--------|------------|
| `table_operations`, `table_operations_history`, `table_stats`, `table_stats_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, and `DROP` once a migration drops a table |
| `optimizer_schema_history` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `INDEX`: Flyway creates the table, then its index with `CREATE INDEX`. Without `INDEX`, it keeps the table and goes on without the index. |

Grant these before the service first starts; the optimizer tables don't exist until then. Write
index changes on the optimizer's tables as `ALTER TABLE`, which doesn't need `INDEX`.
