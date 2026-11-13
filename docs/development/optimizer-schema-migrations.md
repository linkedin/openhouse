# Optimizer Schema Migrations

The Optimizer Service runs [Flyway](https://documentation.red-gate.com/fd) migrations when it
starts. These migrations create and update its MySQL tables. Each migration is handwritten SQL,
committed with its entity change.

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

The optimizer repository, service, and scheduler integration tests run on MySQL 8.0.46, the release
production runs, in Docker, so start Docker before you run them. The test modules include Flyway on
their test runtime classpath. The shared `optimizer-mysql-testcontainers.properties` (optimizer-common
test fixtures) points the optimizer's datasource at a Testcontainers
[JDBC URL](https://java.testcontainers.org/modules/databases/jdbc/), `jdbc:tc:mysql:8.0.46:///...`,
which starts a [MySQL container](https://java.testcontainers.org/modules/databases/mysql/) at the
first connection and stops it when the context closes its datasource. Flyway migrates the database
as the service does at startup. The URL carries a UUID that Spring resolves once per context, so
each distinct test context gets a container of its own. The URL's `socketTimeout=60000` bounds the
application's JDBC connections, not Testcontainers' own readiness check, which uses its defaults. Add
the properties to a `@SpringBootTest`:

```java
@SpringBootTest
@TestPropertySource(locations = "classpath:optimizer-mysql-testcontainers.properties")
class MyRepositoryTest { ... }
```

`OptimizerSchemaMigrationsTest` starts the service as a MySQL user that can access only the four
optimizer tables and `optimizer_schema_history`. A migration that reads or changes any other table
fails, and so does the startup. This includes `DROP TABLE IF EXISTS` on a table that doesn't exist.
The test starts the service on an empty database, and on one that holds the real HouseTables schema
with seeded rows. Both must end up with the same optimizer tables, and HouseTables' table
definitions and rows must not change.

Hibernate validates the schema after each migration run, so a mapped table or column missing from
the migrations, or an incompatible column type, fails the test. Validation does not check indexes,
nullability, column lengths, or extra database objects.

The test grants access through its own `OPTIMIZER_TABLES` list, not one derived from the
migrations. Add each new optimizer table to it. Never replace it with a database-wide grant.

This check runs before deployment. It does not isolate production tables. It cannot cover SQL
paths that depend on production-only data or configuration. It cannot protect a deployment that
skips the checked migrations. Production credentials are unchanged. Review the SQL and require
`:services:optimizer:test` to pass before deploying.

The same test also starts the service on another database with Flyway off and Hibernate's
`ddl-auto=create` on. Hibernate creates the tables the entities define there. Each table's
`SHOW CREATE TABLE` must match the migrated table line for line, so this check covers indexes,
nullability, column lengths, and extra tables. Column order doesn't count. An entity change without
its migration fails the test. So does a migration without its entity change. Neither check covers
triggers or stored routines; review those in each migration.

## Changing the Schema

1. Write the migration in `libs/optimizer/optimizer-common/src/main/resources/db/migration/`. Name
   it `V<UTC timestamp>__<description>.sql`. An example is `V20261020000000__add_job_attempts.sql`.
   Flyway applies migrations in version order. A timestamp keeps two branches from taking the same
   version.
2. Edit the entities in `com.linkedin.openhouse.optimizer.db` to match. Declare what Hibernate can't
   infer. Set a column type with `columnDefinition`, as the `TIMESTAMP(6)` columns do. Set a default
   with `@ColumnDefault`.
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
4. Run the tests. `OptimizerSchemaMigrationsTest` fails until the entities and the migrations define
   the same tables:
   ```bash
   ./gradlew :libs:optimizer:optimizer-common:test :services:optimizer:test
   ```
5. Commit the entity change and the migration together.

Another change may merge a migration with a later version first. Keep your timestamp. The service
sets `spring.flyway.out-of-order=true`, so Flyway applies your migration on a database that already
has the later one. Databases can therefore apply the two in either order. If your migration depends
on another, give it a version after that dependency and test the upgrade from the deployed schema.

CI compares the migration directory with the PR's base commit. This includes stacked PRs. It fails
if the PR edits, deletes, or renames a migration that is in the base. Merged migrations are
immutable even before they are deployed: Flyway refuses to start where an applied migration's file
changed. To change the schema, add a new migration. To check your committed branch changes locally,
compare from the merge base with your fetched base ref. For a PR against `main`:

```bash
git diff --exit-code --stat --diff-filter=DMRT linkedin/main...HEAD -- \
  libs/optimizer/optimizer-common/src/main/resources/db/migration
```

Flyway rejects two migrations with the same version. The service also sets
`spring.flyway.validate-migration-naming=true`, so Flyway rejects a file in `db/migration` whose name
it can't parse instead of skipping it. `:services:optimizer:test` fails on either.

## What Happens at Startup

Spring Boot runs Flyway before JPA starts. Flyway applies pending migrations in version order.
It records each applied migration in `optimizer_schema_history`. A MySQL named lock makes replicas
take turns so that each migration runs once. Hibernate then validates the tables against the
entities (`spring.jpa.hibernate.ddl-auto=validate`) without changing them. The HTTP port opens
after the migrations finish.

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

A rolled-back release has no files for the migrations applied after it. The service sets
`spring.flyway.ignore-migration-patterns=*:future,*:missing`, so validation skips them. This
includes a late migration whose version is below the release's newest. The release still starts
against the newer schema. Validation skips a deleted migration file the same way. CI rejects
deletions instead.

The analyzer and scheduler do not run migrations. They validate the tables the service migrated.
Roll out the service first.

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

MySQL commits each DDL statement on its own. A failed migration leaves in place every statement that
ran before the failure. Flyway doesn't roll them back.

A killed client doesn't tell you what ran. The statement it was running may still be running. It may
have finished. It may have been undone. A killed pod may never have written the failed row. So the
database can hold all or some or none of the migration's changes. This is true whatever the history
table says.

A missing `success = 0` row doesn't mean nothing changed. Don't restart the service to make Flyway
record one. That is not a recovery step. It reruns the migration against the half-applied schema.

Flyway's `repair` does not undo database changes. In 8.5.13 it can:

- Delete failed rows (`success = 0`) from `optimizer_schema_history`.
- Update the recorded checksum to match the supplied migration files. It also updates the
  recorded description and type.
- Mark applied rows as deleted when their migration files are missing.

It does not change application tables or their data.

Only run `repair` with the exact migration files that produced the history. An edited file can
make `repair` record a change that never ran. An older migration set can make it hide an applied
migration. Never use `repair` to bypass a checksum mismatch or a missing migration. Resolve those
with a new migration or a reviewed decision.

Recover as a MySQL admin. Follow these steps in order:

1. **Stop the writers.** Scale the service to zero replicas. Then no replica can start Flyway again
   and take the migration lock. Pause the analyzer and scheduler apps. Pause anything else that
   writes the optimizer tables. Rolling the service back doesn't help. Every release with this
   migration fails the same way. Don't start recovery while a replica may still be restarting.
2. **Find out what is running.** A statement from the killed migration can still be running in
   MySQL. It may be blocked on `Waiting for table metadata lock`. Let it finish.
   Don't stop it with `KILL`. That isn't guaranteed to leave the schema as it was.
   ```sql
   SELECT id, time, state, info FROM performance_schema.processlist
   WHERE db = DATABASE() AND command = 'Query' AND id <> CONNECTION_ID();
   ```
3. **Back up the current state and verify the pre-deploy backup.** Take a current backup to preserve
   the half-applied state and any data written since. Also have a tested restore of the last backup
   from before the deploy. Check that the restore works before relying on it.
4. **Inspect the real state.** Read the history table and the actual schema:
   ```sql
   SELECT installed_rank, version, description, checksum, success
   FROM optimizer_schema_history ORDER BY installed_rank;
   SHOW CREATE TABLE <table>;  -- for each table the migration touches
   ```
   Compare the result with the failed migration script one statement at a time. Find the script
   in `libs/optimizer/optimizer-common/src/main/resources/db/migration/` in the release that ran
   it. Find out why the statement failed from the database's error message. The cause is usually in
   the environment. Examples are rows that break a new unique index and an object that already
   exists.
5. **Fix the external cause with the data's owners.** Don't change the migration file. CI forbids
   editing any merged migration. A recorded checksum has to stay valid. Remove or
   correct the conflicting data or object only after review. Do it only after the backup. An invalid
   script needs a separate recovery plan. Invalid SQL is one example. No environment change will
   make it pass. Review and approve a recovery plan for that case. Do not use the replay below.
6. **Revert what the migration already did.** Revert only that. Undo each statement that took
   effect in reverse order. First check with `SHOW CREATE TABLE` that the object is the one the
   script created. Don't drop a column or index on a guess. An earlier migration or a later hand
   change may own it. Some changes can't be reverted here. These are changes that are not purely
   additive such as a dropped or rewritten column or table. Restore those from the backup and tell
   the data's owners. The goal is exactly the state the script was written for. Then the unchanged
   script can run again from its first statement. Don't edit `optimizer_schema_history` by hand.
7. **Validate with Flyway and replay the migration.** Run Flyway 8.5.13 against the complete
   migration directory of the release that ran the failed migration. That means every file in
   `db/migration/` of that release unedited. Check out its tag or commit. Record which one you
   used. Use the same history table and baseline settings as the service. Don't assume a Flyway CLI
   is installed. The commands below run Flyway from the pinned image in the checkout. They read the
   password from the shell's environment and not the command line. The preflight is the validation
   that runs before any repair. It checks the history against the migration files. Run it first.
   Don't go on to `repair` automatically:
   ```bash
   export FLYWAY_PASSWORD   # the admin's password, set without echoing it or writing it to a file
   flyway() {  # flyway <command>
     docker run --rm \
       -e FLYWAY_URL='jdbc:mysql://<host>:3306/<optimizer database>' \
       -e FLYWAY_USER='<admin user>' -e FLYWAY_PASSWORD \
       -e FLYWAY_TABLE=optimizer_schema_history \
       -e FLYWAY_BASELINE_ON_MIGRATE=true -e FLYWAY_BASELINE_VERSION=0 \
       -e FLYWAY_OUT_OF_ORDER=true -e FLYWAY_VALIDATE_MIGRATION_NAMING=true \
       -e FLYWAY_CLEAN_DISABLED=true \
       -e FLYWAY_LOCATIONS=filesystem:/flyway/sql \
       -v "$PWD/libs/optimizer/optimizer-common/src/main/resources/db/migration:/flyway/sql:ro" \
       flyway/flyway:8.5.13 "$@"
   }
   flyway info
   flyway -ignoreFutureMigrations=false -ignoreMigrationPatterns='*:pending' -outputType=json validate
   ```
   `info` does not compare checksums. Inspect `validationSuccessful` and every entry in
   `invalidMigrations`. **Flyway 8.5.13 JSON validation exits 0 even when validation fails.**

   Continue only in one of these cases:
   - Validation succeeds. There may be no failed history row.
   - The only invalid entry is the failed version with `FAILED_VERSIONED_MIGRATION`.

   Stop on any of these. Never `repair` through them:
   - a checksum mismatch
   - a missing migration
   - another failed version

   The explicit options allow pending versions from this release. They reject applied versions that
   are absent from the checkout. The service ignores those so that a rolled-back release starts.
   These options are for recovery only. They are not changes to the service configuration.

   Remove the failed row only after this review. Check that the version is pending. Validate again:
   ```bash
   flyway repair
   flyway info
   flyway -ignoreFutureMigrations=false -ignoreMigrationPatterns='*:pending' validate
   ```
   This text-mode `validate` must exit 0. Only then replay the unchanged migration. Then validate
   with no pending versions ignored:
   ```bash
   flyway migrate
   flyway -ignoreFutureMigrations=false validate
   ```
8. **Verify before the restart.** Check all of these:
   - `validate` passes.
   - `SHOW CREATE TABLE` for each table the migration touched shows the intended final schema.
   - The rows are intact. Compare row values and key invariants against the backup. Use checksums
     of the rows or a diff of the primary keys and key columns. Row counts alone are not enough.

   Then scale the service up to one replica and watch it start. After that resume the other
   replicas. Then resume the analyzer and the scheduler.

A crash can also leave no failed row with the whole migration already applied. This happens when
every statement finished before the kill. The replay above can't handle this case. Running the
script again fails on the objects that already exist. `repair` can't correct the history either.
Flyway Community 8.5.13 has no supported command to record a migration as applied without running
it. A paid edition has one (`skipExecutingMigrations`). This runbook doesn't cover it.

Do not edit the history by hand. Do not guess. Keep the writers stopped. Have a DBA review the
actual schema against the script. The DBA must approve a plan in writing to make the history match
the schema before any more commands run. Only a backup can undo dropped or rewritten data.

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
