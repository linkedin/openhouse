# Views HTTP integration tests

`ViewsHttpIntegrationTest` exercises real HTTP requests through random-port Tables
and HTS servers to Testcontainers MySQL, with local metadata files. Both servers
run in the test JVM; the suite uses no MockMvc or mocked persistence.

## Run

The suite runs as part of the ordinary Tables test task and requires JDK 17 and
Docker:

```bash
./gradlew :services:tables:test --tests '*ViewsHttpIntegrationTest'
```

In linked worktrees, add `-x CopyGitHooksTask` because the task assumes `.git` is
a directory. To run both affected modules, use
`./gradlew :services:housetables:test :services:tables:test`.

## Setup and coverage

The suite shares the HTS MySQL fixture and canonical, filename-sorted DDL. The
image version comes from `mysqlTestVersion` in `gradle.properties`, passed as
`openhouse.test.mysql.version`; missing configuration fails fast. This is a test
version, not a claim about production. Spring SQL initialization and Hibernate
schema generation are disabled for HTS; the test schema is not altered.

Cases cover view lifecycle and metadata, replacement and concurrency behavior,
pagination across HTS source pages, table/view isolation, database-scoped feature
gates, authentication, and representative invalid requests. Assertions compare
Tables responses with HTS reads, direct MySQL rows, and metadata files. Rejected
and no-op operations are checked for unintended persistence changes.

Tests use isolated databases and metadata paths, clean up through typed HTS
operations, and run serially against the shared database. SQL initialization is
disabled in test resources so transitive HTS scripts are not loaded by Tables'
H2 tests. The historical `e2e.h2` package name does not indicate this suite uses
H2.

## Boundaries

This verifies the real Tables-to-HTS HTTP and MySQL persistence path, not
packaged images, deployment wiring, remote storage, production authentication,
or external audit delivery. Tests use dummy authentication and stub audit sinks.
The short metadata path supports the maximum view-ID length within the canonical
MySQL column size; arbitrary long storage prefixes are not covered.
