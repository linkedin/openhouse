# Views HTTP integration tests

`ViewsHttpIntegrationTest` exercises the complete views execution path:

```text
JUnit HTTP client -> random-port Tables -> random-port HTS -> Testcontainers MySQL
                             |
                             +-> real local metadata files
```

Both Spring servers run in the test JVM. Requests cross real HTTP listeners;
neither the catalog nor HTS persistence is mocked. MySQL uses the same
`HtsMysqlDatabase` test fixture as the HTS integration tests, not a separate
Compose recipe or Python runner.

## Running

Use JDK 17 and a running Docker daemon, then run from the repository root:

```bash
./gradlew :services:tables:test --tests '*ViewsHttpIntegrationTest'
```

The suite is included in the ordinary `:services:tables:test` task and therefore
does not require a backend flag, manually started stack, or separate CI job.
Docker or database startup failures fail the run; there is no H2 fallback.
The existing Tables H2 and mocked tests retain their original configuration.
Test contexts use non-global Reactor resource factories so closing a cached
context cannot terminate another context's active HTS HTTP client.

To check both affected modules, including all existing suites:

```bash
./gradlew :services:housetables:test :services:tables:test
```

In linked Git worktrees, add `-x CopyGitHooksTask`: the existing hook-copy task
assumes `.git` is a directory. On macOS, select JDK 17 with
`JAVA_HOME=$(/usr/libexec/java_home -v 17)` if necessary.

## Shared setup and isolation

The shared fixture starts the MySQL image selected by `mysqlTestVersion` in
`gradle.properties` (passed to every test JVM as `openhouse.test.mysql.version`;
the fixture fails fast if it is unset) once per test JVM/classloader and
applies all canonical `services/housetables/ddl/*.sql` in sorted filename order.
Gradle publishes those DDL resources with the fixture; a missing resource set
fails startup. This selects the shared test image version; it does not assert
parity with a production MySQL version. Spring SQL initialization, Hibernate
schema generation, and certificate-based datasource authentication are disabled before HTS starts.
The suite never widens columns or otherwise alters the canonical schema.

HTS tests retain their existing before-each truncate-and-seed lifecycle.
Views tests use isolated, short database names, create seed tables through
Tables REST, and provision only their own feature-toggle rules through JDBC.
They do not load either module's generic `data.sql`. After each case, cleanup
uses typed HTS DELETE and checks physical SQL absence, then removes its toggle
rules. Spring shutdown closes HTS and removes the suite's metadata directory;
Testcontainers owns MySQL cleanup. Tests run serially, not concurrently against
a database being reset by another test. Concurrent-write scenarios explicitly
join their own workers before cleanup.

The suite uses a short temporary root (`/tmp` on POSIX) and short database names
so the 128-character view-ID boundary fits the canonical 255-character metadata
location column. This does not fix or cover arbitrary long storage prefixes.

## Coverage and assertions

Cases cover create/get/replace/list/delete, PUT creation, repeated deletion,
no-op writes, stale versions, competing creates and replacements, pagination,
case-insensitive identity, database isolation, table/view collisions and name
reuse, feature gates, missing bearer authentication, invalid requests, SQL/schema
byte limits, identifier limits, schema evolution, property snapshots, multiple
dialects, and namespace metadata pass-through.
Exhaustive validation permutations remain in `ViewsValidatorTest`; the HTTP
suite keeps representative rejection cases and distinct persistence boundaries.

Assertions compare Tables responses with typed `/hts/views` and neutral
`/hts/entities` reads, direct JDBC snapshots of `user_table_row`, and parsed
metadata files. They check the discriminator, pointer, storage type, timestamps,
optimistic-lock counter, UUID, and metadata semantics. Rejected/no-op operations
must leave rows unchanged or absent; pre-commit rejections and no-ops must not
allocate metadata files. These persistence checks are distinct from validator
and mocked-service coverage.

Requests provide `openhouse.view.sourceDialect` in `viewProperties`. Replacement
requests preserve all server-managed properties and treat user properties as a
complete replacement snapshot. Public `viewVersion` and HTS `tableVersion` are
metadata-location tokens; MySQL's numeric `version` is the CAS counter, while
the legacy SQL `table_version` column remains null.

## Boundaries

This tests real HTTP and MySQL persistence, not packaged Docker images,
entrypoints, deployment wiring, remote storage, or production credentials.
The Tables test application uses dummy JWT/authorization and stubbed audit
sinks; missing-token cases check that local authentication is wired, not JWT
signatures or OPA policy. HTS's classpath-only default Spring Security
autoconfiguration is excluded for its unauthenticated test endpoint.

The historical `e2e.h2` package and `SpringH2Application` bootstrap are reused,
but this suite disables Tables JPA repositories and routes catalog operations
through the real HTS HTTP client to MySQL. Existing H2 tests remain supporting
coverage and are not presented as MySQL integration proof.
