# Views Docker end-to-end tests

`ViewsDockerE2eTest` sends HTTP requests to the Tables Service on a random local
port. The service uses its real catalog and HTTP client to call a separate HTS
process in Docker, which persists to a separate Docker MySQL instance. The suite
does not use MockMvc, H2, mocked view services, or mocked persistence.

## Run

Use JDK 17 and a running Docker daemon. Run from the repository root:

```bash
./gradlew :services:tables:test --tests '*ViewsDockerE2eTest' \
  -PtableE2eBackend=docker \
  -PtableOpenApiPort=18000 -PhtsOpenApiPort=18001 \
  -x CopyGitHooksTask --console=plain --no-parallel --max-workers=2
```

On macOS, select JDK 17 with `JAVA_HOME=$(/usr/libexec/java_home -v 17)` before
the command if your default JVM is different. The OpenAPI ports are build-time
ports; runtime test ports are allocated dynamically. `CopyGitHooksTask` is
excluded because the existing task assumes `.git` is a directory, which is not
true in a linked worktree.

The existing `TableE2eContextInitializer` builds and copies the repository's HTS
boot JAR into `eclipse-temurin:17-jre`, starts `mysql:8.4.11`, and applies the
checked-in baseline and entity-discriminator DDL. Docker startup failures fail
the run; they never fall back to H2. No external deployment, production
credentials, or manually provisioned database is required.

## Coverage and independent persistence checks

The suite covers POST and PUT creation, GET, changed and unchanged replacement,
listing, deletion, repeated deletion, duplicate creation, stale updates,
concurrent creates and replacements, name reuse, case-insensitive identity,
namespace resolution, multiple SQL dialects, schema evolution, property merge,
table/view isolation and collisions, continuation tokens across HTS source
pages, invalid requests, exact schema/SQL byte limits, identifier limits, the
database feature gate, and missing bearer authentication.

Successful writes are checked against both typed `/hts/views` and neutral
`/hts/entities` reads, then against a direct JDBC query of MySQL's
`user_table_row`. Checks include the `VIEW` discriminator, metadata pointer,
storage type, creation time, numeric optimistic-lock version, and stable UUID
in the persisted metadata file. Rejected writes must leave the complete SQL
row unchanged or absent; pre-commit rejection and no-op cases also check that
no metadata files were allocated. Concurrent writers must produce exactly one
winner and one conflict.

The public `viewVersion` and HTS `tableVersion` are current metadata-location
tokens. MySQL's numeric `version` is the optimistic-lock counter; the legacy
`table_version` SQL column is unmapped and remains null. These are deliberately
asserted as separate facts, not treated as interchangeable versions.

Each test seeds an isolated database by creating a table through REST and
provisions its real `views` toggle rule through the shared MySQL fixture.
Cleanup reads the neutral HTS occupant and uses the correct typed DELETE, then
checks SQL absence. Spring context shutdown disposes of the containers and
network. Fixture cleanup is not counted as proof of REST lifecycle behavior.

## Boundaries

The Tables Service runs in the test JVM, but every suite request crosses its
real HTTP listener and every HTS request crosses the Docker network boundary.
The existing local dummy-JWT interceptor and dummy authorization handler are
used. Missing-token checks prove that local authentication is wired; they do
not claim to verify production JWT signatures, OPA policies, or external ACLs.
The existing test application stubs audit sinks, not the view execution path.

Supporting `Views*H2*`, managed-auth, and audit tests retained from the views
integration branch use explicit H2 configuration and may use MockMvc or mocks.
They remain useful supporting coverage but are not Docker persistence proof.
The Docker-only class is not executed with `-PtableE2eBackend=h2`; select
`docker` explicitly with the command above for end-to-end validation.

The existing `scripts/python/table_hts_integration_test.py` remains a separate
deployed-table smoke check. It is not a replacement for this suite's views and
direct-MySQL assertions.
