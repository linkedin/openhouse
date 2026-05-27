# Optimizer subsystem

Continuous table optimizer for OpenHouse Iceberg tables. The analyzer issues PENDING
operation rows per table on a cadence; the scheduler claims those rows, packs them into
bins, and submits one Spark job per bin to the Jobs Service. Analysis and scheduling
state lives in the optimizer DB (`table_operations`, `table_operations_history`,
`table_stats`, `table_stats_history`).

## Layout

Libraries (logic):

- `services/optimizer/` — REST service: `api/spec/` DTOs, controllers, `model/` DTOs,
  `db/` JPA entities, `repository/`, `service/`. Owns the schema in
  `src/main/resources/db/optimizer-schema.sql`.
- `services/optimizer/analyzer/` — analyzer library: `AnalyzerRunner`, the
  `OperationAnalyzer` strategy interface, `CadencePolicy`, per-operation analyzers
  (e.g. `CadenceBasedOrphanFilesDeletionAnalyzer`).
- `services/optimizer/scheduler/` — scheduler library: `SchedulerRunner`, `BinPacker`,
  `Bin`, `SchedulingCandidate`, `JobsServiceClient`.

Deployable Spring Boot wrappers (entry points only):

- `apps/optimizer/analyzerapp/` — `AnalyzerApplication` + `application.properties`.
- `apps/optimizer/schedulerapp/` — `SchedulerApplication` + `application.properties`.

The REST service (`services/optimizer/`) is itself a runnable Spring Boot app
(`OptimizerServiceApplication`); it does not have an `apps/` wrapper.

## No internal-LinkedIn references

This is an OSS subsystem. Source and docs under `services/optimizer/` and
`apps/optimizer/` must not contain: internal ticket IDs (`BDP-XXXX`, etc.),
`linkedin.atlassian.net` URLs, internal team names, internal usernames, internal
Confluence or wiki links, internal Slack channels, internal email addresses, or
LinkedIn-prod-specific table or service identifiers. Generic references to the public
`linkedin/openhouse` GitHub repo and the published docs at `linkedin.github.io/openhouse`
are fine.

## Library / deployable split

Library modules disable `bootJar` and publish a plain `jar`; deployable apps depend on
the library via `implementation project(':services:optimizer:<lib>')`.

Library `build.gradle` shape:

```groovy
bootJar { enabled = false }
jar { enabled = true; archiveClassifier = '' }

dependencies {
  api project(':services:optimizer')
  // ...
}
```

App `build.gradle` shape:

```groovy
dependencies {
  implementation project(':services:optimizer:analyzer') // or :scheduler
  // ...
}
```

The `@SpringBootApplication` class lives only in the app wrapper. Library modules carry
no `main` method.

## Package convention

Everything is under `com.linkedin.openhouse.optimizer.*`. The library and its deployable
share the same root package, which is what lets Spring component-scan from the app's
main class pick up the library's `@Component` / `@Service` beans without extra
`@ComponentScan` configuration. Do not introduce a deeper sub-package just because the
directory nests — `apps/optimizer/analyzerapp/.../analyzer/AnalyzerApplication.java`
sits in the same package as `services/optimizer/analyzer/.../analyzer/AnalyzerRunner.java`.

JPA wiring is centralized in the app classes:

```java
@SpringBootApplication
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
```

## `table_operations` concurrent-instance contract

Multiple `PENDING` (or `SCHEDULING` / `SCHEDULED`) rows for the same
`(tableUuid, operationType)` are intentional and allowed. The only primary key on
`table_operations` is `id`. There is no uniqueness constraint on
`(tableUuid, operationType)`, by design.

Two analyzer instances can race and both insert a PENDING row for the same table; that
is fine. Dedup is performed per scheduling cycle inside
`SchedulerRunner.cancelDuplicates`: for each `tableUuid` with more than one PENDING row,
the oldest survives (lex-tiebreak on `id`) and the rest are cancelled via
`TableOperationsRepository.cancel`. Cancellation is gated on `status = PENDING` so a row
another instance has already claimed is never dropped.

Do not add a unique index on `(tableUuid, operationType)`. Discuss before changing this
contract.

## Pagination caveat (analyzer + scheduler)

Aligned per-page pagination across the analyzer's three pre-load reads — current ops,
latest history, tables — is incorrect. Page N of one query has no relationship to page N
of the others, so keyed lookups by `tableUuid` mostly miss, the maps look empty, and the
analyzer issues duplicate PENDING rows. The codebase uses `Pageable.unpaged()` for these
reads (`AnalyzerRunner.analyzeDatabase`) and for the scheduler's PENDING load
(`SchedulerRunner.schedule`). The working set is bounded by tables-per-database
(analyzer) or count of PENDING rows for the operation type (scheduler).

If memory ever needs to be bounded here:

- Paginate the primary list (tables for the analyzer, PENDING ops for the scheduler).
- Then either (a) load the joined maps unbounded, or (b) batch-lookup the maps by
  `tableUuid IN <page-of-tableUuids>`.
- Do not reintroduce the broken aligned-page pattern.

## Property naming

`kebab-case` keys, module-prefixed where applicable. One default per key, set via
`${ENV_VAR:fallback}` if it should be overridable at runtime.

Examples:

```
ofd.success-retry-hours=16
ofd.failure-retry-hours=1
scheduler.ofd.max-files-per-bin=${SCHEDULER_OFD_MAX_FILES_PER_BIN:1000000}
scheduler.results-endpoint=${SCHEDULER_RESULTS_ENDPOINT:http://openhouse-optimizer:8080/v1/optimizer/operations}
scheduler.cluster-id=${SCHEDULER_CLUSTER_ID:LocalHadoopCluster}
jobs.base-uri=${JOBS_BASE_URI:http://localhost:8002}
```

Per-operation property convention: prefix with the operation short-name. OFD uses
`ofd.*` (analyzer) and `scheduler.ofd.*` (scheduler). New operations follow the same
shape (`<op>.*` and `scheduler.<op>.*`).

Table-level opt-in is a table property, not a server config:
`maintenance.optimizer.<op>.enabled=true` (e.g. `maintenance.optimizer.ofd.enabled`).
This piggybacks on the existing `maintenance.*` prefix and lives on the table itself.

## REST error contract

Controllers throw `org.springframework.web.server.ResponseStatusException(status,
reason)`. `application.properties` sets `server.error.include-message=always` so the
reason reaches the wire (Spring Boot 2.7 omits the `message` field by default).

Example:

```java
.orElseThrow(() -> new ResponseStatusException(
    HttpStatus.NOT_FOUND, String.format("no operation with id %s", id)));
```

Do not introduce a custom global `@ControllerAdvice` exception handler or `ApiError`
DTO unless there is a concrete reason the codebase can't grow into. That was tried; it
duplicated Spring's defaults and was removed.

`MockMvc` does not trigger Spring's error-dispatch path, so the response body of a
`ResponseStatusException` is empty in tests even though it's populated in production.
Assert on status code, not body, in controller tests.

## `@ApiResponses` convention

Annotate each endpoint with the codes it actually returns. Use the form
`"Resource ACTION: STATUS"` for descriptions:

```java
@ApiResponses(value = {
  @ApiResponse(responseCode = "201", description = "Operation UPDATE: CREATED"),
  @ApiResponse(responseCode = "400", description = "Operation UPDATE: BAD_REQUEST"),
  @ApiResponse(responseCode = "404", description = "Operation UPDATE: NOT_FOUND")
})
```

Do not list `500`. Do not list codes the endpoint cannot return.

## `Optional<T>` over nullable

All optional parameters in repository and service signatures are `Optional<T>`, never
bare nullable. This includes filter parameters on `find(...)` methods. Example
(`TableOperationsRepository.find`): every filter — `operationType`, `status`,
`tableUuid`, `databaseName`, `tableName`, `scheduledAt`, `ids` — is `Optional<T>`.

Spring Data's `@Query` doesn't unwrap `Optional`. Bridge with a `default`-method facade
that takes `Optional<T>` and dispatches to a nullable internal `@Query`-annotated
method:

```java
default List<TableOperationsRow> find(Optional<OperationType> operationType, ...) {
  return findInternal(operationType.orElse(null), ...);
}

@Query("SELECT r FROM ... WHERE (:operationType IS NULL OR r.operationType = :operationType) ...")
List<TableOperationsRow> findInternal(@Param("operationType") OperationType operationType, ...);
```

Same shape applies for `updateBatch` / `updateBatchInternal`. `List<T>` parameters can't
share an `:ids IS NULL OR r.id IN :ids` pattern (Hibernate expands the list inline and
the `IS NULL` check turns ungrammatical) — branch in the `default` method on
`ids.isPresent()` and dispatch to two separate `@Query` methods.

## Tests run on H2 in MySQL mode; production is MySQL

Test resources set `jdbc:h2:mem:...;MODE=MySQL`. Production is MySQL 8 (see
`spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver` and the MySQL8 dialect in
`services/optimizer/src/main/resources/application.properties`). Schema is shared:
`db/optimizer-schema.sql`, executed via `spring.sql.init.schema-locations` on both.

Watch for behavior that differs across the two:

- `@Modifying` queries use `flushAutomatically = true, clearAutomatically = true` so
  pending writes hit the DB before the modifying query runs and the persistence context
  is cleared after. Required for the SchedulerRunner's
  `updateBatch` + re-`find` claim pattern to see its own writes.
- Timestamp precision: MySQL `TIMESTAMP(6)` stores microseconds; H2 and `Instant.now()`
  on macOS may carry nanoseconds. Round-tripping reads back at lower precision and
  breaks equality. Use `Instant.now().truncatedTo(ChronoUnit.MICROS)` when constructing
  test fixtures that are later compared to repository-loaded rows. The pattern is in
  `TableOperationsRepositoryTest`.

Any new `@Query` should be sanity-checked against MySQL 8 — particularly anything using
`IN`, `IS NULL`, `COALESCE`, or window functions.

## Entry-point pattern (analyzer / scheduler)

Both deployable apps follow the same shape: a `CommandLineRunner` bean iterates the
registered strategies once per process invocation. The process is expected to be driven
externally (cron, scheduler, etc.) — there is no internal scheduling loop.

Analyzer:

```java
@Bean
public CommandLineRunner run(AnalyzerRunner runner, List<OperationAnalyzer> analyzers) {
  return args -> analyzers.forEach(a -> runner.analyze(a.getOperationType()));
}
```

Scheduler:

```java
@Bean
public CommandLineRunner run(SchedulerRunner runner, Map<OperationTypeDto, BinPacker> binPackers) {
  return args -> binPackers.keySet().forEach(runner::schedule);
}
```

Adding a new operation type:

1. Add a value to `OperationTypeDto` / `OperationType` / api-spec `OperationType` and
   wire their `toDb` / `fromDb` / `toModel` conversions.
2. Implement an `OperationAnalyzer` in `services/optimizer/analyzer/` and register it as
   a `@Component`. The analyzer app picks it up via the `List<OperationAnalyzer>`
   injection.
3. Implement a `BinPacker` in `services/optimizer/scheduler/` and register it as a
   `@Component` keyed by its `OperationTypeDto` (the scheduler app injects the
   `Map<OperationTypeDto, BinPacker>`).
4. Add any operation-specific table-properties opt-in flag under
   `maintenance.optimizer.<op>.*` and any tuning knobs under `<op>.*` (analyzer) /
   `scheduler.<op>.*` (scheduler).
