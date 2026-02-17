# Iceberg REST Catalog Implementation Plan (Subset, Contract-First)

## Objective

Add Iceberg REST Catalog support directly in OpenHouse so standard clients can work without custom adapters.

Phase 1 is intentionally read-only and targets immediate compatibility for:

- PyIceberg `load_table`
- PyIceberg `list_tables`
- PyIceberg `table_exists`
- Engines that use the same Iceberg REST read paths (DuckDB, Trino, Spark)

## Implementation (Current)

The phase-1 contract-first wiring is implemented as:

- Canonical spec:
  - `spec/openhouse-iceberg-rest-readonly-v1.yaml`
- Build/codegen/lint gate in `services/tables/build.gradle`:
  - `setUpOpenApiCliForIcebergRest`
  - `validateIcebergRestOpenApiSpec`
  - `generateIcebergRestOpenApiServer`
  - `compileJava` depends on generated interfaces
  - `check` depends on OpenAPI validation
- Controller contract implementation:
  - `services/tables/src/main/java/com/linkedin/openhouse/tables/controller/IcebergRestCatalogController.java`
  - Implements generated `IcebergReadOnlyApi`
  - Adds explicit `HEAD /v1/namespaces/{namespace}/tables/{table}` for `table_exists`
- Runtime compatibility coverage:
  - Java controller tests in
    `services/tables/src/test/java/com/linkedin/openhouse/tables/mock/controller/IcebergRestCatalogControllerTest.java`
  - PyIceberg smoke test in
    `integrations/python/dataloader/scripts/iceberg_rest_catalog_smoke.py`
- CI automation:
  - `.github/workflows/build-run-tests.yml` runs:
    - `./gradlew :services:tables:check`
    - PyIceberg smoke test script

Justification for each component:

- Canonical spec in-repo:
  - PR-reviewable contract changes and reproducible builds.
- Generated interfaces + controller implementation:
  - Compile-time enforcement that route/method/signature drift fails build.
- `check` dependency on spec validation:
  - Ensures OpenAPI syntax/contract checks are in the standard CI gate.
- Runtime smoke tests:
  - Validates wire behavior that compile-time checks cannot prove (`HEAD`, status behavior, client interoperability).

## Why This Plan

OpenHouse already has partial read-only Iceberg REST endpoints in:

- `services/tables/src/main/java/com/linkedin/openhouse/tables/controller/IcebergRestCatalogController.java`

But current contract enforcement is weak because:

- OpenAPI docs in `docs/specs/*.md` are generated documentation, not canonical source.
- Controllers are handwritten and not required to implement generated API interfaces.
- CI does not have a contract-specific gate for Iceberg REST behavior.

This plan moves to a contract-first model similar to Polaris:

1. Canonical OpenAPI in repo
2. Generated server API interfaces and models
3. Compile and runtime compatibility gates in CI

## Phase 1 Scope (Read-Only Only)

Implement and enforce only these Iceberg REST operations:

- `GET /v1/config`
- `GET /v1/namespaces/{namespace}/tables`
- `GET /v1/namespaces/{namespace}/tables/{table}`
- `HEAD /v1/namespaces/{namespace}/tables/{table}`

Reason for `HEAD`: stock PyIceberg uses it for `table_exists`.

## Phase 1 Non-Goals

Do not implement these in phase 1:

- `create_table`
- `drop_table`
- `purge_table`
- `rename_table`
- `commit_table`
- `create_table_transaction`
- `register_table`
- views APIs (`drop_view`, `list_views`, `view_exists`)
- namespace mutation APIs (`list_namespaces`, `create_namespace`, `drop_namespace`, `update_namespace_properties`, `load_namespace_properties`)

For unsupported Iceberg REST operations, return `501 Not Implemented` with Iceberg-style error payload.

## Contract Strategy

### 1) Spec as source of truth

- Add canonical OpenAPI file in repo:
  - `spec/openhouse-iceberg-rest-readonly-v1.yaml`
- Keep `docs/specs/*.md` as generated docs only.

Justification:

- Contract diffs are visible in PRs.
- Builds are reproducible and do not depend on remote spec availability.
- Branches/tags remain self-contained.

### 2) Server implements generated API interfaces

- Generate Spring server interfaces/models from canonical spec during build.
- Keep business logic in existing handlers/services.
- Make controller classes thin adapters that implement generated interfaces.

Justification:

- Compile-time contract enforcement for endpoint signatures, params, and DTO wiring.
- Clear separation of concerns:
  - generated API surface = contract
  - handwritten handlers/services = business logic

## CI Enforcement Strategy

### 3) `./gradlew check` as single contract gate

Wire `check` to include:

- OpenAPI generation task
- Java compile task for generated-interface implementation
- OpenAPI validation/lint task

Justification:

- `compileJava` alone only verifies shape/signature compatibility.
- Runtime HTTP behavior is not statically provable.
- `check` becomes the static contract gate, while runtime client compatibility remains a CI workflow gate.

## Why Both Static and Runtime Checks Are Required

### Static (compile-time) checks catch

- Missing interface implementation
- Signature mismatches
- Model type mismatches
- Route method contract mismatches at interface level

### Runtime checks catch

- HTTP status code behavior
- Required headers
- Error payload shape/content
- Serialized JSON field names on the wire
- `HEAD` behavior (`200/204/404` and empty response body semantics)
- Real client interoperability (PyIceberg against live service)

## Implementation Breakdown

### A) Spec and generation

1. Add canonical subset spec file:
   - `spec/openhouse-iceberg-rest-readonly-v1.yaml`
2. Add Gradle codegen task in `services/tables/build.gradle`:
   - generate server interfaces/models into `build/generated/openapi/iceberg-rest-server`
3. Add generated sources to `compileJava`
4. Ensure `compileJava` depends on codegen task

### B) Controller alignment

1. Update `IcebergRestCatalogController` to implement generated interfaces
2. Keep delegation to current logic:
   - `OpenHouseInternalCatalog`
   - `TablesService`
   - `TablesApiValidator`
3. Add explicit `HEAD` handler for table existence
4. Keep `IcebergRestExceptionHandler` as the only mapper for Iceberg REST error payloads

### C) Test coverage

1. Extend unit/controller tests:
   - `services/tables/src/test/java/com/linkedin/openhouse/tables/mock/controller/IcebergRestCatalogControllerTest.java`
2. Add tests for:
   - `HEAD` success and not-found
   - status/error payload correctness
3. Add Python compatibility smoke test using stock PyIceberg `RestCatalog`:
   - verifies `load_table`, `list_tables`, `table_exists`

### D) CI automation

1. Update `.github/workflows/build-run-tests.yml` to run:
   - `./gradlew :services:tables:check`
   - Python PyIceberg smoke test
2. Ensure PR workflow keeps this required via:
   - `.github/workflows/pr-validations.yml`

## Deliverables

- Canonical spec file for Iceberg REST subset
- Generated server API interfaces/models wired into build
- Controller implementing generated interfaces
- `HEAD` support for table existence
- OpenAPI validation and codegen wired into `check`
- Runtime compatibility tests wired into CI workflow
- CI gate proving compatibility

## Acceptance Criteria

- `./gradlew :services:tables:check` fails on contract drift.
- `./gradlew check` in CI enforces spec + compile + OpenAPI validation.
- CI workflow enforces runtime compatibility via PyIceberg smoke test.
- Stock PyIceberg can perform:
  - `load_table`
  - `list_tables`
  - `table_exists`
  without custom OpenHouse adapter logic.

## Follow-Up (Phase 2+)

After phase 1 stabilizes, add write paths incrementally:

- table creation/update/drop/rename/commit
- namespace APIs
- optional view APIs

Each addition must update canonical spec first, then generated interfaces, then controller/handler implementations, then compatibility tests.
