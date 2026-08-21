# Iceberg REST catalog

OpenHouse exposes a read-only Apache Iceberg REST Catalog facade for new clients while preserving
the existing OpenHouse APIs and business behavior.

## Enablement

The facade is disabled by default. Enable it with:

```properties
cluster.tables.iceberg-rest.enabled=true
```

`GET /v1/config` returns the `iceberg` route prefix and advertises only the implemented endpoints:

- `GET /v1/{prefix}/namespaces/{namespace}/tables`
- `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`
- `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`

## Architecture

The OpenAPI-generated interfaces own the HTTP contract. `IcebergRestCatalogController` is a thin
Spring MVC adapter, and `IcebergRestApiHandler` translates the Iceberg protocol to existing
`TablesApiHandler` and `OpenHouseInternalCatalog` behavior. The facade does not add business rules
or change the existing OpenHouse endpoints.

Iceberg response types use a narrowly scoped Spring `HttpMessageConverter`. Errors are translated
by controller-scoped advice into the standard Iceberg error envelope.

## Compatibility and limitations

- Only single-level namespaces are supported.
- The optional `warehouse` configuration hint does not select a different OpenHouse warehouse.
- List responses support opaque continuation tokens and page sizes from 1 through 1000.
- Table loads return all snapshots. The `snapshots=refs` projection is explicitly unsupported.
- The Iceberg 1.11 `referenced-by` query parameter is accepted and ignored.
- Access delegation may be requested, but this read-only version does not vend credentials.
- Conditional ETag responses are not currently emitted.
- Namespace, table-write, view, transaction, credential, and OAuth endpoints are not advertised.

Existing OpenHouse APIs remain supported. Client migrations can therefore be incremental.

## Observability and audit

Spring Boot records the facade through the standard `http.server.requests` metrics, including URI,
status, and latency. Table reads delegate through `TablesApiHandler`, retaining existing
authorization, lock visibility, and table-read audit behavior.

## Contract maintenance

`spec/iceberg-rest-catalog-open-api.yaml` is the full Apache Iceberg REST OpenAPI with OpenHouse
support declared per operation:

```yaml
operationId: listTables
x-openhouse-support: supported   # or unsupported
```

The build asserts every operation is annotated, codegens Spring interfaces only for `supported`
operations, and generates `IcebergRestOpenHouseSupport.SUPPORTED_ENDPOINTS` for `/v1/config`.
Marking a new operation `supported` (or changing a supported signature) fails compilation until the
facade implements it. New upstream operations without annotations fail the build.

To upgrade Iceberg OpenAPI: merge the newer upstream YAML into the checked-in file, keep/adjust
`x-openhouse-support` markers, update the pinned checksum in `services/tables/build.gradle`, then
compile.
