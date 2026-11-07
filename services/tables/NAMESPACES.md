# Namespace metadata API

Tables Service exposes independent database metadata through four namespace operations from the
[Apache Iceberg REST catalog specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
The `prefix` is the client catalog name. A namespace contains exactly one logical database name,
using 1 to 128 letters, digits, or underscores. Lookup is case-insensitive.

| Method | Route | Success |
|---|---|---|
| POST | `/v1/{prefix}/namespaces` | 200 |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 200 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 204 |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 200 |

Creation accepts `{"namespace":["foo"],"properties":{"a":"1"}}`. Properties may be omitted.
Creation and load return `{"namespace":["foo"],"properties":{"a":"1"}}`.
The internal HouseTables UUID stays private.

Property updates accept `{"updates":{"b":"2"},"removals":["a","c"]}`.
Either collection may be omitted. Explicit null collections, null elements, and non-string values
are rejected. Updates preserve unspecified keys. Responses contain `updated`, `removed`, and
`missing` arrays. A key appearing in both collections returns 422.
Create, update, and removal keys must contain well-formed Unicode and at most 512 UTF-16 code
units. Empty keys, spaces, accents, case differences, and supplementary characters are preserved.
Unpaired UTF-16 surrogates and overlength keys return 400.

## Authorization and errors

The existing token interceptor and secured-method guard protect these routes. The service checks
database privileges before reading or mutating metadata: `CREATE_TABLE` for creation,
`GET_TABLE_METADATA` for load, `UPDATE_TABLE_METADATA` for updates, and `DELETE_TABLE` for drop.
The namespace authorization resource identifier is normalized with `Locale.ROOT` to lowercase,
matching HouseTables identity. The stored display name may retain its original case.
Database access denial returns 403 before looking up metadata, so reads cannot disclose properties
or existence to an unauthorized principal. The configured authorization handler controls access;
the reference OPA handler retains its existing permissive behavior when OPA is unconfigured.

Errors use `{"error":{"message":"...","type":"...","code":400}}`. Stack traces and upstream error
bodies are omitted. Validation and malformed JSON return 400, unauthenticated requests return 401,
missing metadata returns 404, duplicate names and concurrent mutations return 409, and unavailable
dependencies return 503. Unsupported ICEBERG HouseTables persistence also returns 503.
An uncertain mutation outcome returns 500 with `CommitStateUnknownException`. A nonempty
namespace uses `NamespaceNotEmptyException`, while a concurrent metadata conflict uses
`CommitFailedException`, even though both conflicts have status 409.
A namespace-scoped filter normalizes errors returned before controller execution. Existing APIs
retain their error format.

## Storage and compatibility

Database metadata is an independent HouseTables resource stored with its properties in `catalog_databases`.
Apply the [HouseTables database DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_database_metadata.sql)
before serving these requests. Existing table creation, loading, updates, deletion, and database listing
retain their existing behavior and perform zero database-metadata lookups.
An empty namespace need not appear in the legacy table-derived database listing.

Drop first checks metadata existence, then requests at most one table row and one view row from
HouseTables. These queries read identifiers and row metadata, without opening Iceberg metadata
files. Either entity type makes the namespace nonempty and returns 409. A failed listing stops
the operation. Successful drop deletes only independent database metadata.

The emptiness check and metadata deletion are separate operations. Concurrent table or view
creation can occur between them, leaving an entity whose database metadata has been removed.
Existing table and view lifecycles are deliberately unchanged, and namespace drop never deletes
their rows or files.

The generated HouseTables adapter uses a 30-second budget for each read and a 60-second budget
for each write. Automatic transport retries are disabled for `DatabaseApi` requests.
Occupancy reads through `UserTableApi` retain their configured transport retries.
An uncertain mutation outcome returns HTTP 500 with `CommitStateUnknownException`; callers must
reload metadata and reconcile before retrying. This transport setting does not control retries
in external clients.

Internal HouseTables metadata responses require `databaseUuid`, `databaseName`, and `properties`;
update responses require `updated`, `removed`, and `missing`. The decoder checks presence before
generated defaults can conceal omissions. Empty maps and lists are valid. Deletion succeeds only
after HTTP 204 from HouseTables; other successful or redirect statuses leave its outcome uncertain.
Conflict reasons come from the operation: duplicate creation uses `AlreadyExistsException`,
while concurrent updates and deletes use `CommitFailedException`, regardless of the route spelling.

## Generated client and validation

The stable tag is `Namespace`. The generated `NamespaceApi` methods are:

```java
Mono<NamespaceResponse> createNamespaceV1(String prefix, CreateNamespaceRequest request);
Mono<NamespaceResponse> loadNamespaceMetadataV1(String prefix, String namespace);
Mono<Void> dropNamespaceV1(String prefix, String namespace);
Mono<UpdateNamespacePropertiesResponse> updateNamespacePropertiesV1(
    String prefix, String namespace, UpdateNamespacePropertiesRequest request);
```

Each method also has a `WithHttpInfo` variant. Generate and compile the client through Gradle;
generated files are build outputs.

```bash
./gradlew :client:tableclient:compileJava -x CopyGitHooksTask
./gradlew :iceberg:openhouse:internalcatalog:test --tests '*HouseDatabase*Test' \
  :services:tables:test --tests '*IcebergNamespaces*Test' --tests '*NamespaceTableIsolationTest' \
  :services:tables:namespaceIntegrationTest -x CopyGitHooksTask
```

The dedicated `namespaceIntegrationTest` task runs Tables HTTP handlers through the generated HTS
client against a live H2-backed HouseTables server. It is included in `check`. Its separate source
set keeps HouseTables bootstrap resources out of the existing Tables Service test classpath.
Java catalog namespace support and effective-property resolution are subsequent client work.
