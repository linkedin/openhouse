# Managing namespace defaults

Tables Service exposes independent namespace metadata through four operations from the
[Apache Iceberg REST catalog specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
Use namespace properties for shared defaults that consumers explicitly opt into when resolving
effective table properties. Explicit table values win. Creating or changing defaults does not
rewrite tables, and ordinary table operations continue to work without namespace metadata.

The `prefix` is the client catalog name. A namespace contains exactly one name,
using 1 to 128 letters, digits, or underscores. Lookup is case-insensitive and responses retain
the name's insertion spelling. Creating `Finance` makes `FINANCE` refer to the same record.

| Method | Route | Success |
|---|---|---|
| POST | `/v1/{prefix}/namespaces` | 200 |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 200 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 204 |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 200 |

## Create and edit defaults

An administrator supplies a one-element `namespace` list and an optional `properties` map to
create a record. Loading returns the stored name and properties. A duplicate create fails
without changing the original spelling or values.

To edit defaults, send an `updates` map containing only the keys to set and a `removals` list
containing only the keys to remove. Either collection may be omitted. Updates preserve
unspecified keys. The response identifies updated keys, removed keys, and requested removal
keys that were already absent. A key appearing in both collections returns 422; correct the
overlap before resubmitting. Explicit null collections, null elements, and non-string values
are rejected.
Create, update, and removal keys must contain well-formed Unicode and at most 512 UTF-16 code
units. Empty keys, spaces, accents, case differences, and supplementary characters are preserved.
Unpaired UTF-16 surrogates and overlength keys return 400.

## Access and recovery

Creation, property updates, removals, and drop require `SYSTEM_ADMIN`. Loading metadata requires
`GET_TABLE_METADATA`. Defaults can affect every table whose consumer opts into effective-property
resolution, so table-writing permission alone cannot authorize changing or deleting defaults.
Authorization resolves the same resource for every capitalization of its name.

Invalid requests return 400 or 422; correct the request before retrying. Authentication failures
return 401, and insufficient privileges return 403. A missing namespace returns 404. Consumers
may treat that absence as having no defaults, but an unavailable dependency is an error, not
evidence that defaults are absent.

Conflicts require different actions. An existing name means creation did not replace its
metadata. A nonempty namespace must have its tables and views moved or removed before drop.
A concurrent property change requires reloading the current metadata and deciding whether
the intended update still applies.

An uncertain mutation returns `CommitStateUnknownException`. The operation may have committed
even though the response was lost or unusable. Reload metadata and reconcile the observed
identity and properties with the intended change before retrying. A missing response does not
mean a failed write, and replaying a sparse update can overwrite a later administrator's edit.
Namespace transport requests have one total attempt. Callers remain responsible for their own
retry decisions.

## Drop and table independence

Drop succeeds only for existing namespace metadata whose table and view listings establish
emptiness. An unavailable listing prevents deletion. Successful drop removes only the namespace
metadata, leaving table rows and files intact.

Concurrent table or view creation is not serialized with metadata drop. A table can therefore
exist after its namespace metadata is dropped. Namespace records are optional, not mandatory
parents. An empty namespace need not appear in the legacy table-derived database listing.

Administrators must deploy the HouseTables internal namespace API and apply the
[HouseTables namespace DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_namespace_metadata.sql)
before serving namespace requests. LinkedIn rollout also requires a separate `SYSTEM_ADMIN`
authorization-handler change. Existing table creation, loading, updates, deletion, and database
listing perform zero namespace-metadata lookups.

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
./gradlew :iceberg:openhouse:internalcatalog:test --tests '*HouseNamespace*Test' \
  :services:tables:test --tests '*TablesServiceNamespaceTest' --tests '*IcebergNamespaces*Test' \
  --tests '*NamespaceTableIsolationTest' --tests '*MainApplicationConfigTest' \
  :services:tables:namespaceIntegrationTest -x CopyGitHooksTask
```

The dedicated `namespaceIntegrationTest` task runs Tables HTTP handlers through the generated HTS
client against a live H2-backed HouseTables server. It is included in `check`. Its separate source
set keeps HouseTables bootstrap resources out of the existing Tables Service test classpath.
