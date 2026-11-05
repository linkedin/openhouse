# Java namespace metadata and effective table properties

Both packaged Java catalogs, for Iceberg 1.2 and 1.5, expose the same opt-in API:

```java
Map<String, String> effective = catalog.getEffectiveTableProperties(table);
```

The method is public on `OpenHouseCatalog` and declares `NamespaceMetadataException`.
It performs one explicit, uncached namespace metadata request. Database properties supply
defaults; actual table properties override matching keys. The returned map is an immutable
defensive copy. Calling `table.properties()` still returns the actual table properties.
Existing table operations, including `loadTable`, retain their existing request counts.

The checked exception makes remote I/O and its expected failures explicit to callers.
Only HTTP 404 from the namespace lookup means that the database has no inherited properties.
Authorization failures, connection failures, timeouts, malformed responses, and other service
errors fail the call. Reads have a 30-second budget; mutations have a 60-second budget.
Interruption cancels the request and preserves the thread's interrupted status.
Requests use the existing generated client and its authentication, configured headers,
connection strategy, and TLS configuration.

The supplied table name must have the form `<catalog>.<database>.<table>`, with an exact
catalog-name prefix. Catalog names can contain dots. The remaining database and table
identifiers each contain 1 to 128 letters, digits, or underscores. Foreign catalog names,
extra namespace levels, metadata-table suffixes, and malformed names fail before any request.
The method is intended for tables loaded or created by this catalog. Name validation
establishes the naming convention, rather than proving the provenance of an arbitrary
implementation of Iceberg's `Table` interface.

## Iceberg namespace operations

The catalog implements these `SupportsNamespaces` methods through generated `NamespaceApi`
operations. Every request sends the catalog name as the encoded REST prefix.

| Catalog operation | Behavior |
|---|---|
| `createNamespace(namespace)` | It creates metadata with an empty property map. |
| `createNamespace(namespace, properties)` | It creates metadata with the supplied properties. |
| `loadNamespaceMetadata(namespace)` | It returns an immutable defensive property map. |
| `dropNamespace(namespace)` | It returns true on deletion and false on HTTP 404. The service rejects a nonempty namespace. |
| `setProperties(namespace, updates)` | It sends sparse updates and returns whether the response acknowledges any updated keys. Unchanged values acknowledged by the service still return true. |
| `removeProperties(namespace, removals)` | It returns true if any keys were removed and false if every requested key was already missing. |

Creation, loading, and updates require HTTP 200. Deletion requires HTTP 204.
Response decoding checks required fields and string types before generated DTO defaults can
conceal malformed responses. Omitted or null namespace properties mean an empty property map.
The optional `missing` array may be omitted or null; servers may leave already-absent removal
keys unreported. Acknowledgements must identify requested keys and contain unique, disjoint
removal results.
Namespace names are one level. A response may preserve the database's original letter case.

The Iceberg overrides translate checked failures to Iceberg exceptions because their interface
signatures cannot declare this checked exception.

| Failure | Iceberg exception |
|---|---|
| Invalid input or HTTP 400/422 | `ValidationException` |
| HTTP 401 | `NotAuthorizedException` |
| HTTP 403 | `ForbiddenException` |
| HTTP 404 | `NoSuchNamespaceException`, except that drop returns false |
| HTTP 409 during creation | `AlreadyExistsException` |
| HTTP 409 during deletion | `NamespaceNotEmptyException` |
| HTTP 409 during property updates | `CommitFailedException` |
| Transport, malformed response, or other service failure | `RESTException` |

`namespaceExists` remains explicitly unsupported. `listNamespaces()` retains the legacy
table-derived endpoint, so an empty newly created namespace can be absent from that listing.
Namespace metadata does not change table lifecycle behavior.
