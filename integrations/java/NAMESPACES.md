# Java namespace metadata and effective table properties

The packaged Java catalog for Iceberg 1.5 resolves namespace defaults only when callers request
effective properties. The Iceberg 1.2 catalog retains its existing behavior and does not provide
this API. Actual table properties keep precedence:

```java
Map<String, String> effective = catalog.getEffectiveTableProperties(table);
```

The method is public on `OpenHouseCatalog` and declares `CatalogOperationException`.
It performs one explicit, uncached namespace metadata request. Namespace properties supply
defaults; actual table properties override matching keys. The returned map is an immutable
defensive copy. Calling `table.properties()` still returns the actual table properties.
Existing table operations, including `loadTable`, retain their existing request counts.

Only a missing namespace error falls back to table-only properties. Successful responses
with empty, omitted, or null properties also contribute no namespace properties.
Authorization failures, connection failures, timeouts, malformed responses, and other service
errors fail the call because they do not establish that defaults are absent. Callers must handle
the checked failure rather than silently using table-only values during an outage.

The supplied table name must have the form `<catalog>.<namespace>.<table>`, with an exact
catalog-name prefix. Catalog names can contain dots. The remaining namespace and table
identifiers each contain 1 to 128 letters, digits, or underscores. Foreign catalog names,
extra namespace levels, metadata-table suffixes, and malformed names fail before any request.
The method is intended for tables loaded or created by this catalog. Name validation checks the naming convention; callers supply the table from the intended catalog.

## Iceberg namespace operations

Administrators can create and edit defaults with these `SupportsNamespaces` methods.
Creation, property updates, removal, and drop require `SYSTEM_ADMIN`. Reads require
`GET_TABLE_METADATA`. Permission to write a table does not grant permission to alter the defaults
used by other consumers.

| Catalog operation | Behavior |
|---|---|
| `createNamespace(namespace)` | It creates metadata with an empty property map. |
| `createNamespace(namespace, properties)` | It creates metadata with the supplied properties. |
| `loadNamespaceMetadata(namespace)` | It returns an immutable defensive property map and propagates verified absence as `NoSuchNamespaceException`. |
| `dropNamespace(namespace)` | It returns true on deletion and false on verified namespace absence. The service rejects a nonempty namespace. |
| `setProperties(namespace, updates)` | It sends sparse updates and returns whether the response acknowledges any updated keys. Unchanged values acknowledged by the service still return true. |
| `removeProperties(namespace, removals)` | It returns true if any keys were removed and false if every requested key was already missing. |

Omitted or null namespace properties mean an empty property map.
The optional `missing` array may be omitted or null; servers may leave already-absent removal
keys unreported. Acknowledgements must identify requested keys and contain unique, disjoint
removal results.
Namespace names are one level. The service retains insertion spelling and resolves names
case-insensitively. A create of `Finance` followed by a load of `FINANCE` addresses the same
namespace.

## Recovering from errors

The effective-property method declares `CatalogOperationException`. Supported request rejections,
such as missing metadata and access denial, have specialized checked types. Other failures use
the checked base directly. Iceberg namespace methods adapt those rejections to the corresponding
Iceberg exceptions and other failures to `RESTException`. Both APIs preserve their local cause.

Use `loadNamespaceMetadata` to retrieve namespace properties independently of any table.
A missing namespace fails that lookup. The effective-property helper supplies empty defaults
only for a verified missing namespace.

A status alone does not establish absence or rejection. The client validates the Iceberg error
envelope, its code, its type, and the requested operation. Empty, HTML, mismatched, and
unrecognized 404 responses fail retrieval and deletion.

Correct invalid identifiers or property values before trying again. Reauthenticate after an
authentication rejection, and obtain the required privilege after an access rejection. An existing
namespace cannot be replaced by create; load it and decide whether an explicit property update
is appropriate.

The service returns `400/NamespaceNotEmptyException` for a nonempty drop. This catalog also
accepts the recognized legacy `409/NamespaceNotEmptyException` response, only for drop.

Deploy the HouseTables namespace semantic-error contract before the Tables Service and this
client. Older error bodies without a semantic type cannot establish reliable absence or
rejection. Required internal listing failures and service credential or generated-request
rejections are service failures, rather than missing caller namespaces or caller authorization
errors.

Every generic write failure may have changed stored metadata. Read current metadata and
reconcile its identity and properties with the intended operation before submitting another
write. This applies to stale-write rejection, timeouts, interrupted writes, lost connections,
and unusable success responses. Replaying a write without reconciling can overwrite someone
else's later update.

Reads have a 30-second budget; mutations have a 60-second budget. Interruption cancels the pending
request and restores the thread's interrupted status. Exception handling does not retry
requests. The existing HTTP connector configuration remains unchanged; applications and
external REST clients control their own retry policies.

`namespaceExists` remains explicitly unsupported. `listNamespaces()` retains the legacy
table-derived endpoint, so an empty newly created namespace can be absent from that listing.
Drop rejects a namespace occupied by a table or view. Successful drop removes only the optional
defaults; table metadata and files remain intact. Concurrent table creation is not serialized
with drop, so an existing table may have no namespace metadata.
