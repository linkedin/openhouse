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

The effective-property method declares concrete checked failures under
`CatalogOperationException`. Iceberg namespace methods expose the corresponding Iceberg runtime
exceptions. Both APIs preserve the cause while distinguishing missing metadata, access failures,
concurrent edits, and uncertain mutations.

Use `loadNamespaceMetadata` to retrieve namespace properties independently of any table.
A missing namespace fails that lookup. The effective-property helper supplies empty defaults
only for a verified missing namespace.

A status alone does not establish absence or rejection. The client validates the Iceberg error
envelope, its code, its type, and the requested operation. Empty, HTML, mismatched, and
unrecognized 404 responses fail retrieval and leave deletion uncertain. A confirmed service
failure is distinct from an uncertain mutation even though both use HTTP 500.

Correct invalid identifiers or property values before trying again. Reauthenticate after an
authentication rejection, and obtain the required privilege after an access rejection. An existing
namespace cannot be replaced by create; load it and decide whether an explicit property update
is appropriate. After a concurrent edit, reload properties before reapplying your intended change.
Automatic conflict retries are not performed.

The service returns `400/NamespaceNotEmptyException` for a nonempty drop. This catalog also
accepts the recognized legacy `409/NamespaceNotEmptyException` response, only for drop.
Confirmed version conflicts use `409/CommitFailedException` as an OpenHouse extension, not a
requirement of the [Iceberg 1.5.2 REST specification](https://github.com/apache/iceberg/blob/apache-iceberg-1.5.2/open-api/rest-catalog-open-api.yaml).
The stock Iceberg 1.5 REST client does not preserve these distinctions: its namespace handler
maps 400 to `BadRequestException` and 409 to `AlreadyExistsException`. Use the OpenHouse catalog
to receive the operation-specific outcomes described here.

Deploy the HouseTables namespace semantic-error contract before the Tables Service and this
client. Older error bodies without a semantic type cannot establish reliable absence or
rejection. Required internal listing failures and service credential or generated-request
rejections are service failures, rather than missing caller namespaces or caller authorization
errors.

`CommitStateUnknownException` means a mutation may have completed. It includes local write
timeouts, interrupted writes, lost connections, and unusable success responses, as well as an
uncertain outcome reported by the service. Read the metadata and reconcile the observed values
with the intended operation before retrying. Replaying a write without reconciling can overwrite
someone else's later update. A read timeout instead reports unavailable metadata and does not
claim a commit occurred.

Reads have a 30-second budget; mutations have a 60-second budget. Interruption cancels the pending
request and restores the thread's interrupted status. The adapter does not replay uncertain
mutations. Applications and external REST clients control their own retry policies.

`namespaceExists` remains explicitly unsupported. `listNamespaces()` retains the legacy
table-derived endpoint, so an empty newly created namespace can be absent from that listing.
Drop rejects a namespace occupied by a table or view. Successful drop removes only the optional
defaults; table metadata and files remain intact. Concurrent table creation is not serialized
with drop, so an existing table may have no namespace metadata.
