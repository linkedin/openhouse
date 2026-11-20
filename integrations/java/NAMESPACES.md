# Java namespace metadata

The Iceberg 1.5 `OpenHouseCatalog` supports namespace metadata through Iceberg's
`SupportsNamespaces` API. These methods create, read, update, and delete optional database-level
metadata independently of tables.

This API does not merge namespace properties into table properties, change `loadTable`, or add
namespace support to the Iceberg 1.2 catalog.

## Supported operations

| Catalog operation | Behavior |
|---|---|
| `createNamespace(namespace)` | Creates metadata with an empty property map. |
| `createNamespace(namespace, properties)` | Creates metadata with the supplied properties. |
| `loadNamespaceMetadata(namespace)` | Returns an immutable property map. Missing metadata throws `NoSuchNamespaceException`. |
| `setProperties(namespace, updates)` | Applies sparse updates and returns whether the service acknowledged an updated key. |
| `removeProperties(namespace, removals)` | Removes requested keys and returns whether any key was removed. |
| `dropNamespace(namespace)` | Returns `true` after deletion and `false` when metadata is absent. Tables and views do not block metadata deletion. |

Namespace names contain one level with 1 to 128 letters, digits, or underscores. Lookup is
case-insensitive, while responses retain the spelling used at creation. Omitted or null property
maps are returned as empty maps.

Property updates are sparse. Unspecified keys remain unchanged. Removal results may report keys as
removed or already missing.

## Permissions and failures

Create, update, remove, and drop require `SYSTEM_ADMIN`. Loading metadata requires
`GET_TABLE_METADATA`. Permission to modify a table does not grant permission to modify namespace
metadata.

Recognized service rejections map to the Iceberg exceptions callers already use:

| Result | Exception |
|---|---|
| Invalid namespace or property request | `ValidationException` |
| Missing metadata during load or update | `NoSuchNamespaceException` |
| Duplicate create | `AlreadyExistsException` |
| Concurrent update or drop | `CommitFailedException` |
| Authentication required | `NotAuthorizedException` |
| Insufficient permission | `ForbiddenException` |
| Other transport or service failure | `RESTException` |

The client maps each operation's HTTP status to these outcomes. Other statuses remain
`RESTException` failures.

## Operational behavior

Reads have a 30-second budget. Mutations have a 60-second budget. The client does not retry.
Interruption cancels the pending request and restores the thread's interrupted status.

A failed mutation may have committed even when the response was lost. Load the current metadata
and compare it with the intended change before submitting another write.

`listNamespaces()` retains the existing table-derived listing, so metadata for an empty namespace
may not appear there. `namespaceExists()` remains unsupported. Successful drop removes namespace
metadata without listing, deleting, or modifying tables or views.

## Version and deployment

This surface is available in the packaged Iceberg 1.5 Java catalog. Iceberg 1.2 behavior is
unchanged.

Deploy the matching HouseTables API and Tables Service namespace API before this client. The
service must return the HTTP statuses required for the mapped Iceberg exceptions.
