# Managing namespace defaults

Tables Service exposes namespace metadata through four operations from the
[Apache Iceberg 1.5.2 REST catalog specification](https://github.com/apache-iceberg/blob/apache-iceberg-1.5.2/open-api/rest-catalog-open-api.yaml).
Use namespace properties for shared defaults that consumers explicitly apply when resolving
effective table properties. Explicit table values win. Creating or changing defaults does not
rewrite tables, and table operations continue to work without namespace metadata.

The `prefix` is the client catalog name. A namespace contains exactly one name using 1 to 128
letters, digits, or underscores. Lookup is case-insensitive, while responses retain the spelling
submitted at creation. Creating `Finance` makes `FINANCE` refer to the same record.

| Method | Route | Success |
|---|---|---|
| POST | `/v1/{prefix}/namespaces` | 200 |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 200 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 204 |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 200 |

## Create and edit defaults

An administrator supplies a one-element `namespace` list and an optional `properties` map.
Loading returns the stored name and properties. A duplicate create returns 409 without changing
the original spelling or values.

To edit defaults, send an `updates` map containing the keys to set and a `removals` list containing
the keys to remove. Either collection may be omitted. Updates preserve unspecified keys. The
response identifies updated keys, removed keys, and requested removal keys that were absent. A key
appearing in both collections returns 422.

Property keys may contain well-formed Unicode and at most 512 UTF-16 code units. Empty keys,
spaces, accents, case differences, and supplementary characters are preserved. Unpaired UTF-16
surrogates and overlength keys return 400.

## Access and recovery

Creation, property updates, removals, and drop require `SYSTEM_ADMIN`. Loading metadata requires
`GET_TABLE_METADATA`. Authorization uses case-insensitive namespace identity.

Invalid requests return 400 or 422. Authentication failures return 401, insufficient privileges
return 403, and a missing namespace returns 404. Consumers may treat 404 as having no defaults.
Concurrent update or drop conflicts return 409 with `CommitFailedException`. Tables Service reads
the current document once, merges the sparse request, and submits one conditional replacement.
It does not retry conflicts. Other failures return 500 and may leave a write outcome uncertain.
Read current metadata before submitting another write.

## Drop and table independence

Dropping namespace metadata does not inspect, delete, or modify tables or views. The namespace
metadata must exist, otherwise drop returns 404. Tables and views can remain after the metadata is
dropped because namespace records are optional defaults, not parent records.

Administrators must deploy the HouseTables internal namespace API and apply the
[HouseTables namespace DDL](https://github.com/linkedin/openhouse/blob/mkuchenbecker/database-properties-hts/services/housetables/ddl/0002__add_namespace_metadata.sql)
before serving namespace requests. LinkedIn rollout also requires a separate `SYSTEM_ADMIN`
authorization-handler change.
