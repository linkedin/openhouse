# VACUUM

**Status: Alpha.** `VACUUM` is opt-in per table. Please See [Enabling VACUUM](#enabling-vacuum)).

`VACUUM` is an OpenHouse Spark SQL extension that reclaims storage for an OpenHouse
Iceberg table by removing files that are no longer needed. It is thin, ergonomic sugar
over the underlying Iceberg maintenance stored procedures.

## Syntax

```sql
VACUUM <table> [REMOVE ORPHAN FILES] [RETAIN <n> HOURS]
```

- `<table>` — an OpenHouse table identifier (e.g. `openhouse.db.table`).
- `REMOVE ORPHAN FILES` — *(optional)* also delete orphaned files (see below). Off by default.
- `RETAIN <n> HOURS` — *(optional)* retention window in whole hours. When omitted, each
  underlying operation uses its own default retention.

## Behavior

Running `VACUUM` reclaims files beyond the retention window that are no longer referenced
by the current version of the table.

1. **Orphan-file deletion** (`REMOVE ORPHAN FILES`, opt-in). Orphan files are files under the table's location that are not referenced by any table metadata typically left behind by failed or aborted writes. This step only deletes files from storage; it does not commit table metadata, so it succeeds even when the table is out of write quota. 

2. **Snapshot expiration** always runs. It removes snapshots older than the retention window and deletes the data, delete, manifest, and manifest-list files that those expired snapshots exclusively referenced. This command adds a commit and can conflict with in-flight transactions.

3. **Retention** (`RETAIN <n> HOURS`) bounds both operations: only files older than `now - n hours` are eligible. The cutoff is resolved to a concrete timestamp in the session time zone at execution time. When `RETAIN` is omitted, snapshot expiration falls back to the table's configured snapshot-age retention and orphan-file deletion's configured default.


## Enabling VACUUM

`VACUUM` is Alpha and must be enabled on each table before use:

```sql
ALTER TABLE openhouse.db.table
  SET TBLPROPERTIES ('openhouse.vacuum.enabled' = 'true');
```

| Property                   | Value    | Meaning                                     |
| -------------------------- | -------- | ------------------------------------------- |
| `openhouse.vacuum.enabled` | `'true'` | Opt this table into the Alpha `VACUUM` command. |

Any other value (or the property being absent) leaves `VACUUM` disabled for the table, and
running the command throws an `UnsupportedOperationException` that explains how to enable it.
`VACUUM` is only supported on OpenHouse tables; running it on a non-OpenHouse table also
throws.

## Examples

Enable the feature, then expire snapshots older than 24 hours:

```sql
ALTER TABLE openhouse.db.table
  SET TBLPROPERTIES ('openhouse.vacuum.enabled' = 'true');

VACUUM openhouse.db.table RETAIN 24 HOURS;
```

Expire snapshots using the table's default retention:

```sql
VACUUM openhouse.db.table;
```

Also remove orphaned files, retaining anything from the last 168 hours (7 days):

```sql
VACUUM openhouse.db.table REMOVE ORPHAN FILES RETAIN 168 HOURS;
```

## Notes and caveats

- **`REMOVE ORPHAN FILES` is expensive.** It performs a recursive listing of the table's
  location to find unreferenced files. On tables with very large file counts this can be
  slow and memory-intensive, and may require a larger Spark driver to avoid running out of
  memory.
- **Low Retention causes in-flight operations to fail** A query sees the same snapshot of the table they start with, and expiring a snapshot that is in-use will cause transactions to fail. Deleting orphans of in-flight transactions can cause failure.  24 hours is the suggested minimum but can be lowered to mitigate emergency scenarios. 
- **Snapshot expiration requires write quota**; orphan-file deletion does not. This is why
  orphan-file deletion runs first — on a table that is out of quota, orphan cleanup still
  proceeds even though expiration cannot commit.
