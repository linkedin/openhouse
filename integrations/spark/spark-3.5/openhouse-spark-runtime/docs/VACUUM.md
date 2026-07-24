# VACUUM

> **Status: Alpha.** `VACUUM` is opt-in per table. A table must explicitly enable it
> (see [Enabling VACUUM](#enabling-vacuum)); running `VACUUM` on a table that has not
> enabled it fails with an `UnsupportedOperationException`. Behavior may change in future
> releases.

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

The keywords `VACUUM`, `REMOVE`, `ORPHAN`, `FILES`, `RETAIN`, and `HOURS` are
non-reserved, so existing identifiers with those names continue to parse.

## Behavior

Running `VACUUM` reclaims files beyond the retention window that are no longer referenced
by the current version of the table.

1. **Orphan-file deletion** (`REMOVE ORPHAN FILES`, opt-in) runs **first**. Orphan files
   are files under the table's location that are not referenced by any table metadata —
   typically left behind by failed or aborted writes. This step only deletes files from
   storage; it does not commit table metadata, so it succeeds even when the table is out
   of write quota. Running it before expiration also means it evaluates against the
   pre-expiration set of referenced files, so it can never delete a file that a still-live
   snapshot references.

2. **Snapshot expiration** always runs, **after** orphan-file deletion. It removes
   snapshots older than the retention window and deletes the data, delete, manifest, and
   manifest-list files that those expired snapshots exclusively referenced. Expiration
   commits table metadata.

3. **Retention** (`RETAIN <n> HOURS`) bounds both operations: only files older than
   `now - n hours` are eligible. The cutoff is resolved to a concrete timestamp in the
   session time zone at execution time. When `RETAIN` is omitted, snapshot expiration
   falls back to the table's configured snapshot-age retention and orphan-file deletion
   falls back to Iceberg's safe default.

### Merge-on-read (MoR) delete files

`VACUUM` handles merge-on-read delete files (position and equality deletes) the same way
it handles data files:

- Delete files referenced only by expired snapshots are removed by **snapshot expiration**.
- Orphaned delete files are removed by **`REMOVE ORPHAN FILES`**.

Dangling delete files that are still referenced by the current snapshot but no longer
apply to any live data (for example, because the data files they targeted were compacted
away) are **not** removed by `VACUUM`. Clearing those is a data-rewrite/compaction concern
handled by `OPTIMIZE`, not by `VACUUM`.

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
  memory. Prefer a generous `RETAIN` window and run it less frequently.
- **Choose the retention window carefully.** Files newer than the retention window are
  never deleted. Time travel and snapshot rollback are only possible for snapshots that
  have not been expired, so retain enough history for your recovery needs.
- **Snapshot expiration requires write quota**; orphan-file deletion does not. This is why
  orphan-file deletion runs first — on a table that is out of quota, orphan cleanup still
  proceeds even though expiration cannot commit.
