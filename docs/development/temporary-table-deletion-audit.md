# Auditing Optimizer Deletion of Temporary Tables

The Optimizer deletes expired temporary tables with `ExpiredTableDeletionSparkApp`, one table per
Spark job. Three records answer "who or what deleted my table, and when". Each one covers a
different part of the question, so start with the one that matches what you know.

| Record | Written by | Identifies the Optimizer | Table UUID | Expiration time | Tells a drop from a skip |
|--------|------------|--------------------------|------------|-----------------|--------------------------|
| Deletion job audit line | `ExpiredTableDeletionSparkApp` | Yes, by logger and operation ID | Yes | Yes | Yes |
| Optimizer operation history | Optimizer service | Yes, by operation type | Yes | No | No |
| Tables service audit event | Tables service | Only by the job's principal | No | No | Not applicable |

## Deletion job audit line

Every run of the deletion job writes exactly one line at INFO level to the logger
`com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionAudit`, whatever the outcome. The line has
this form:

```
Expired table deletion audit: operationId=<id> jobId=<id> tableUuid=<uuid> databaseName=<db> tableName=<table> expirationTime=<time> completedAt=<instant> outcome=<outcome> failureReason=<reason>
```

Absent values are written as `none`. `tableUuid` and `expirationTime` are the values the job read
from the table before it acted, and `completedAt` is when the job finished the attempt. The failure
reason can contain spaces, so it is the last field and runs to the end of the line.

The outcome values are the same as the `outcome` tag of the `expired_table_deletion_count` metric
described in [Temporary Table Deletion Metrics](../temporary-tables-metrics.md), because both come
from one outcome that the job decides per run.

| Outcome | Meaning | Reported to the Optimizer as |
|---------|---------|------------------------------|
| `deleted` | The job dropped and purged the table. | `SUCCESS` |
| `skipped_missing` | The table did not exist when the job loaded or dropped it. | `SUCCESS` |
| `skipped_not_temporary` | The table is no longer temporary, so the job left it in place. | `SUCCESS` |
| `skipped_not_expired` | The table is temporary but has not expired by the job's clock, so the job left it in place. | `FAILED` |
| `failed` | Loading or dropping the table threw an exception. | `FAILED` |

To find the line, search the deletion job's driver logs for the table name and the logger name, for
example:

```
grep 'Expired table deletion audit:.* databaseName=db tableName=t ' driver.log
```

A line with `outcome=deleted` is the record that the Optimizer deleted the table, and `completedAt`
is when. The job ID in the line leads to the Spark job's own logs.

## Optimizer operation history

The Optimizer service writes a `table_operations_history` row when the job reports its result. Look
up the table's deletion operations by name, which also returns the table UUID and the job ID:

```
GET /v1/optimizer/operations?operationType=EXPIRED_TABLE_DELETION&databaseName=db&tableName=t&limit=10
```

Then read the completed attempts for that UUID:

```
GET /v1/optimizer/operations-history/{tableUuid}?limit=10
```

The same data is in the Optimizer database:

```sql
SELECT id, table_uuid, completed_at, status, failure_reason
FROM table_operations_history
WHERE database_name = 'db' AND table_name = 't'
  AND operation_type = 'EXPIRED_TABLE_DELETION'
ORDER BY completed_at DESC;
```

The history row's `id` equals the audit line's `operationId`. A `SUCCESS` row means only that the
job finished without needing a retry. It is also written when the table was already gone or was no
longer temporary, so use the audit line's outcome to confirm that the job dropped the table.

## Tables service audit event

The drop request from the job goes through `TablesApiHandler.deleteTable`, so the tables service
emits a `TableAuditEvent` with `operationType=DELETE` through the configured
`AuditHandler<TableAuditEvent>`. The event carries the database and table names,
`eventTimestamp`, `operationStatus`, and `user`, which is the principal the deletion job
authenticates as. The deletion job does not declare the `SYSTEM` action type, so `actionType` is
empty. A `DELETE` event from that principal at the audit line's `completedAt` is the service-side
record of the same drop. Where the events are stored depends on the deployment's audit handler.

## Open questions

- Should the tables service audit event for this drop name the Optimizer directly, for example
  through a client tag or a drop reason? The service has no field for either today.
- Should the Optimizer operation history store the outcome, so that a `SUCCESS` row distinguishes
  a drop from a skip without reading job logs?
- How long are the deletion job's driver logs retained, and is that long enough for this audit
  record?
