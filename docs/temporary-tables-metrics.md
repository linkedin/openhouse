# Temporary Table Deletion Metrics

This page lists the metrics that the Optimizer analyzer and `ExpiredTableDeletionSparkApp` emit for
the deletion of expired temporary tables. It also proposes dashboard panels and alerts for oncall.
The dashboard panels and alert thresholds are proposals and have not been agreed on yet.

## How deletion works

The analyzer runs a full scan as a Kubernetes CronJob. For each temporary table whose
`openhouse.expirationTime` has passed, it creates a PENDING `EXPIRED_TABLE_DELETION` operation. The
scheduler later claims the operation and submits one `ExpiredTableDeletionSparkApp` job per table.
The job loads the table again, drops it only if it is still temporary and still expired, and
reports the result to the Optimizer service. A failed deletion is retried on the next hourly scan.
The design target is that deletion completes within 24 hours of expiration for at least 99.9% of
tables.

## Metrics

### Analyzer metrics

The analyzer emits these through Micrometer, following the `openhouse_postcommit_operation`
convention used by the tables service. They are published at the end of each full scan by
`ExpiredTableDeletionAnalyzer`. They carry no database tag, because no existing Micrometer metric
in the repository tags by database.

| Metric | Type | Unit | Tags | Meaning |
| --- | --- | --- | --- | --- |
| `openhouse_optimizer_expired_temporary_tables` | Gauge | tables | none | The number of temporary tables the last scan found past their expiration time that no deletion has removed yet. |
| `openhouse_optimizer_expired_table_deletions_scheduled` | Counter | operations | none | The number of deletion operations the analyzer created. Each scan adds the number of PENDING operations it created. |
| `openhouse_optimizer_oldest_expired_temporary_table_age_seconds` | Gauge | seconds | none | How long the oldest expired temporary table from the last scan has been past its expiration time. It is 0 when no expired table remains. |

A table counts as expired until a deletion succeeds at or after its current expiration time. The
stats row of a dropped table stays in the Optimizer database, so the analyzer uses the latest
deletion history entry, not the presence of the stats row, to decide whether the table is gone.

The two gauges are registered at the end of the first scan, not at startup. The Optimizer service
runs only commit-driven analysis, which this analyzer does not take part in, so the service does
not report a zero that would hide a real backlog. A scan that throws does not publish its partial
counts, and the gauges keep the values of the last completed scan.

### Deletion job metrics

`ExpiredTableDeletionSparkApp` emits these through `OtelEmitter` under the
`com.linkedin.openhouse.jobs.spark.BaseSparkApp` scope, following the convention of the other
Spark apps. Each run emits exactly one outcome.

| Metric | Type | Unit | Tags | Meaning |
| --- | --- | --- | --- | --- |
| `expired_table_deletion_count` | Counter | runs | `table_name`, `outcome` | One run of the deletion job, tagged with its outcome. |
| `expired_table_deletion_lag` | Histogram | milliseconds | `table_name` | The time from the table's expiration time to the drop. It is recorded only for the `deleted` outcome. |

The `outcome` tag takes these values:

| Outcome | Meaning | Reported status |
| --- | --- | --- |
| `deleted` | The table was still temporary and expired, and the job dropped it. | SUCCESS |
| `skipped_not_temporary` | The table is no longer temporary, because it was converted or replaced by an ordinary table with the same name. | SUCCESS |
| `skipped_not_expired` | The table is temporary but has not expired by the job's clock, for example because the owner extended the expiration time. | FAILED, so the analyzer retries |
| `skipped_missing` | The table no longer exists, either before the job loaded it or because it was dropped between the load and the drop. | SUCCESS |
| `failed` | Loading or dropping the table threw an exception. | FAILED, so the analyzer retries |

`table_name` is the fully qualified `db.table` name, as in the other Spark app metrics. Temporary
tables are short-lived, so this tag has high cardinality. Dashboards should aggregate it away.

## Proposed dashboard panels

These panels are proposals.

1. **Oldest expired temporary table age.** A time series of
   `openhouse_optimizer_oldest_expired_temporary_table_age_seconds`, shown in hours, with lines at
   6 hours and 24 hours that match the proposed alerts. This is the primary panel for the 24 hour
   target.
2. **Expired temporary tables awaiting deletion.** A time series of
   `openhouse_optimizer_expired_temporary_tables`. A steady rise means deletion is not keeping up
   with expiration.
3. **Deletions scheduled per hour.** The hourly increase of
   `openhouse_optimizer_expired_table_deletions_scheduled`.
4. **Deletion outcomes.** The rate of `expired_table_deletion_count`, summed by `outcome` and
   stacked. `failed` and `skipped_not_expired` should stay near zero.
5. **Deletion failure ratio.** The `failed` rate divided by the total rate of
   `expired_table_deletion_count`.
6. **Deletion lag percentiles.** P50, P99, and P999 of `expired_table_deletion_lag` across all
   tables, shown in hours, with a line at 24 hours.

## Proposed alerts

These thresholds are proposals and have not been agreed on with the oncall rotation.

| Alert | Condition | Severity |
| --- | --- | --- |
| Expired temporary table backlog | `openhouse_optimizer_oldest_expired_temporary_table_age_seconds` is greater than 21600 (6 hours) | Warn |
| Expired temporary table past deletion target | `openhouse_optimizer_oldest_expired_temporary_table_age_seconds` is greater than 86400 (24 hours) | Page |
| Analyzer scan stale | The age gauge has not been updated for more than 3 hourly scans | Warn |

The age gauge holds the value of the last completed scan. If the analyzer CronJob stops running,
the gauge stops changing instead of growing, so the staleness alert is needed to catch a stopped
analyzer. How to detect staleness depends on the metrics backend, which is an open question below.

## Open questions

1. **Exporting the analyzer metrics.** The analyzer runs as a one-shot CronJob without a web
   server, and the Micrometer registry on its classpath is the pull-based Prometheus registry.
   Nothing scrapes the process before it exits, so the analyzer metrics are not exported yet. The
   options are to add a push-based Micrometer registry, such as OTLP, to the analyzer app, or to
   compute the gauges in the long-running Optimizer service from its database. This must be
   decided before the dashboard and alerts can work.
2. **Tag cardinality.** The deletion job metrics carry `table_name` to match the other Spark apps.
   It is not settled whether the metrics backend accepts one series per temporary table, or
   whether the tag should be dropped or replaced by a database tag for these metrics.
3. **Lag for missing tables.** Lag is recorded only when the job drops the table. A table that
   another actor dropped first is counted as `skipped_missing` without a lag value. It is not
   settled whether that case should also record lag.
4. **Filtered scans.** The CronJob always runs a full scan. A scan filtered to one database or
   table would publish gauges for only that subset. It is not settled whether filtered scans
   should publish the gauges at all.
5. **Alert thresholds.** The 6 hour warning, the 24 hour page, and the staleness window are
   proposals and need agreement from the oncall rotation.
