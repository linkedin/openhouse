package com.linkedin.openhouse.tables.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

// In-memory snapshot of the Iceberg current-snapshot metadata that was loaded alongside a
// TableDto.
//
// This carries only fields already materialized by the catalog client. Constructing it never
// triggers a separate HDFS or object-store read.
//
// The value is present whenever the underlying table has at least one committed snapshot at
// construction time. It is absent (modeled as Optional.empty() on TableDto) for tables with no
// committed data, such as a CREATE TABLE with no rows yet.
@Getter
@Builder
@AllArgsConstructor
@EqualsAndHashCode
public class CurrentSnapshotInfo {

  // Iceberg snapshot ID. Stable per commit; usable as an idempotency token.
  private final long snapshotId;

  // Iceberg Snapshot.summary() map, unmodified. Keys include total-data-files, total-files-size,
  // added-data-files, deleted-data-files, added-files-size, removed-files-size.
  private final Map<String, String> summary;
}
