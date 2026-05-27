package com.linkedin.openhouse.tables.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * In-memory snapshot of the Iceberg current-snapshot metadata that was loaded alongside a {@link
 * TableDto}. Carries only fields already materialized by the catalog client — never triggers a
 * separate HDFS / object-store read.
 *
 * <p>Present whenever the table has at least one committed snapshot at construction time; absent
 * (modeled as {@link java.util.Optional#empty()} on {@link TableDto}) when the table has no
 * committed data — e.g. a {@code CREATE TABLE} with no rows yet.
 */
@Getter
@Builder
@AllArgsConstructor
@EqualsAndHashCode
public class CurrentSnapshotInfo {

  /** Iceberg snapshot ID (decimal long). Stable per commit; usable as an idempotency token. */
  private final long snapshotId;

  /**
   * Iceberg {@code Snapshot.summary()} map, unmodified. Keys include {@code total-data-files},
   * {@code total-files-size}, {@code added-data-files}, {@code deleted-data-files}, {@code
   * added-files-size}, {@code removed-files-size}.
   */
  private final Map<String, String> summary;
}
