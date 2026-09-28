package com.linkedin.openhouse.optimizer.db;

import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Per-operation-type watermark for the analyzer's incremental scan. {@link #watermark} is the start
 * time of the last completed analysis pass; the next pass reads {@code table_stats} rows with
 * {@code updated_at >= watermark}, so only tables written since the last run are re-evaluated.
 */
@Entity
@Table(name = "analyzer_run_state")
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class AnalyzerRunStateRow {

  /** Operation type name (e.g. {@code ORPHAN_FILES_DELETION}). Primary key. */
  @Id
  @Column(name = "operation_type", nullable = false, length = 50)
  private String operationType;

  /** Start time of the last completed analysis pass for this operation type. */
  @Column(name = "watermark", nullable = false)
  private Instant watermark;
}
