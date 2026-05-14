package com.linkedin.openhouse.optimizer.db;

import com.vladmihalcea.hibernate.type.json.JsonStringType;
import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

/**
 * Append-only record of per-commit stats reported by the Tables Service.
 *
 * <p>Each Iceberg commit produces one row. Consumers can query this table to reconstruct change
 * rates over arbitrary time windows.
 *
 * <p>Self-contained DB-layer type. The stats payload is split across two JSON columns — {@link
 * SnapshotMetrics} (point-in-time fields at commit time) and {@link CommitDeltaMetrics} (per-commit
 * counters).
 */
@TypeDef(name = "json", typeClass = JsonStringType.class)
@Entity
@Table(
    name = "table_stats_history",
    indexes = {
      @Index(name = "idx_tsh_table_uuid", columnList = "table_uuid"),
      @Index(name = "idx_tsh_recorded_at", columnList = "recorded_at")
    })
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableStatsHistoryRow {

  /** UUID primary key — set by the caller, not generated server-side. */
  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  /** Stable Iceberg table UUID. */
  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  /** Denormalized database name. */
  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  /** Denormalized table name. */
  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  /** Snapshot fields at commit time. Stored as a JSON blob in the {@code snapshot} column. */
  @Type(type = "json")
  @Column(name = "snapshot", columnDefinition = "TEXT")
  private SnapshotMetrics snapshot;

  /** Per-commit delta counters. Stored as a JSON blob in the {@code delta} column. */
  @Type(type = "json")
  @Column(name = "delta", columnDefinition = "TEXT")
  private CommitDeltaMetrics delta;

  /** When this history row was recorded (commit time). */
  @Column(name = "recorded_at", nullable = false)
  private Instant recordedAt;
}
