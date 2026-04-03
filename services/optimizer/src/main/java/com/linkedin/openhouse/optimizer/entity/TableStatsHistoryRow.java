package com.linkedin.openhouse.optimizer.entity;

import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.vladmihalcea.hibernate.type.json.JsonStringType;
import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
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
 * <p>Each Iceberg commit produces one row. The {@code stats} JSON contains both the snapshot
 * metrics (point-in-time) and the commit delta (files added/deleted in this commit). Consumers can
 * query this table to reconstruct change rates over arbitrary time windows.
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

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id", nullable = false)
  private Long id;

  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_id", nullable = false, length = 255)
  private String databaseId;

  @Column(name = "table_name", nullable = false, length = 255)
  private String tableName;

  @Type(type = "json")
  @Column(name = "stats", columnDefinition = "TEXT")
  private TableStats stats;

  @Column(name = "recorded_at", nullable = false)
  private Instant recordedAt;
}
