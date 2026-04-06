package com.linkedin.openhouse.optimizer.entity;

import com.linkedin.openhouse.optimizer.model.TableStats;
import com.vladmihalcea.hibernate.type.json.JsonStringType;
import java.time.Instant;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

/**
 * JPA entity for the optimizer {@code table_stats} table. Written by the Tables Service on every
 * Iceberg commit; read by the Analyzer and Scheduler directly via JPA.
 */
@TypeDef(name = "json", typeClass = JsonStringType.class)
@Entity
@Table(name = "table_stats")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatsRow {

  @Id
  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_id", nullable = false, length = 255)
  private String databaseId;

  @Column(name = "table_name", nullable = false, length = 255)
  private String tableName;

  @Type(type = "json")
  @Column(name = "stats", columnDefinition = "TEXT")
  private TableStats stats;

  @Type(type = "json")
  @Column(name = "table_properties", columnDefinition = "TEXT")
  private Map<String, String> tableProperties;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;
}
