package com.linkedin.openhouse.optimizer.api.model;

import com.linkedin.openhouse.optimizer.model.Table;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code PUT /v1/table-stats/{tableUuid}}.
 *
 * <p>{@code tableUuid} comes from the path variable. {@code databaseName} and {@code tableName} are
 * denormalized display columns carried in the body.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpsertTableStatsRequest {

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Combined snapshot + delta stats payload from this commit. */
  private TableStats stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /**
   * Build the internal-model {@link Table} described by this request. {@code tableUuid} comes from
   * the URL path, not the body. {@link Table#getUpdatedAt()} is left {@code null}; the service
   * stamps it server-side at write time.
   */
  public Table toModel(String tableUuid) {
    return Table.builder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableId(tableName)
        .tableProperties(tableProperties != null ? tableProperties : Collections.emptyMap())
        .stats(stats == null ? null : stats.toModel())
        .build();
  }
}
