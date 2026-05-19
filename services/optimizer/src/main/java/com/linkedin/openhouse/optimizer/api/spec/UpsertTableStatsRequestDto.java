package com.linkedin.openhouse.optimizer.api.spec;

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
public class UpsertTableStatsRequestDto {

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Combined snapshot + delta stats payload from this commit. */
  private TableStatsPayloadDto stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /**
   * Build the internal-model {@link com.linkedin.openhouse.optimizer.model.TableStats} described by
   * this request. {@code tableUuid} comes from the URL path, not the body. {@code updatedAt} is
   * left {@code null}; the service stamps it server-side at write time.
   */
  public com.linkedin.openhouse.optimizer.model.TableStats toModel(String tableUuid) {
    com.linkedin.openhouse.optimizer.model.TableStats payload =
        stats == null ? new com.linkedin.openhouse.optimizer.model.TableStats() : stats.toModel();
    return payload
        .toBuilder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .tableProperties(tableProperties != null ? tableProperties : Collections.emptyMap())
        .build();
  }
}
