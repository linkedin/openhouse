package com.linkedin.openhouse.optimizer.api.spec;

import java.util.Collections;
import java.util.Map;
import javax.validation.constraints.NotBlank;
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

  /** Denormalized database name for display. Required. */
  @NotBlank private String databaseName;

  /** Denormalized table name for display. Required. */
  @NotBlank private String tableName;

  /** Combined snapshot + delta stats payload from this commit. */
  private TableStatsPayload stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /**
   * Build the internal-model {@link com.linkedin.openhouse.optimizer.model.TableStatsDto} described
   * by this request. {@code tableUuid} comes from the URL path, not the body. {@code updatedAt} is
   * left {@code null}; the service stamps it server-side at write time.
   */
  public com.linkedin.openhouse.optimizer.model.TableStatsDto toModel(String tableUuid) {
    com.linkedin.openhouse.optimizer.model.TableStatsDto payload =
        stats == null
            ? new com.linkedin.openhouse.optimizer.model.TableStatsDto()
            : stats.toModel();
    return payload
        .toBuilder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .tableProperties(tableProperties != null ? tableProperties : Collections.emptyMap())
        .build();
  }
}
