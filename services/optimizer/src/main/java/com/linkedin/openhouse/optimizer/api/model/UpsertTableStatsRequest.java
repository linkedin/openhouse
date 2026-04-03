package com.linkedin.openhouse.optimizer.api.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code PUT /v1/table-stats/{tableUuid}}.
 *
 * <p>{@code tableUuid} comes from the path variable. {@code databaseId} and {@code tableName} are
 * denormalized display columns carried in the body.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpsertTableStatsRequest {

  private String databaseId;
  private String tableName;
  private TableStats stats;
  private Map<String, String> tableProperties;
}
