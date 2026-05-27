package com.linkedin.openhouse.tables.services.optimizer;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wire body for {@code PUT /v1/optimizer/stats/{tableUuid}}. Mirrors the optimizer's {@code
 * UpsertTableStatsRequest} field-for-field. Duplicated here so the tables service does not take a
 * compile-time dependency on the optimizer jar; keep in sync with that type.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OptimizerStatsRequest {

  private String databaseName;
  private String tableName;
  private Stats stats;
  private Map<String, String> tableProperties;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Stats {
    private Snapshot snapshot;
    private Delta delta;
  }

  /**
   * Point-in-time snapshot metrics. Map to optimizer's {@code
   * TableStatsPayload.SnapshotMetricsDto}.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Snapshot {
    /**
     * Iceberg snapshot ID. Sent so the optimizer can use it as an idempotency token on upsert and
     * reject out-of-order replays — server-side wiring tracked in BDP-102985.
     */
    private Long snapshotId;

    private String tableVersion;
    private String tableLocation;
    private Long tableSizeBytes;
    private Long numCurrentFiles;
  }

  /**
   * Per-commit incremental counters. Map to optimizer's {@code TableStatsPayload.CommitDeltaDto}.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Delta {
    private Long numFilesAdded;
    private Long numFilesDeleted;
    private Long addedSizeBytes;
    private Long deletedSizeBytes;
  }
}
