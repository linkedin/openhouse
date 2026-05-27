package com.linkedin.openhouse.tables.services.optimizer;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wire body for {@code PUT /v1/optimizer/stats/{tableUuid}}. Mirrors the optimizer's {@code
 * UpsertTableStatsRequest} field-for-field — see {@code
 * services/optimizer/src/main/java/com/linkedin/openhouse/optimizer/api/spec/UpsertTableStatsRequest.java}.
 *
 * <p>Tables service owns its own copy so that the wire contract is explicit at the call site and
 * the optimizer client jar is not a compile-time dependency.
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
