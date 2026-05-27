package com.linkedin.openhouse.tables.services.optimizer;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// Wire body for PUT /v1/optimizer/stats/{tableUuid}.
//
// This mirrors the optimizer's UpsertTableStatsRequest field-for-field. The type is duplicated
// here so that the tables service does not take a compile-time dependency on the optimizer jar.
// Keep both copies in sync.
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

  // Point-in-time snapshot metrics. Maps to the optimizer's
  // TableStatsPayload.SnapshotMetricsDto.
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Snapshot {
    // Iceberg snapshot ID. Sent so the optimizer can use it as an idempotency token on upsert and
    // reject out-of-order replays. Server-side wiring is tracked in BDP-102985.
    private Long snapshotId;

    private String tableVersion;
    private String tableLocation;
    private Long tableSizeBytes;
    private Long numCurrentFiles;
  }

  // Per-commit incremental counters. Maps to the optimizer's TableStatsPayload.CommitDeltaDto.
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
