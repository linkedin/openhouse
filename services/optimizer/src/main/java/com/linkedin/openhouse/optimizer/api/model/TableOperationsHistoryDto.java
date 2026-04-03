package com.linkedin.openhouse.optimizer.api.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DTO for {@code table_operations_history} — append-only Spark job results. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationsHistoryDto {

  /** Same UUID as the originating {@code table_operations.id}; supplied by the caller. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  private String databaseName;
  private String tableName;
  private OperationType operationType;

  /** When the Spark job was submitted / ran. */
  private Instant submittedAt;

  /** {@code SUCCESS} or {@code FAILED}. */
  private OperationHistoryStatus status;

  /** Spark job ID. */
  private String jobId;

  /** Job result payload; both fields null on success. */
  private JobResult result;

  /** Number of orphan files deleted; null for non-OFD operations or before completion. */
  private Integer orphanFilesDeleted;

  /** Bytes reclaimed by orphan file deletion; null for non-OFD operations. */
  private Long orphanBytesDeleted;
}
