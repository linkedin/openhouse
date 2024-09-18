package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.Data;

/** Compaction event to be logged as part of the table properties. */
@Data
@Builder
public class CompactionEvent {
  private final long compactionTimestamp;
  private final long addedDataFilesCount;
  private final long rewrittenDataFilesCount;
  private final long rewrittenBytesCount;
}
