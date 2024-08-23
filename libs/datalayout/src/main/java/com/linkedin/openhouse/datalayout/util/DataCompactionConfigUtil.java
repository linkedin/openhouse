package com.linkedin.openhouse.datalayout.util;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;

/** Creates a default data compaction config to be used for data layout compaction strategies. */
public class DataCompactionConfigUtil {
  private static final long NUM_FILE_GROUPS_PER_COMMIT = 100;
  private static final long MAX_NUM_COMMITS = 30;
  private static final long MAX_BYTES_SIZE_RATIO = 10;
  private static final int MAX_CONCURRENT_FILE_GROUP_REWRITES = 50;
  private static final int REWRITE_PARALLELISM = 900; // number of Spark tasks to run in parallel

  public static DataCompactionConfig createDefaultCompactionConfig(
      long targetBytesSize, long tablePartitionStatsCount, long rewriteFileBytes) {
    DataCompactionConfig.DataCompactionConfigBuilder configBuilder = DataCompactionConfig.builder();

    configBuilder.targetByteSize(targetBytesSize);

    long estimatedFileGroupsCount =
        Math.max(
            tablePartitionStatsCount,
            rewriteFileBytes / DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    int maxCommitsCount =
        (int)
            Math.min(
                MAX_NUM_COMMITS,
                (estimatedFileGroupsCount + NUM_FILE_GROUPS_PER_COMMIT - 1)
                    / NUM_FILE_GROUPS_PER_COMMIT);
    configBuilder.partialProgressMaxCommits(maxCommitsCount);

    int estimatedTasksPerFileGroupCount =
        (int) (DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT / targetBytesSize);

    configBuilder.maxConcurrentFileGroupRewrites(
        Math.min(
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            (estimatedTasksPerFileGroupCount + REWRITE_PARALLELISM - 1)
                / estimatedTasksPerFileGroupCount));

    // don't split large files
    configBuilder.maxByteSizeRatio(MAX_BYTES_SIZE_RATIO);
    return configBuilder.build();
  }
}
