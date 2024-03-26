package com.linkedin.openhouse.jobs.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Builder
@Getter
public final class DataCompactionConfig {
  // Default values for the compaction configuration match default values in the iceberg library
  // see https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files
  public static final long MB = 1024 * 1024;
  public static final long DEFAULT_TARGET_BYTE_SIZE = MB * 512;
  public static final long DEFAULT_MIN_BYTE_SIZE = (long) (DEFAULT_TARGET_BYTE_SIZE * 0.75);
  public static final long DEFAULT_MAX_BYTE_SIZE = (long) (DEFAULT_TARGET_BYTE_SIZE * 1.8);
  public static final int DEFAULT_MIN_INPUT_FILES = 5;
  public static final int DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES = 5;
  public static final int DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS = 10;

  private long targetByteSize;
  private long minByteSize;
  private long maxByteSize;
  private int minInputFiles;
  private int maxConcurrentFileGroupRewrites;
  private boolean partialProgressEnabled;
  private int partialProgressMaxCommits;
}
