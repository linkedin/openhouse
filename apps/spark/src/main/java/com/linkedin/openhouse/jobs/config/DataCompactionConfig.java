package com.linkedin.openhouse.jobs.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;

@ToString
@Builder
@Getter
public final class DataCompactionConfig {
  public static final long DEFAULT_TARGET_BYTE_SIZE =
      TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
  public static final double DEFAULT_MIN_BYTE_SIZE_RATIO =
      BinPackStrategy.MIN_FILE_SIZE_DEFAULT_RATIO;
  public static final double DEFAULT_MAX_BYTE_SIZE_RATIO =
      BinPackStrategy.MAX_FILE_SIZE_DEFAULT_RATIO;
  public static final int DEFAULT_MIN_INPUT_FILES = BinPackStrategy.MIN_INPUT_FILES_DEFAULT;
  public static final int DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES =
      RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT;
  public static final int DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS =
      RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT;

  private long targetByteSize;
  private long minByteSize;
  private long maxByteSize;
  private int minInputFiles;
  private int maxConcurrentFileGroupRewrites;
  private boolean partialProgressEnabled;
  private int partialProgressMaxCommits;
}
