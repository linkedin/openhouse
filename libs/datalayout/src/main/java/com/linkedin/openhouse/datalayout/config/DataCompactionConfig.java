package com.linkedin.openhouse.datalayout.config;

import lombok.Builder;
import lombok.Data;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;

@Data
@Builder
public final class DataCompactionConfig {
  public static final long TARGET_BYTE_SIZE_DEFAULT =
      TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
  public static final double MIN_BYTE_SIZE_RATIO_DEFAULT =
      BinPackStrategy.MIN_FILE_SIZE_DEFAULT_RATIO;
  public static final double MAX_BYTE_SIZE_RATIO_DEFAULT =
      BinPackStrategy.MAX_FILE_SIZE_DEFAULT_RATIO;
  public static final int MIN_INPUT_FILES_DEFAULT = BinPackStrategy.MIN_INPUT_FILES_DEFAULT;
  public static final int MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT =
      RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT;
  public static final int PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT =
      RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT;
  public static final long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT =
      RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT;

  public static final int DELETE_FILE_THRESHOLD_DEFAULT =
      BinPackStrategy.DELETE_FILE_THRESHOLD_DEFAULT;

  @Builder.Default private long targetByteSize = TARGET_BYTE_SIZE_DEFAULT;
  @Builder.Default private double minByteSizeRatio = MIN_BYTE_SIZE_RATIO_DEFAULT;
  @Builder.Default private double maxByteSizeRatio = MAX_BYTE_SIZE_RATIO_DEFAULT;
  @Builder.Default private int minInputFiles = MIN_INPUT_FILES_DEFAULT;

  @Builder.Default
  private int maxConcurrentFileGroupRewrites = MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT;

  @Builder.Default private boolean partialProgressEnabled = true;
  @Builder.Default private int partialProgressMaxCommits = PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT;
  @Builder.Default private long maxFileGroupSizeBytes = MAX_FILE_GROUP_SIZE_BYTES_DEFAULT;
  @Builder.Default private int deleteFileThreshold = DELETE_FILE_THRESHOLD_DEFAULT;
}
