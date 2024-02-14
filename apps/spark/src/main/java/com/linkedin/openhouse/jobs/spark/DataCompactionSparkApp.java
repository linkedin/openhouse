package com.linkedin.openhouse.jobs.spark;

import static com.linkedin.openhouse.jobs.spark.Operations.*;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.actions.RewriteDataFiles;

/**
 * Spark app that compacts data files in a table to optimize the file sizes and number of files.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.DataCompactionSparkApp --tableName
 * db.testTable --targetByteSize 1048576 --minByteSize 786432 --maxByteSize 1887436 --minInputFiles
 * 5 --maxConcurrentFileGroupRewrites 2 --partialProgressEnabled --partialProgressMaxCommits 10
 */
@Slf4j
public class DataCompactionSparkApp extends BaseTableSparkApp {
  // Default values for the compaction configuration match default values in the iceberg library
  // see https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files
  private static final long MB = 1024 * 1024;
  private static final long DEFAULT_TARGET_BYTE_SIZE = MB * 512;
  private static final long DEFAULT_MIN_BYTE_SIZE = (long) (DEFAULT_TARGET_BYTE_SIZE * 0.75);
  private static final long DEFAULT_MAX_BYTE_SIZE = (long) (DEFAULT_TARGET_BYTE_SIZE * 1.8);
  private static final int DEFAULT_MIN_INPUT_FILES = 5;
  private static final int DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES = 5;
  private static final int DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS = 10;
  private final DataCompactionConfig config;

  protected DataCompactionSparkApp(
      String jobId, StateManager stateManager, String fqtn, DataCompactionConfig config) {
    super(jobId, stateManager, fqtn);
    this.config = config;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info("Rewrite data files app start for table {}, config {}", fqtn, config);
    RewriteDataFiles.Result result =
        ops.rewriteDataFiles(
            ops.getTable(fqtn),
            config.targetByteSize,
            config.minByteSize,
            config.maxByteSize,
            config.minInputFiles,
            config.maxConcurrentFileGroupRewrites,
            config.partialProgressEnabled,
            config.partialProgressMaxCommits);
    log.info(
        "Added {} data files, rewritten {} data files, rewritten {} bytes",
        result.addedDataFilesCount(),
        result.rewrittenDataFilesCount(),
        result.rewrittenBytesCount());
    log.info("Processed {} file groups", result.rewriteResults().size());
    for (RewriteDataFiles.FileGroupRewriteResult fileGroupRewriteResult : result.rewriteResults()) {
      log.info(
          "File group {} has {} added files, {} rewritten files, {} rewritten bytes",
          groupInfoToString(fileGroupRewriteResult.info()),
          fileGroupRewriteResult.addedDataFilesCount(),
          fileGroupRewriteResult.rewrittenDataFilesCount(),
          fileGroupRewriteResult.rewrittenBytesCount());
    }
    METER
        .counterBuilder(AppConstants.ADDED_DATA_FILE_COUNT)
        .build()
        .add(
            result.addedDataFilesCount(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
    METER
        .counterBuilder(AppConstants.REWRITTEN_DATA_FILE_COUNT)
        .build()
        .add(
            result.rewrittenDataFilesCount(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
    METER
        .counterBuilder(AppConstants.REWRITTEN_DATA_FILE_BYTES)
        .build()
        .add(
            result.rewrittenDataFilesCount(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
    METER
        .counterBuilder(AppConstants.REWRITTEN_DATA_FILE_GROUP_COUNT)
        .build()
        .add(
            result.rewriteResults().size(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
  }

  public static void main(String[] args) {
    createApp(args).run();
  }

  public static DataCompactionSparkApp createApp(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option(null, "targetByteSize", true, "Target data file byte size"));
    extraOptions.add(
        new Option(
            null,
            "minByteSize",
            true,
            "Minimum data file byte size, files smaller than this will be rewritten"));
    extraOptions.add(
        new Option(
            null,
            "maxByteSize",
            true,
            "Maximum data file byte size, files larger than this will be rewritten"));
    extraOptions.add(
        new Option(
            null,
            "minInputFiles",
            true,
            "Minimum number of input files in a group sufficient for rewrite"));
    extraOptions.add(
        new Option(
            null,
            "maxConcurrentFileGroupRewrites",
            true,
            "Maximum number of file groups to be simultaneously rewritten"));
    extraOptions.add(
        new Option(
            null,
            "partialProgressEnabled",
            false,
            "Enable committing groups of files prior to the entire rewrite completing"));
    extraOptions.add(
        new Option(
            null,
            "partialProgressMaxCommits",
            true,
            "Maximum amount of commits that this rewrite is allowed to produce if partial progress is enabled"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new DataCompactionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine),
        cmdLine.getOptionValue("tableName"),
        DataCompactionConfig.builder()
            .targetByteSize(
                NumberUtils.toLong(
                    cmdLine.getOptionValue("targetByteSize"), DEFAULT_TARGET_BYTE_SIZE))
            .minByteSize(
                NumberUtils.toLong(cmdLine.getOptionValue("minByteSize"), DEFAULT_MIN_BYTE_SIZE))
            .maxByteSize(
                NumberUtils.toLong(cmdLine.getOptionValue("maxByteSize"), DEFAULT_MAX_BYTE_SIZE))
            .minInputFiles(
                NumberUtils.toInt(cmdLine.getOptionValue("minInputFiles"), DEFAULT_MIN_INPUT_FILES))
            .maxConcurrentFileGroupRewrites(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("maxConcurrentFileGroupRewrites"),
                    DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES))
            .partialProgressEnabled(cmdLine.hasOption("partialProgressEnabled"))
            .partialProgressMaxCommits(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("partialProgressMaxCommits"),
                    DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS))
            .build());
  }

  @ToString
  @Builder
  protected static class DataCompactionConfig {
    private long targetByteSize;
    private long minByteSize;
    private long maxByteSize;
    private int minInputFiles;
    private int maxConcurrentFileGroupRewrites;
    private boolean partialProgressEnabled;
    private int partialProgressMaxCommits;
  }
}
