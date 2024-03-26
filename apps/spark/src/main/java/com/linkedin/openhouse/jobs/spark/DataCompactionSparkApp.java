package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.config.DataCompactionConfig;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.List;
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
            config.getTargetByteSize(),
            config.getMinByteSize(),
            config.getMaxByteSize(),
            config.getMinInputFiles(),
            config.getMaxConcurrentFileGroupRewrites(),
            config.isPartialProgressEnabled(),
            config.getPartialProgressMaxCommits());
    log.info(
        "Added {} data files, rewritten {} data files, rewritten {} bytes",
        result.addedDataFilesCount(),
        result.rewrittenDataFilesCount(),
        result.rewrittenBytesCount());
    log.info("Processed {} file groups", result.rewriteResults().size());
    for (RewriteDataFiles.FileGroupRewriteResult fileGroupRewriteResult : result.rewriteResults()) {
      log.info(
          "File group {} has {} added files, {} rewritten files, {} rewritten bytes",
          Operations.groupInfoToString(fileGroupRewriteResult.info()),
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
                    cmdLine.getOptionValue("targetByteSize"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig
                        .DEFAULT_TARGET_BYTE_SIZE))
            .minByteSize(
                NumberUtils.toLong(
                    cmdLine.getOptionValue("minByteSize"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig.DEFAULT_MIN_BYTE_SIZE))
            .maxByteSize(
                NumberUtils.toLong(
                    cmdLine.getOptionValue("maxByteSize"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig.DEFAULT_MAX_BYTE_SIZE))
            .minInputFiles(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("minInputFiles"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig
                        .DEFAULT_MIN_INPUT_FILES))
            .maxConcurrentFileGroupRewrites(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("maxConcurrentFileGroupRewrites"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig
                        .DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES))
            .partialProgressEnabled(cmdLine.hasOption("partialProgressEnabled"))
            .partialProgressMaxCommits(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("partialProgressMaxCommits"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig
                        .DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS))
            .build());
  }
}
