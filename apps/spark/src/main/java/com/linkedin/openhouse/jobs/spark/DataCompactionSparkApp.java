package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.datalayout.layoutselection.DataCompactionLayout;
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
import org.apache.commons.lang.StringEscapeUtils;
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
    Gson gson = new GsonBuilder().create();
    String serializedLayout =
        ops.spark()
            .sql(String.format("SHOW TBLPROPERTIES %s ('data-layout')", fqtn))
            .collectAsList()
            .get(0)
            .getString(1);
    DataCompactionLayout dataCompactionLayout =
        gson.fromJson(StringEscapeUtils.unescapeJava(serializedLayout), DataCompactionLayout.class);
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
            "minByteSizeRatio",
            true,
            "Minimum data file byte size, files smaller than this will be rewritten"));
    extraOptions.add(
        new Option(
            null,
            "maxByteSizeRatio",
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
    extraOptions.add(
        new Option(null, "entropyThreshold", true, "Entropy threshold for file rewrite"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    long targetByteSize =
        NumberUtils.toLong(
            cmdLine.getOptionValue("targetByteSize"),
            DataCompactionConfig.DEFAULT_TARGET_BYTE_SIZE);
    double minByteSizeRatio =
        NumberUtils.toDouble(
            cmdLine.getOptionValue("minByteSizeRatio"),
            DataCompactionConfig.DEFAULT_MIN_BYTE_SIZE_RATIO);
    if (minByteSizeRatio <= 0.0 || minByteSizeRatio >= 1.0) {
      throw new RuntimeException("minByteSizeRatio must be in range (0.0, 1.0)");
    }
    double maxByteSizeRatio =
        NumberUtils.toDouble(
            cmdLine.getOptionValue("maxByteSizeRatio"),
            DataCompactionConfig.DEFAULT_MAX_BYTE_SIZE_RATIO);
    if (maxByteSizeRatio <= 1.0) {
      throw new RuntimeException("maxByteSizeRatio must be greater than 1.0");
    }
    double entropyThreshold =
        NumberUtils.toDouble(
            cmdLine.getOptionValue("entropyThreshold"),
            DataCompactionConfig.DEFAULT_ENTROPY_THRESHOLD);
    return new DataCompactionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine),
        cmdLine.getOptionValue("tableName"),
        DataCompactionConfig.builder()
            .targetByteSize(targetByteSize)
            .minByteSize((long) (targetByteSize * minByteSizeRatio))
            .maxByteSize((long) (targetByteSize * maxByteSizeRatio))
            .minInputFiles(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("minInputFiles"),
                    DataCompactionConfig.DEFAULT_MIN_INPUT_FILES))
            .maxConcurrentFileGroupRewrites(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("maxConcurrentFileGroupRewrites"),
                    DataCompactionConfig.DEFAULT_MAX_CONCURRENT_FILE_GROUP_REWRITES))
            .partialProgressEnabled(cmdLine.hasOption("partialProgressEnabled"))
            .partialProgressMaxCommits(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("partialProgressMaxCommits"),
                    com.linkedin.openhouse.jobs.config.DataCompactionConfig
                        .DEFAULT_PARTIAL_PROGRESS_MAX_COMMITS))
            .entropyThreshold(entropyThreshold)
            .build());
  }
}
