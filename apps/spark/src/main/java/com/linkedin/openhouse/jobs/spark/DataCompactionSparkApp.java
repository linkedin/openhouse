package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.CompactionEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.lang.reflect.Type;
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
 * db.testTable --targetByteSize 1048576 --maxByteSizeRatio 0.75 --maxByteSizeRatio 1.8
 * --minInputFiles 5 --maxConcurrentFileGroupRewrites 2 --partialProgressEnabled
 * --partialProgressMaxCommits 10
 */
@Slf4j
public class DataCompactionSparkApp extends BaseTableSparkApp {
  private final DataCompactionConfig config;
  public static final String DATA_LAYOUT_COMPACTION_PROPERTY_KEY = "write.data-layout.compaction";

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
            (long) (config.getTargetByteSize() * config.getMinByteSizeRatio()),
            (long) (config.getTargetByteSize() * config.getMaxByteSizeRatio()),
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
    // Add compaction timestamp to table properties.
    CompactionEvent event =
        CompactionEvent.builder()
            .compactionTimestamp(System.currentTimeMillis())
            .addedDataFilesCount(result.addedDataFilesCount())
            .rewrittenDataFilesCount(result.rewrittenDataFilesCount())
            .rewrittenBytesCount(result.rewrittenBytesCount())
            .build();
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<CompactionEvent>() {}.getType();
    ops.spark()
        .sql(
            String.format(
                "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
                ops.getTable(fqtn),
                DATA_LAYOUT_COMPACTION_PROPERTY_KEY,
                StringEscapeUtils.escapeJava(gson.toJson(event, type))));
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
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    long targetByteSize =
        NumberUtils.toLong(
            cmdLine.getOptionValue("targetByteSize"),
            DataCompactionConfig.TARGET_BYTE_SIZE_DEFAULT);
    double minByteSizeRatio =
        NumberUtils.toDouble(
            cmdLine.getOptionValue("minByteSizeRatio"),
            DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT);
    if (minByteSizeRatio <= 0.0 || minByteSizeRatio >= 1.0) {
      throw new RuntimeException("minByteSizeRatio must be in range (0.0, 1.0)");
    }
    double maxByteSizeRatio =
        NumberUtils.toDouble(
            cmdLine.getOptionValue("maxByteSizeRatio"),
            DataCompactionConfig.MAX_BYTE_SIZE_RATIO_DEFAULT);
    if (maxByteSizeRatio <= 1.0) {
      throw new RuntimeException("maxByteSizeRatio must be greater than 1.0");
    }
    return new DataCompactionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine),
        cmdLine.getOptionValue("tableName"),
        DataCompactionConfig.builder()
            .targetByteSize(targetByteSize)
            .minByteSizeRatio(minByteSizeRatio)
            .maxByteSizeRatio(maxByteSizeRatio)
            .minInputFiles(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("minInputFiles"),
                    DataCompactionConfig.MIN_INPUT_FILES_DEFAULT))
            .maxConcurrentFileGroupRewrites(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("maxConcurrentFileGroupRewrites"),
                    DataCompactionConfig.MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT))
            .partialProgressEnabled(cmdLine.hasOption("partialProgressEnabled"))
            .partialProgressMaxCommits(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("partialProgressMaxCommits"),
                    DataCompactionConfig.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT))
            .build());
  }
}
