package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.PartitionWhereClauseBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expression;

/**
 * Spark app that compacts data files of a set of partitions in a table. Unlike {@link
 * DataCompactionSparkApp}, which rewrites the whole table, this app accepts the partition column
 * names and a list of partition-value tuples, builds a {@code WHERE} clause selecting those
 * partitions, and runs a filtered {@code rewriteDataFiles}.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.PartitionDataCompactionSparkApp
 * --tableName db.testTable --partitionColumns date,hour,late --partitionValues a,b,c
 * --partitionValues a,b,d --targetByteSize 1048576 --maxByteSizeRatio 1.8 --minInputFiles 5
 */
@Slf4j
public class PartitionDataCompactionSparkApp extends BaseTableSparkApp {
  private final DataCompactionConfig config;
  private final String partitionColumns;
  private final List<String> partitionValues;

  protected PartitionDataCompactionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      DataCompactionConfig config,
      String partitionColumns,
      List<String> partitionValues,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
    this.config = config;
    this.partitionColumns = partitionColumns;
    this.partitionValues = partitionValues;
  }

  @Override
  protected void runInner(Operations ops) {
    String whereClause = PartitionWhereClauseBuilder.build(partitionColumns, partitionValues);
    log.info(
        "Partition rewrite data files app start for table {}, partitions {} (where {}), config {}",
        fqtn,
        partitionValues,
        whereClause,
        config);
    Expression filter = ops.toIcebergExpression(fqtn, whereClause);
    RewriteDataFiles.Result result =
        ops.rewriteDataFiles(
            ops.getTable(fqtn),
            config.getTargetByteSize(),
            (long) (config.getTargetByteSize() * config.getMinByteSizeRatio()),
            (long) (config.getTargetByteSize() * config.getMaxByteSizeRatio()),
            config.getMinInputFiles(),
            config.getMaxConcurrentFileGroupRewrites(),
            config.isPartialProgressEnabled(),
            config.getPartialProgressMaxCommits(),
            config.getDeleteFileThreshold(),
            filter);
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
    Attributes tableAttributes =
        Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.ADDED_DATA_FILE_COUNT,
        result.addedDataFilesCount(),
        tableAttributes);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.REWRITTEN_DATA_FILE_COUNT,
        result.rewrittenDataFilesCount(),
        tableAttributes);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.REWRITTEN_DATA_FILE_BYTES,
        result.rewrittenBytesCount(),
        tableAttributes);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.REWRITTEN_DATA_FILE_GROUP_COUNT,
        result.rewriteResults().size(),
        tableAttributes);
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static PartitionDataCompactionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option(
            null,
            "partitionColumns",
            true,
            "Comma-separated partition column names, e.g. date,hour,late"));
    extraOptions.add(
        Option.builder(null)
            .longOpt("partitionValues")
            .hasArgs()
            .desc(
                "Partition value tuple aligned with partitionColumns, e.g. a,b,c. "
                    + "Repeat the flag (or pass multiple tuples) to compact multiple partitions.")
            .build());
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
        new Option(
            null,
            "deleteFileThreshold",
            true,
            "Minimum number of deletes that needs to be associated with a data file for it to be considered for rewriting"));

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
    DataCompactionConfig config =
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
            .deleteFileThreshold(
                NumberUtils.toInt(
                    cmdLine.getOptionValue("deleteFileThreshold"),
                    DataCompactionConfig.DELETE_FILE_THRESHOLD_DEFAULT))
            .build();

    String[] partitionValues = cmdLine.getOptionValues("partitionValues");
    if (partitionValues == null || partitionValues.length == 0) {
      throw new RuntimeException("At least one --partitionValues tuple is required");
    }
    return new PartitionDataCompactionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        config,
        cmdLine.getOptionValue("partitionColumns"),
        Collections.unmodifiableList(Arrays.asList(partitionValues)),
        otelEmitter);
  }
}
