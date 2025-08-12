package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

/**
 * Class with main entry point to run as an orphan table dirs deletion job.
 *
 * <p>Example of invocation:
 * com.linkedin.openhouse.jobs.spark.OrphanTableDirectoriesDeletionSparkApp --tableDirectoryPath
 * tableDirectoryPath
 */
@Slf4j
public class OrphanTableDirectoryDeletionSparkApp extends BaseTableDirectorySparkApp {
  private final String trashDir;
  private final int orphanOlderThanDays;
  private final int stagedDeleteOrderThanDays;

  public OrphanTableDirectoryDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      Path tableDirectoryPath,
      String trashDir,
      int orphanOlderThanDays,
      int stagedDeleteOrderThanDays,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, tableDirectoryPath, otelEmitter);
    this.trashDir = trashDir;
    this.orphanOlderThanDays = orphanOlderThanDays;
    this.stagedDeleteOrderThanDays = stagedDeleteOrderThanDays;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    log.info(
        "Orphan directories deletion app start triggered by table directory path {}",
        tableDirectoryPath);
    long orphanThresholdMillis =
        System.currentTimeMillis() - TimeUnit.DAYS.toMillis(orphanOlderThanDays);
    if (ops.deleteOrphanDirectory(tableDirectoryPath, trashDir, orphanThresholdMillis)) {
      log.info(
          "Staged table directory path {}; timeForSelection {}",
          tableDirectoryPath,
          orphanOlderThanDays);
      otelEmitter.count(
          METRICS_SCOPE,
          AppConstants.ORPHAN_DIRECTORY_COUNT,
          1,
          Attributes.of(
              AttributeKey.stringKey(AppConstants.TABLE_DIRECTORY_PATH),
              tableDirectoryPath.toString()));
    } else {
      log.info("Staged directories deletion by table directory path {}", tableDirectoryPath);
      long deleteThresholdMillis =
          orphanThresholdMillis - TimeUnit.DAYS.toMillis(stagedDeleteOrderThanDays);
      ops.deleteStagedOrphanDirectory(tableDirectoryPath, trashDir, deleteThresholdMillis);
      log.info(
          "Deleted table directory path {}; timeForSelection {}",
          tableDirectoryPath,
          stagedDeleteOrderThanDays);
      otelEmitter.count(
          METRICS_SCOPE,
          AppConstants.STAGED_DIRECTORY_COUNT,
          1,
          Attributes.of(
              AttributeKey.stringKey(AppConstants.TABLE_DIRECTORY_PATH),
              tableDirectoryPath.toString()));
    }
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static OrphanTableDirectoryDeletionSparkApp createApp(
      String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableDirectoryPath", true, "Path to the directory"));
    extraOptions.add(new Option("b", "trashDir", false, "Trash dir to perform delete action"));
    extraOptions.add(new Option("o", "orphanDaysOld", false, "Days old files are staged"));
    extraOptions.add(new Option("d", "stagedDeleteDaysOld", false, "Days old files are deleted"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new OrphanTableDirectoryDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        new Path(cmdLine.getOptionValue("tableDirectoryPath")),
        cmdLine.getOptionValue("trashDir", ".trash"),
        Integer.parseInt(cmdLine.getOptionValue("orphanDaysOld", "7")),
        Integer.parseInt(cmdLine.getOptionValue("stagedDeleteDaysOld", "3")),
        otelEmitter);
  }
}
