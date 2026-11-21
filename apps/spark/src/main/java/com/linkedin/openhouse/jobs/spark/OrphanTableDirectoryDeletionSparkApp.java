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
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

/**
 * Class with main entry point to run as an orphan table directory deletion job. Stages a directory
 * by moving its files under its trash subdirectory once none of them was modified for {@code
 * --orphanDaysOld} days, and deletes a staged directory once it has been staged and unmodified for
 * {@code --orphanDaysOld} plus {@code --stagedDeleteDaysOld} days.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.OrphanTableDirectoryDeletionSparkApp
 * --tableDirectoryPath tableDirectoryPath --trashDir .trash --orphanDaysOld 7 --stagedDeleteDaysOld
 * 3
 */
@Slf4j
public class OrphanTableDirectoryDeletionSparkApp extends BaseTableDirectorySparkApp {
  private static final String DEFAULT_TRASH_DIR = ".trash";
  private static final int DEFAULT_ORPHAN_DAYS_OLD = 7;
  private static final int DEFAULT_STAGED_DELETE_DAYS_OLD = 3;
  private static final Pattern TRASH_DIR_PATTERN = Pattern.compile("[\\w.-]+");

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
    // Files are moved to tableDirectoryPath/trashDir, which must be inside the table directory.
    if (!TRASH_DIR_PATTERN.matcher(trashDir).matches() || trashDir.matches("\\.+")) {
      throw new IllegalArgumentException("trashDir must be a subdirectory name, got: " + trashDir);
    }
    // A table's files are written before the table is registered, so they must not be staged
    // right away.
    checkDays("orphanDaysOld", orphanOlderThanDays, 1);
    checkDays("stagedDeleteDaysOld", stagedDeleteOrderThanDays, 0);
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
      if (ops.deleteStagedOrphanDirectory(tableDirectoryPath, trashDir, deleteThresholdMillis)) {
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
    extraOptions.add(
        new Option(
            "b",
            "trashDir",
            true,
            "Subdirectory to stage files under, defaults to " + DEFAULT_TRASH_DIR));
    extraOptions.add(
        new Option(
            "o",
            "orphanDaysOld",
            true,
            "Stage a directory once none of its files was modified for this many days, defaults to "
                + DEFAULT_ORPHAN_DAYS_OLD));
    extraOptions.add(
        new Option(
            "s",
            "stagedDeleteDaysOld",
            true,
            "Delete a staged directory once it has been staged and unmodified for orphanDaysOld"
                + " plus this many days, defaults to "
                + DEFAULT_STAGED_DELETE_DAYS_OLD));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new OrphanTableDirectoryDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        new Path(cmdLine.getOptionValue("tableDirectoryPath")),
        cmdLine.getOptionValue("trashDir", DEFAULT_TRASH_DIR),
        getInt(cmdLine, "orphanDaysOld", DEFAULT_ORPHAN_DAYS_OLD),
        getInt(cmdLine, "stagedDeleteDaysOld", DEFAULT_STAGED_DELETE_DAYS_OLD),
        otelEmitter);
  }

  private static int getInt(CommandLine cmdLine, String option, int defaultValue) {
    String value = cmdLine.getOptionValue(option);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  private static void checkDays(String name, int days, int minDays) {
    if (days < minDays) {
      throw new IllegalArgumentException(
          String.format("%s must be at least %d, got: %d", name, minDays, days));
    }
  }
}
