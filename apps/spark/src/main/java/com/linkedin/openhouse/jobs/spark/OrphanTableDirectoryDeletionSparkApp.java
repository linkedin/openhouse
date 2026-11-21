package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.DirectoryHold;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Class with main entry point to run as an orphan table directory deletion job. Acts on a directory
 * once none of its files was modified for {@code --orphanDaysOld} days, as set by {@code --action}:
 *
 * <ul>
 *   <li>{@code stage}, the default, moves its files under its trash subdirectory, and deletes it
 *       once it has been staged and unmodified for {@code --orphanDaysOld} plus {@code
 *       --stagedDeleteDaysOld} days.
 *   <li>{@code hold} removes all permissions on it, for a superuser to delete it after {@code
 *       --holdDays} days. Unlike staging, it works when the job cannot move files written by
 *       others, and the directory's owner can release it by setting its previous permission back.
 *       If {@code --heldTableName} is set, records the hold in that table before holding the
 *       directory, so that every held directory is listed there.
 * </ul>
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.OrphanTableDirectoryDeletionSparkApp
 * --tableDirectoryPath tableDirectoryPath --trashDir .trash --orphanDaysOld 7 --stagedDeleteDaysOld
 * 3
 *
 * <p>Example of invocation to hold directories:
 * com.linkedin.openhouse.jobs.spark.OrphanTableDirectoryDeletionSparkApp --tableDirectoryPath
 * tableDirectoryPath --orphanDaysOld 7 --action hold --holdDays 7 --heldTableName
 * db.held_directories
 */
@Slf4j
public class OrphanTableDirectoryDeletionSparkApp extends BaseTableDirectorySparkApp {
  private static final String DEFAULT_TRASH_DIR = ".trash";
  private static final int DEFAULT_ORPHAN_DAYS_OLD = 7;
  private static final int DEFAULT_STAGED_DELETE_DAYS_OLD = 3;
  private static final int DEFAULT_HOLD_DAYS = 7;
  private static final Pattern TRASH_DIR_PATTERN = Pattern.compile("[\\w.-]+");
  private static final Pattern HELD_TABLE_NAME_PATTERN = Pattern.compile("\\w+\\.\\w+");

  /** Source of the holds this app records, to tell them from holds recorded otherwise. */
  static final String HOLD_SOURCE = "JOB";

  static final StructType HELD_TABLE_SCHEMA =
      new StructType()
          .add("path", DataTypes.StringType)
          .add("table_uuid", DataTypes.StringType)
          .add("owner", DataTypes.StringType)
          .add("group_name", DataTypes.StringType)
          .add("previous_permission", DataTypes.StringType)
          .add("held_at", DataTypes.TimestampType)
          .add("delete_after", DataTypes.TimestampType)
          .add("job_id", DataTypes.StringType)
          .add("hold_source", DataTypes.StringType);

  /** What to do with an orphan table directory once none of its files was recently modified. */
  public enum Action {
    STAGE,
    HOLD
  }

  private final String trashDir;
  private final int orphanOlderThanDays;
  private final int stagedDeleteOrderThanDays;
  private final Action action;
  private final int holdDays;
  private final @Nullable String heldTableName;

  public OrphanTableDirectoryDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      Path tableDirectoryPath,
      String trashDir,
      int orphanOlderThanDays,
      int stagedDeleteOrderThanDays,
      OtelEmitter otelEmitter) {
    this(
        jobId,
        stateManager,
        tableDirectoryPath,
        trashDir,
        orphanOlderThanDays,
        stagedDeleteOrderThanDays,
        Action.STAGE,
        DEFAULT_HOLD_DAYS,
        null,
        otelEmitter);
  }

  public OrphanTableDirectoryDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      Path tableDirectoryPath,
      String trashDir,
      int orphanOlderThanDays,
      int stagedDeleteOrderThanDays,
      Action action,
      int holdDays,
      @Nullable String heldTableName,
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
    checkDays("holdDays", holdDays, 0);
    // The name is put in SQL statements.
    if (heldTableName != null && !HELD_TABLE_NAME_PATTERN.matcher(heldTableName).matches()) {
      throw new IllegalArgumentException(
          "heldTableName must be <database>.<table>, got: " + heldTableName);
    }
    this.trashDir = trashDir;
    this.orphanOlderThanDays = orphanOlderThanDays;
    this.stagedDeleteOrderThanDays = stagedDeleteOrderThanDays;
    this.action = action;
    this.holdDays = holdDays;
    this.heldTableName = heldTableName;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    log.info(
        "Orphan directories deletion app start triggered by table directory path {}",
        tableDirectoryPath);
    long orphanThresholdMillis =
        System.currentTimeMillis() - TimeUnit.DAYS.toMillis(orphanOlderThanDays);
    if (action == Action.HOLD) {
      hold(ops, orphanThresholdMillis);
    } else if (ops.deleteOrphanDirectory(tableDirectoryPath, trashDir, orphanThresholdMillis)) {
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

  private void hold(Operations ops, long orphanThresholdMillis) throws Exception {
    Optional<DirectoryHold> hold =
        ops.prepareOrphanDirectoryHold(tableDirectoryPath, trashDir, orphanThresholdMillis);
    if (!hold.isPresent()) {
      return;
    }
    // Recorded first, so that no directory is held without a record. A directory recorded but not
    // held is held and recorded again by a later run.
    if (heldTableName != null) {
      recordHold(ops.spark(), hold.get());
    }
    ops.holdDirectory(hold.get(), jobId);
    log.info(
        "Held table directory path {}; timeForSelection {}",
        tableDirectoryPath,
        orphanOlderThanDays);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.HELD_DIRECTORY_COUNT,
        1,
        Attributes.of(
            AttributeKey.stringKey(AppConstants.TABLE_DIRECTORY_PATH),
            tableDirectoryPath.toString()));
  }

  private void recordHold(SparkSession spark, DirectoryHold hold) throws NoSuchTableException {
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s)", heldTableName, HELD_TABLE_SCHEMA.toDDL()));
    long deleteAfterMillis = hold.getHeldAtMillis() + TimeUnit.DAYS.toMillis(holdDays);
    spark
        .createDataFrame(
            Collections.singletonList(
                RowFactory.create(
                    hold.getPath().toUri().getPath(),
                    hold.getTableUuid(),
                    hold.getOwner(),
                    hold.getGroup(),
                    hold.getPreviousPermission(),
                    new Timestamp(hold.getHeldAtMillis()),
                    new Timestamp(deleteAfterMillis),
                    jobId,
                    HOLD_SOURCE)),
            HELD_TABLE_SCHEMA)
        .writeTo(heldTableName)
        .append();
    log.info("Recorded hold of {} in {}", hold.getPath(), heldTableName);
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
    extraOptions.add(
        new Option(
            null,
            "action",
            true,
            "What to do with a directory once none of its files was modified for orphanDaysOld"
                + " days: stage, the default, or hold"));
    extraOptions.add(
        new Option(
            null,
            "holdDays",
            true,
            "With --action hold, days to keep a held directory before deleting it, recorded in"
                + " heldTableName, defaults to "
                + DEFAULT_HOLD_DAYS));
    extraOptions.add(
        new Option(
            null,
            "heldTableName",
            true,
            "With --action hold, fully-qualified name of the table to record holds in, created if"
                + " needed"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    Action action = getAction(cmdLine);
    if (action != Action.HOLD
        && (cmdLine.hasOption("holdDays") || cmdLine.hasOption("heldTableName"))) {
      throw new IllegalArgumentException("holdDays and heldTableName require --action hold");
    }
    return new OrphanTableDirectoryDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        new Path(cmdLine.getOptionValue("tableDirectoryPath")),
        cmdLine.getOptionValue("trashDir", DEFAULT_TRASH_DIR),
        getInt(cmdLine, "orphanDaysOld", DEFAULT_ORPHAN_DAYS_OLD),
        getInt(cmdLine, "stagedDeleteDaysOld", DEFAULT_STAGED_DELETE_DAYS_OLD),
        action,
        getInt(cmdLine, "holdDays", DEFAULT_HOLD_DAYS),
        cmdLine.getOptionValue("heldTableName"),
        otelEmitter);
  }

  private static Action getAction(CommandLine cmdLine) {
    String value = cmdLine.getOptionValue("action", Action.STAGE.name());
    try {
      return Action.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("action must be stage or hold, got: " + value, e);
    }
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
