package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

/**
 * Class with main entry point to run a table staged files deletion job.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.StagedFilesDeletionSparkApp
 * --tableName db.testTable --trashDir .trash --daysOld 3 --recursive true
 */
@Slf4j
public class StagedFilesDeletionSparkApp extends BaseTableSparkApp {
  private final String trashDir;
  private final int olderThanDays;
  private final boolean recursive;

  public StagedFilesDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      String trashDir,
      int olderThanDays,
      boolean recursive) {
    super(jobId, stateManager, fqtn);
    this.trashDir = trashDir;
    this.olderThanDays = olderThanDays;
    this.recursive = recursive;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    log.info("Staged files deletion app start for table");
    Path trashPathForTable = new Path(ops.getTable(fqtn).location(), trashDir);
    List<Path> deletedFiles = ops.deleteStagedFiles(trashPathForTable, olderThanDays, recursive);
    log.info("Deleted {} staged files", deletedFiles.size());
    METER
        .counterBuilder(AppConstants.STAGED_FILE_COUNT)
        .build()
        .add(
            deletedFiles.size(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("b", "trashDir", false, "Base dir to perform delete action"));
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option("o", "daysOld", false, "Days old files are deleted"));
    extraOptions.add(
        new Option("r", "recursive", false, "Delete files recursively from <trashDir>"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    StagedFilesDeletionSparkApp app =
        new StagedFilesDeletionSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("trashDir", ".trash"),
            Integer.parseInt(cmdLine.getOptionValue("daysOld", "3")),
            Boolean.parseBoolean(cmdLine.getOptionValue("recursive", "true")));
    app.run();
  }
}
