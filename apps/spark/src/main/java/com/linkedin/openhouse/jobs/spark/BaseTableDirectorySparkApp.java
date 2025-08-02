package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import org.apache.hadoop.fs.Path;

/** Base table directory app class implemented for table directory specific operations */
public abstract class BaseTableDirectorySparkApp extends BaseSparkApp {
  protected final Path tableDirectoryPath;

  protected BaseTableDirectorySparkApp(
      String jobId, StateManager stateManager, Path tableDirectoryPath, OtelEmitter otelEmitter) {
    super(jobId, stateManager, otelEmitter);
    this.tableDirectoryPath = tableDirectoryPath;
  }
}
