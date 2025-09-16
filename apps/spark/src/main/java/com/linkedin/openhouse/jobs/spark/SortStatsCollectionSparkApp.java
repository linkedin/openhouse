package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SortStatsCollectionSparkApp extends BaseTableSparkApp {
  private static final int TARGET_FILE_SIZE = 512 * 1024 * 1024; // 512MB
  private static final String SORT_ORDER =
      "header.memberId asc nulls last, header.time asc nulls last";
  private static final String COMPRESSION_RATE_KEY = "sort-compression-rate";

  public SortStatsCollectionSparkApp(
      String jobId, StateManager stateManager, String fqtn, OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static SortStatsCollectionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new SortStatsCollectionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        otelEmitter);
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    SparkSession spark = ops.spark();
    String tempTableName = getTempTableName(fqtn);
    String datepartition = getDatePartition();
    try {
      // sample average record size from table and calculate rows needed for target file size
      long avgRecordSize =
          spark
              .sql(
                  String.format(
                      "select sum(file_size_in_bytes) * 1.0 / sum(record_count) as avg_bytes_per_record "
                          + "from openhouse.%s.data_files limit 10",
                      fqtn))
              .first()
              .getDecimal(0)
              .longValue();
      int numOfRowsNeeded = (int) (TARGET_FILE_SIZE / avgRecordSize);
      log.info(
          "Avg record size for table {} is {}, copy {} rows for sort stats collection",
          fqtn,
          avgRecordSize,
          numOfRowsNeeded);
      // create a temporary table with limited rows for the given partition
      // if the partition doesn't provide enough rows, the whole partition will be copied
      spark.sql(
          String.format(
              "create table openhouse.%s as select * from spark_catalog.%s where datepartition = '%s' limit %d",
              tempTableName, fqtn, datepartition, numOfRowsNeeded));
      double sizeBefore =
          spark
              .sql(
                  String.format(
                      "select sum(file_size_in_bytes)/1000/1000 from openhouse.%s.data_files",
                      tempTableName))
              .first()
              .getDouble(0);
      // call rewrite procedure to rewrite data files with sort strategy
      String rewriteOptions = getRewriteOptions();
      spark.sql(
          String.format(
              "call openhouse.system.rewrite_data_files("
                  + "table => '%s', options => map(%s), strategy => 'sort', sort_order => '%s')",
              tempTableName, rewriteOptions, SORT_ORDER));
      double sizeAfter =
          spark
              .sql(
                  String.format(
                      "select sum(file_size_in_bytes)/1000/1000 from openhouse.%s.data_files",
                      tempTableName))
              .first()
              .getDouble(0);
      // calculate compression rate and store it as table property
      double compressionRate = (sizeBefore - sizeAfter) / sizeBefore * 100;
      spark.sql(
          String.format(
              "alter table openhouse.%s set tblproperties ('%s'='%f')",
              fqtn, COMPRESSION_RATE_KEY, compressionRate));
      log.info(
          "Sort stats collection for table {} completed. Size before: {} MB, size after: {} MB, compression rate: {}%",
          fqtn, sizeBefore, sizeAfter, String.format("%.2f", compressionRate));
    } catch (Exception e) {
      log.error("Error during sort stats collection for table {}", fqtn, e);
      throw e;
    } finally {
      spark.sql(String.format("drop table if exists openhouse.%s", tempTableName));
    }
  }

  private String getTempTableName(String fqtn) {
    return fqtn + "_sample";
  }

  private String getDatePartition() {
    LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    return yesterday.format(formatter);
  }

  private String getRewriteOptions() {
    Map<String, String> options = new HashMap();
    options.put("target-file-size-bytes", String.valueOf(TARGET_FILE_SIZE));
    options.put("rewrite-all", "true");
    options.put("partial-progress.enabled", "true");

    return options.entrySet().stream()
        .map(entry -> String.format("'%s', '%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", "));
  }
}
