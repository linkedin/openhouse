package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.datasource.TableSnapshotStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseDataLayoutStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDao;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DataLayoutStrategyGeneratorSparkApp extends BaseTableSparkApp {
  private @Nullable String outputFqtn;
  private @Nullable String partitionLevelOutputFqtn;

  protected DataLayoutStrategyGeneratorSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      @Nullable String outputFqtn,
      @Nullable String partitionLevelOutputFqtn,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
    this.outputFqtn = outputFqtn;
    this.partitionLevelOutputFqtn = partitionLevelOutputFqtn;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    SparkSession spark = ops.spark();
    TableFileStats tableFileStats = TableFileStats.builder().tableName(fqtn).spark(spark).build();
    TablePartitionStats tablePartitionStats =
        TablePartitionStats.builder().tableName(fqtn).spark(spark).build();
    TableSnapshotStats tableSnapshotStats =
        TableSnapshotStats.builder().tableName(fqtn).spark(spark).build();
    boolean isPartitioned = ops.getTable(fqtn).spec().isPartitioned();
    OpenHouseDataLayoutStrategyGenerator strategiesGenerator =
        OpenHouseDataLayoutStrategyGenerator.builder()
            .tableFileStats(tableFileStats)
            .tablePartitionStats(tablePartitionStats)
            .tableSnapshotStats(tableSnapshotStats)
            .partitioned(isPartitioned)
            .build();
    // Run table scope for unpartitioned table, and run both table and partition scope for
    // partitioned table
    runInnerTableScope(spark, strategiesGenerator, isPartitioned);
    if (isPartitioned) {
      runInnerPartitionScope(spark, strategiesGenerator);
    }
  }

  private void runInnerTableScope(
      SparkSession spark,
      OpenHouseDataLayoutStrategyGenerator strategiesGenerator,
      boolean isPartitioned) {
    log.info("Generating table-level strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generateTableLevelStrategies();
    log.info("Generated {} strategies for table {}", strategies.size(), fqtn);
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.save(fqtn, strategies);
    appendToDloStrategiesTable(spark, outputFqtn, strategies, false, isPartitioned);
  }

  private void runInnerPartitionScope(
      SparkSession spark, OpenHouseDataLayoutStrategyGenerator strategiesGenerator) {
    log.info("Generating partition-level strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generatePartitionLevelStrategies();
    log.info("Generated {} strategies for table {}", strategies.size(), fqtn);
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.deletePartitionScope(fqtn);
    appendToDloStrategiesTable(spark, partitionLevelOutputFqtn, strategies, true, true);
  }

  protected void appendToDloStrategiesTable(
      SparkSession spark,
      String outputFqtn,
      List<DataLayoutStrategy> strategies,
      boolean isPartitionScope,
      boolean isPartitioned) {
    if (outputFqtn != null && !strategies.isEmpty()) {
      createTableIfNotExists(spark, outputFqtn, isPartitionScope);
      List<String> rows = new ArrayList<>();
      for (DataLayoutStrategy strategy : strategies) {
        if (isPartitionScope) {
          rows.add(
              String.format(
                  "('%s', '%s', '%s', current_timestamp(), %f, %f, %f, %d, %d, %d, %d, %d, %d, %f)",
                  fqtn,
                  strategy.getPartitionId(),
                  strategy.getPartitionColumns(),
                  strategy.getCost(),
                  strategy.getGain(),
                  strategy.getEntropy(),
                  strategy.getPosDeleteFileCount(),
                  strategy.getEqDeleteFileCount(),
                  strategy.getPosDeleteFileBytes(),
                  strategy.getEqDeleteFileBytes(),
                  strategy.getPosDeleteRecordCount(),
                  strategy.getEqDeleteRecordCount(),
                  strategy.getFileCountReductionPenalty()));
        } else {
          rows.add(
              String.format(
                  "('%s', current_timestamp(), %f, %f, %f, %d, %d, %d, %d, %d, %d, %b, %f)",
                  fqtn,
                  strategy.getCost(),
                  strategy.getGain(),
                  strategy.getEntropy(),
                  strategy.getPosDeleteFileCount(),
                  strategy.getEqDeleteFileCount(),
                  strategy.getPosDeleteFileBytes(),
                  strategy.getEqDeleteFileBytes(),
                  strategy.getPosDeleteRecordCount(),
                  strategy.getEqDeleteRecordCount(),
                  isPartitioned,
                  strategy.getFileCountReductionPenalty()));
        }
      }
      String strategiesInsertStmt =
          String.format("INSERT INTO %s VALUES %s", outputFqtn, String.join(", ", rows));
      log.info("Running {}", strategiesInsertStmt);
      spark.sql(strategiesInsertStmt);
    }
  }

  private void createTableIfNotExists(
      SparkSession spark, String outputFqtn, boolean isPartitionScope) {
    if (isPartitionScope) {
      spark.sql(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s ("
                  + "fqtn STRING, "
                  + "partition_id STRING, "
                  + "partition_columns STRING, "
                  + "timestamp TIMESTAMP, "
                  + "estimated_compute_cost DOUBLE, "
                  + "estimated_file_count_reduction DOUBLE, "
                  + "file_size_entropy DOUBLE, "
                  + "pos_delete_file_count LONG, "
                  + "eq_delete_file_count LONG, "
                  + "pos_delete_file_bytes LONG, "
                  + "eq_delete_file_bytes LONG,"
                  + "pos_delete_record_count LONG, "
                  + "eq_delete_record_count LONG, "
                  + "file_count_reduction_penalty DOUBLE"
                  + ") "
                  + "PARTITIONED BY (days(timestamp))",
              outputFqtn));
    } else {
      spark.sql(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s ("
                  + "fqtn STRING, "
                  + "timestamp TIMESTAMP, "
                  + "estimated_compute_cost DOUBLE, "
                  + "estimated_file_count_reduction DOUBLE, "
                  + "file_size_entropy DOUBLE, "
                  + "pos_delete_file_count LONG, "
                  + "eq_delete_file_count LONG, "
                  + "pos_delete_file_bytes LONG, "
                  + "eq_delete_file_bytes LONG,"
                  + "pos_delete_record_count LONG, "
                  + "eq_delete_record_count LONG, "
                  + "isPartitioned BOOLEAN, "
                  + "file_count_reduction_penalty DOUBLE"
                  + ") "
                  + "PARTITIONED BY (days(timestamp))",
              outputFqtn));
    }
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static DataLayoutStrategyGeneratorSparkApp createApp(
      String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option(
            "o",
            "outputTableName",
            true,
            "Fully-qualified table name used to store strategies at table level"));
    extraOptions.add(
        new Option(
            "p",
            "partitionLevelOutputTableName",
            true,
            "Fully-qualified table name used to store strategies at partition level"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new DataLayoutStrategyGeneratorSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        cmdLine.getOptionValue("outputTableName"),
        cmdLine.getOptionValue("partitionLevelOutputTableName"),
        otelEmitter);
  }
}
