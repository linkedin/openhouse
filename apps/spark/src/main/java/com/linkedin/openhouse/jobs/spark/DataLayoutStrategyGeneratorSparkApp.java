package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseDataLayoutStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDao;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
      @Nullable String partitionLevelOutputFqtn) {
    super(jobId, stateManager, fqtn);
    this.outputFqtn = outputFqtn;
    this.partitionLevelOutputFqtn = partitionLevelOutputFqtn;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    SparkSession spark = ops.spark();
    TableFileStats tableFileStats = TableFileStats.builder().tableName(fqtn).spark(spark).build();
    TablePartitionStats tablePartitionStats =
        TablePartitionStats.builder().tableName(fqtn).spark(spark).build();
    OpenHouseDataLayoutStrategyGenerator strategiesGenerator =
        OpenHouseDataLayoutStrategyGenerator.builder()
            .tableFileStats(tableFileStats)
            .tablePartitionStats(tablePartitionStats)
            .build();
    runInnerTableScope(spark, strategiesGenerator);
    runInnerPartitionScope(spark, strategiesGenerator);
  }

  private void runInnerTableScope(
      SparkSession spark, OpenHouseDataLayoutStrategyGenerator strategiesGenerator) {
    log.info("Generating strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generateTableLevelStrategies();
    log.info(
        "Generated {} strategies {}",
        strategies.size(),
        strategies.stream().map(Object::toString).collect(Collectors.joining(", ")));
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.save(fqtn, strategies);
    appendToDloStrategiesTable(spark, outputFqtn, strategies, false);
  }

  private void runInnerPartitionScope(
      SparkSession spark, OpenHouseDataLayoutStrategyGenerator strategiesGenerator) {
    log.info("Generating partition-level strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generatePartitionLevelStrategies();
    log.info("Generated {} strategies", strategies.size());
    appendToDloStrategiesTable(spark, partitionLevelOutputFqtn, strategies, true);
  }

  private void appendToDloStrategiesTable(
      SparkSession spark,
      String outputFqtn,
      List<DataLayoutStrategy> strategies,
      boolean isPartitionScope) {
    if (outputFqtn != null && !strategies.isEmpty()) {
      createTableIfNotExists(spark, outputFqtn, isPartitionScope);
      List<String> rows = new ArrayList<>();
      for (DataLayoutStrategy strategy : strategies) {
        if (isPartitionScope) {
          rows.add(
              String.format(
                  "('%s', '%s', '%s', current_timestamp(), %f, %f, %f, %d, %d, %d, %d, %d, %d)",
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
                  strategy.getEqDeleteRecordCount()));
        } else {
          rows.add(
              String.format(
                  "('%s', current_timestamp(), %f, %f, %f, %d, %d, %d, %d, %d, %d)",
                  fqtn,
                  strategy.getCost(),
                  strategy.getGain(),
                  strategy.getEntropy(),
                  strategy.getPosDeleteFileCount(),
                  strategy.getEqDeleteFileCount(),
                  strategy.getPosDeleteFileBytes(),
                  strategy.getEqDeleteFileBytes(),
                  strategy.getPosDeleteRecordCount(),
                  strategy.getEqDeleteRecordCount()));
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
                  + "eq_delete_record_count LONG"
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
                  + "eq_delete_record_count LONG"
                  + ") "
                  + "PARTITIONED BY (days(timestamp))",
              outputFqtn));
    }
  }

  public static void main(String[] args) {
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
    DataLayoutStrategyGeneratorSparkApp app =
        new DataLayoutStrategyGeneratorSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("outputTableName"),
            cmdLine.getOptionValue("partitionLevelOutputTableName"));
    app.run();
  }
}
