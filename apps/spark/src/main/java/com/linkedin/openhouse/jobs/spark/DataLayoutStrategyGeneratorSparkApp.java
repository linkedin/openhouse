package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseDataLayoutStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDao;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoInternal;
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
    StrategiesDao internalDao =
        StrategiesDaoInternal.builder()
            .spark(spark)
            .outputFqtn(outputFqtn)
            .isPartitionScope(false)
            .build();
    internalDao.save(fqtn, strategies);
  }

  private void runInnerPartitionScope(
      SparkSession spark, OpenHouseDataLayoutStrategyGenerator strategiesGenerator) {
    log.info("Generating partition-level strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generatePartitionLevelStrategies();
    log.info("Generated {} strategies", strategies.size());
    StrategiesDao internalDao =
        StrategiesDaoInternal.builder()
            .spark(spark)
            .outputFqtn(partitionLevelOutputFqtn)
            .isPartitionScope(true)
            .build();
    internalDao.save(fqtn, strategies);
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
