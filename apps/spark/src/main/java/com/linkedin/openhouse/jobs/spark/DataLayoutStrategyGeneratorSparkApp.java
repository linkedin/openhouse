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
  private @Nullable String outputPartitionFqtn;

  protected DataLayoutStrategyGeneratorSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      @Nullable String outputFqtn,
      @Nullable String outputPartitionFqtn) {
    super(jobId, stateManager, fqtn);
    this.outputFqtn = outputFqtn;
    this.outputPartitionFqtn = outputPartitionFqtn;
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
    log.info("Generating strategies for table {}", fqtn);
    List<DataLayoutStrategy> strategies = strategiesGenerator.generate();
    List<DataLayoutStrategy> tableStrategies =
        strategies.isEmpty() ? new ArrayList<>() : strategies.subList(0, 1);
    List<DataLayoutStrategy> partitionStrategies =
        strategies.size() > 1 ? new ArrayList<>() : strategies.subList(1, strategies.size());
    log.info(
        "Generated {} table strategies {},\nGenerated {} partition strategies {}",
        tableStrategies.size(),
        tableStrategies.stream().map(Object::toString).collect(Collectors.joining(", ")),
        partitionStrategies.size(),
        partitionStrategies.stream().map(Object::toString).collect(Collectors.joining(", ")));
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.save(fqtn, StrategiesDaoTableProps.DATA_LAYOUT_STRATEGIES_PROPERTY_KEY, tableStrategies);
    dao.save(
        fqtn,
        StrategiesDaoTableProps.DATA_LAYOUT_PARTITION_STRATEGIES_PROPERTY_KEY,
        partitionStrategies);
    if (outputFqtn != null && !tableStrategies.isEmpty()) {
      createTableIfNotExists(spark, outputFqtn);
      DataLayoutStrategy strategy = tableStrategies.get(0);
      String row =
          String.format(
              "('%s', current_timestamp(), %f, %f, %f)",
              fqtn, strategy.getCost(), strategy.getGain(), strategy.getEntropy());
      String strategiesInsertStmt = String.format("INSERT INTO %s VALUES %s", outputFqtn, row);
      log.info("Running {}", strategiesInsertStmt);
      spark.sql(strategiesInsertStmt);
    }
    if (outputPartitionFqtn != null && !partitionStrategies.isEmpty()) {
      createTableIfNotExists(spark, outputPartitionFqtn);
      List<String> rows = new ArrayList<>();
      for (DataLayoutStrategy strategy : strategies) {
        rows.add(
            String.format(
                "('%s', '%s', current_timestamp(), %f, %f, %f)",
                fqtn,
                strategy.getPartitionId(),
                strategy.getCost(),
                strategy.getGain(),
                strategy.getEntropy()));
      }
      String strategiesInsertStmt =
          String.format("INSERT INTO %s VALUES %s", outputPartitionFqtn, String.join(", ", rows));
      log.info("Running {}", strategiesInsertStmt);
      spark.sql(strategiesInsertStmt);
    }
  }

  private void createTableIfNotExists(SparkSession spark, String outputFqtn) {
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s ("
                + "fqtn STRING, "
                + "timestamp TIMESTAMP, "
                + "estimated_compute_cost DOUBLE, "
                + "estimated_file_count_reduction DOUBLE, "
                + "file_size_entropy DOUBLE "
                + ") "
                + "PARTITIONED BY (days(timestamp))",
            outputFqtn));
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option(
            "o", "outputTableName", true, "Fully-qualified table name used to store strategies"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    DataLayoutStrategyGeneratorSparkApp app =
        new DataLayoutStrategyGeneratorSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("outputTableName"),
            cmdLine.getOptionValue("outputPartitionTableName"));
    app.run();
  }
}
