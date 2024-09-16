package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseDataLayoutStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.SerDeUtil;
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

  protected DataLayoutStrategyGeneratorSparkApp(
      String jobId, StateManager stateManager, String fqtn, @Nullable String outputFqtn) {
    super(jobId, stateManager, fqtn);
    this.outputFqtn = outputFqtn;
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
    log.info(
        "Generated {} strategies {}",
        strategies.size(),
        strategies.stream().map(Object::toString).collect(Collectors.joining(", ")));
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.save(fqtn, strategies);
    if (outputFqtn != null && !strategies.isEmpty()) {
      createTableIfNotExists(spark, outputFqtn);
      List<String> rows = new ArrayList<>();
      for (DataLayoutStrategy strategy : strategies) {
        rows.add(
            String.format(
                "('%s', current_timestamp(), %f, %f, %f, %f, '%s')",
                fqtn,
                strategy.getCost(),
                strategy.getGain(),
                strategy.getEntropy(),
                strategy.getScore(),
                SerDeUtil.toJsonString(strategy)));
      }
      String strategiesInsertStmt =
          String.format("INSERT INTO %s VALUES %s", outputFqtn, String.join(", ", rows));
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
                + "cost DOUBLE, "
                + "gain DOUBLE, "
                + "entropy DOUBLE, "
                + "score DOUBLE, "
                + "strategy STRING"
                + ")",
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
            cmdLine.getOptionValue("outputTableName"));
    app.run();
  }
}
