package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseRewriteStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDao;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.RewriteStrategy;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class RewriteStrategyGeneratorSparkApp extends BaseTableSparkApp {
  protected RewriteStrategyGeneratorSparkApp(String jobId, StateManager stateManager, String fqtn) {
    super(jobId, stateManager, fqtn);
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    SparkSession spark = ops.spark();
    TableFileStats tableFileStats = TableFileStats.builder().tableName(fqtn).spark(spark).build();
    TablePartitionStats tablePartitionStats =
        TablePartitionStats.builder().tableName(fqtn).spark(spark).build();
    OpenHouseRewriteStrategyGenerator strategiesGenerator =
        OpenHouseRewriteStrategyGenerator.builder()
            .tableFileStats(tableFileStats)
            .tablePartitionStats(tablePartitionStats)
            .build();
    log.info("Generating strategies for table {}", fqtn);
    List<RewriteStrategy> strategies = strategiesGenerator.generate();
    log.info(
        "Generated {} strategies {}",
        strategies.size(),
        strategies.stream().map(Object::toString).collect(Collectors.joining(", ")));
    StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
    dao.save(fqtn, strategies);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    RewriteStrategyGeneratorSparkApp app =
        new RewriteStrategyGeneratorSparkApp(
            getJobId(cmdLine), createStateManager(cmdLine), cmdLine.getOptionValue("tableName"));
    app.run();
  }
}
