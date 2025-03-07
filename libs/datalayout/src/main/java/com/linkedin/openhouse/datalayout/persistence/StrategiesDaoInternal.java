package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DAO implementation for persisting and loading data layout optimization strategies in table
 * properties.
 */
@Slf4j
@Builder
public class StrategiesDaoInternal implements StrategiesDao {
  private final SparkSession spark;
  private final boolean isPartitionScope;
  private final String outputFqtn;

  @Override
  public void save(String targetFqtn, List<DataLayoutStrategy> strategies) {
    if (outputFqtn != null && !strategies.isEmpty()) {
      createTableIfNotExists(spark, outputFqtn, isPartitionScope);
      List<String> rows = new ArrayList<>();
      for (DataLayoutStrategy strategy : strategies) {
        if (isPartitionScope) {
          rows.add(
              String.format(
                  "('%s', '%s', '%s', current_timestamp(), %f, %f, %f, %d, %d, %d, %d, %d, %d)",
                  targetFqtn,
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
                  targetFqtn,
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

  @Override
  public List<DataLayoutStrategy> load(String fqtn) {
    Dataset<Row> df = spark.table(fqtn);
    return df.collectAsList().stream()
        .map(
            row -> {
              DataLayoutStrategy.DataLayoutStrategyBuilder builder =
                  DataLayoutStrategy.builder()
                      .config(DataCompactionConfig.builder().build())
                      .entropy(row.getAs("file_size_entropy"))
                      .cost(row.getAs("estimated_compute_cost"))
                      .gain(row.getAs("estimated_file_count_reduction"))
                      .posDeleteFileCount(row.getAs("pos_delete_file_count"))
                      .eqDeleteFileCount(row.getAs("eq_delete_file_count"))
                      .posDeleteFileBytes(row.getAs("pos_delete_file_bytes"))
                      .eqDeleteFileBytes(row.getAs("eq_delete_file_bytes"))
                      .posDeleteRecordCount(row.getAs("pos_delete_record_count"))
                      .eqDeleteRecordCount(row.getAs("eq_delete_record_count"));
              if (isPartitionScope) {
                builder
                    .partitionId(row.getAs("partition_id"))
                    .partitionColumns(row.getAs("partition_columns"));
              }
              return builder.build();
            })
        .collect(Collectors.toList());
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
}
