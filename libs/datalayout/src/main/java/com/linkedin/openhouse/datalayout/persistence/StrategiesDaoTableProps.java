package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.List;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * DAO implementation for persisting and loading data layout optimization strategies in table
 * properties.
 */
@Slf4j
@Builder
public class StrategiesDaoTableProps implements StrategiesDao {
  public static final String DATA_LAYOUT_STRATEGIES_PROPERTY_KEY = "write.data-layout.strategies";
  private final SparkSession spark;

  @Override
  public void save(String fqtn, List<DataLayoutStrategy> strategies) {
    String propValue = SerDeUtil.toJsonString(strategies);
    log.info("Saving strategies {} for table {}", propValue, fqtn);
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
            fqtn, DATA_LAYOUT_STRATEGIES_PROPERTY_KEY, propValue));
  }

  @Override
  public List<DataLayoutStrategy> load(String fqtn) {
    String propValue =
        spark
            .sql(
                String.format(
                    "SHOW TBLPROPERTIES %s ('%s')", fqtn, DATA_LAYOUT_STRATEGIES_PROPERTY_KEY))
            .collectAsList()
            .get(0)
            .getString(1);
    return SerDeUtil.fromJsonStringList(propValue);
  }
}
