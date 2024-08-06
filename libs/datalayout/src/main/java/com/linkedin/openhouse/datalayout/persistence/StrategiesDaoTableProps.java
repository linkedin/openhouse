package com.linkedin.openhouse.datalayout.persistence;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.datalayout.strategy.RewriteStrategy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringEscapeUtils;
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
  public void save(String fqtn, List<RewriteStrategy> strategies) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<RewriteStrategy>>() {}.getType();
    String propValue = StringEscapeUtils.escapeJava(gson.toJson(strategies, type));
    log.info("Saving strategies {} for table {}", propValue, fqtn);
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
            fqtn, DATA_LAYOUT_STRATEGIES_PROPERTY_KEY, propValue));
  }

  @Override
  public List<RewriteStrategy> load(String fqtn) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<RewriteStrategy>>() {}.getType();
    String propValue =
        spark
            .sql(
                String.format(
                    "SHOW TBLPROPERTIES %s ('%s')", fqtn, DATA_LAYOUT_STRATEGIES_PROPERTY_KEY))
            .collectAsList()
            .get(0)
            .getString(1);
    return gson.fromJson(StringEscapeUtils.unescapeJava(propValue), type);
  }
}
