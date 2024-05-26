package com.linkedin.openhouse.datalayout.persistence;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.datalayout.layoutselection.DataLayoutOptimizationStrategy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.sql.SparkSession;

/** Utility class for strategies persistence. */
public final class Utils {
  public static final String DATA_LAYOUT_PROPERTY_KEY = "data-layout-strategies";

  private Utils() {}

  public static void saveStrategies(
      SparkSession spark, String tableName, List<DataLayoutOptimizationStrategy> strategies) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<DataLayoutOptimizationStrategy>>() {}.getType();
    String propValue = StringEscapeUtils.escapeJava(gson.toJson(strategies, type));
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
            tableName, DATA_LAYOUT_PROPERTY_KEY, propValue));
  }

  public static List<DataLayoutOptimizationStrategy> loadStrategies(
      SparkSession spark, String tableName) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<DataLayoutOptimizationStrategy>>() {}.getType();
    String propValue =
        spark
            .sql(String.format("SHOW TBLPROPERTIES %s ('%s')", tableName, DATA_LAYOUT_PROPERTY_KEY))
            .collectAsList()
            .get(0)
            .getString(1);
    return gson.fromJson(StringEscapeUtils.unescapeJava(propValue), type);
  }
}
