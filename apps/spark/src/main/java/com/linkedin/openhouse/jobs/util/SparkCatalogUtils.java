package com.linkedin.openhouse.jobs.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

public class SparkCatalogUtils {
  private SparkCatalogUtils() {
    // utils ctor noop
  }

  public static Catalog getIcebergCatalog(SparkSession spark, String catName) {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = String.format("spark.sql.catalog.%s.", catName);
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    // this initializes the catalog based on runtime Catalog class passed in catalog-impl conf.
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        catName,
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }
}
