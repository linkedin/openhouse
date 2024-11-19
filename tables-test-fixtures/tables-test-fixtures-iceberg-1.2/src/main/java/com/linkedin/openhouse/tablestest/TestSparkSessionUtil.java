package com.linkedin.openhouse.tablestest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;

/**
 * This class is supposed to be used to test and debug integration of Spark with OpenHouse.
 * Specifically, it allows running embedded Spark session against tables endpoint and HDFS in local
 * cluster mode.
 */
public final class TestSparkSessionUtil {
  private TestSparkSessionUtil() {
    // Util class constructor noop
  }

  public static SparkSession create(String customCatalogName, URI tablesServiceURI, URI fsURI)
      throws Exception {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.hadoop.fs.defaultFS", fsURI.toString())
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.driver.memory", "2g")
            .config("spark.driver.bindAddress", "127.0.0.1");
    // default_iceberg catalog need to point to the custom one, required by Iceberg maintenance
    // operations
    // otherwise, Hive default catalog will be set as per
    // https://github.com/apache/iceberg/blob/957ea6d1462fa5b494f443f88bff167e4ec11e11/
    // spark/v3.0/spark/src/main/java/org/apache/iceberg/spark/source/IcebergSource.java#L185

    for (String catalog : new String[] {customCatalogName, "default_iceberg"}) {
      builder =
          builder
              .config(
                  String.format("spark.sql.catalog.%s", catalog),
                  "org.apache.iceberg.spark.SparkCatalog")
              .config(
                  String.format("spark.sql.catalog.%s.catalog-impl", catalog),
                  "com.linkedin.openhouse.spark.OpenHouseCatalog")
              .config(
                  String.format("spark.sql.catalog.%s.uri", catalog), tablesServiceURI.toString())
              .config(String.format("spark.sql.catalog.%s.cluster", catalog), "local-cluster")
              .config(
                  String.format("spark.sql.catalog.%s.auth-token", catalog),
                  IOUtils.toString(
                      Objects.requireNonNull(
                          TestSparkSessionUtil.class
                              .getClassLoader()
                              .getResourceAsStream("dummy.token")),
                      StandardCharsets.UTF_8.name()));
    }
    return builder.getOrCreate();
  }
}
