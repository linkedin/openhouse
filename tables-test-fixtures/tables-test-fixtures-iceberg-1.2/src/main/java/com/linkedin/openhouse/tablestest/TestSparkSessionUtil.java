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
    SparkSession.Builder builder = getBaseBuilder(fsURI);
    configureCatalogs(builder, customCatalogName, tablesServiceURI);
    return createSparkSession(builder);
  }

  public static SparkSession.Builder getBaseBuilder(URI fsURI) {
    return SparkSession.builder()
        .master("local[1]")
        .config(
            "spark.sql.extensions",
            ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
        .config("spark.hadoop.fs.defaultFS", fsURI.toString())
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "127.0.0.1");
  }

  public static void configureCatalogs(
      SparkSession.Builder builder, String customCatalogName, URI tablesServiceURI)
      throws Exception {
    builder
        .config(
            String.format("spark.sql.catalog.%s", customCatalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config(
            String.format("spark.sql.catalog.%s.catalog-impl", customCatalogName),
            "com.linkedin.openhouse.spark.OpenHouseCatalog")
        .config(
            String.format("spark.sql.catalog.%s.uri", customCatalogName),
            tablesServiceURI.toString())
        .config(String.format("spark.sql.catalog.%s.cluster", customCatalogName), "local-cluster")
        .config(
            String.format("spark.sql.catalog.%s.auth-token", customCatalogName),
            IOUtils.toString(
                Objects.requireNonNull(
                    TestSparkSessionUtil.class.getClassLoader().getResourceAsStream("dummy.token")),
                StandardCharsets.UTF_8));
  }

  public static SparkSession createSparkSession(SparkSession.Builder builder) {
    return builder.getOrCreate();
  }
}
