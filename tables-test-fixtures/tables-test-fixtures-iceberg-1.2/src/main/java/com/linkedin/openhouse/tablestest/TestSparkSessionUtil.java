package com.linkedin.openhouse.tablestest;

import java.io.IOException;
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

  public static SparkSession.Builder getBaseBuilder(URI fsURI) {
    return SparkSession.builder()
        .master("local[1]")
        .config(
            "spark.sql.extensions",
            ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
        .config("spark.hadoop.fs.defaultFS", fsURI.toString())
        // Set session timezone to UTC to ensure consistent timestamp handling across different
        // system timezones. This prevents test flakiness when timestamp operations
        // (like using from_unixtime) are interpreted differently based on the
        // system's local timezone.With UTC, all tests behave consistently
        // regardless of where they run.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "127.0.0.1");
  }

  public static void configureCatalogs(
      SparkSession.Builder builder, String catalogName, URI tablesServiceURI) {

    // Set default configurations
    builder
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config(
            String.format("spark.sql.catalog.%s.catalog-impl", catalogName),
            "com.linkedin.openhouse.spark.OpenHouseCatalog")
        .config(String.format("spark.sql.catalog.%s.uri", catalogName), tablesServiceURI.toString())
        .config(String.format("spark.sql.catalog.%s.cluster", catalogName), "local-cluster");

    try {
      builder.config(
          String.format("spark.sql.catalog.%s.auth-token", catalogName),
          IOUtils.toString(
              Objects.requireNonNull(
                  TestSparkSessionUtil.class.getClassLoader().getResourceAsStream("dummy.token")),
              StandardCharsets.UTF_8));
    } catch (IOException e) {
      // Handle the exception by setting a default token or logging
      builder.config(
          String.format("spark.sql.catalog.%s.auth-token", catalogName), "default-token");
    }
  }

  public static SparkSession createSparkSession(SparkSession.Builder builder) {
    return builder.getOrCreate();
  }
}
