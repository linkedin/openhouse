package com.linkedin.openhouse.spark;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.*;

import java.io.FileInputStream;
import java.nio.file.Files;
import lombok.SneakyThrows;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class SparkTestBase
    implements BeforeEachCallback, AfterEachCallback, ExtensionContext.Store.CloseableResource {

  private static boolean started = false;

  public static SparkSession spark = null;

  public static MockWebServer mockTableService = null;

  public static String baseSchema;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    baseSchema = ResourceIoHelper.getSchemaJsonFromResource("dummy_healthy_schema.json");

    if (!started) {
      started = true;
      mockTableService = new MockWebServer();
      mockTableService.start();
      spark =
          SparkSession.builder()
              .master("local[2]")
              .config(
                  "spark.sql.extensions",
                  ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                      + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
              .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
              .config(
                  "spark.sql.catalog.openhouse.catalog-impl",
                  "com.linkedin.openhouse.spark.OpenHouseCatalog")
              .config(
                  "spark.sql.catalog.openhouse.uri",
                  String.format(
                      "http://%s:%s", mockTableService.getHostName(), mockTableService.getPort()))
              .config(
                  "spark.sql.catalog.openhouse.metrics-reporter-impl",
                  "com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter")
              // Helper catalog to create test data easily
              .config("spark.sql.catalog.testhelper", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.testhelper.type", "hadoop")
              .config(
                  "spark.sql.catalog.testhelper.warehouse",
                  Files.createTempDirectory("sparkOpenhouse").toString())
              .config(
                  "spark.sql.catalog.openhouse.auth-token",
                  IOUtils.toString(
                      new FileInputStream(
                          getClass().getClassLoader().getResource("dummy.token").getFile())))
              .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
              .getOrCreate();
      // The following line registers a callback hook when the root test context is shut down
      context.getRoot().getStore(GLOBAL).put("SparkTestBase", this);
    }
  }

  @SneakyThrows
  @Override
  public void close() {
    started = false;
    mockTableService.close();
    spark.close();
  }

  @Override
  public void afterEach(ExtensionContext context) {
    // equivalent to mockTableService.clear()
    mockTableService.setDispatcher(new QueueDispatcher());
  }
}
