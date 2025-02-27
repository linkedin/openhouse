package com.linkedin.openhouse.tablestest;

import java.net.URI;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * A base class for spark-based integration tests running in local container. Instruction: - Extend
 * {@link OpenHouseSparkITest} for the test class. - Use the SparkSession returned by invoking
 * `getSparkSession`.
 *
 * <p>See com.linkedin.openhouse.jobs.spark.TestSparkApp as an example.
 */
public class OpenHouseSparkITest {
  private static final String LOCALHOST = "http://localhost:";
  private static final String LOCAL_FS = "file:///";

  /** Use a singleton {@link OpenHouseLocalServer} that will shutdown when the JVM exits. */
  private static OpenHouseLocalServer openHouseLocalServer = null;

  /**
   * Get a spark session with correctly configured OpenHouse catalog. A single session will be
   * cached and reused for different unit tests.
   *
   * <p>Do not {@link SparkSession#close()} a created SparkSession.
   */
  protected synchronized SparkSession getSparkSession() throws Exception {
    return TestSparkSessionUtil.createSparkSession(getBuilder());
  }

  protected synchronized SparkSession getSparkSession(
      String overrideCatalogName, Map<String, String> overrides) throws Exception {
    SparkSession.Builder builder = getBuilder();
    TestSparkSessionUtil.configureCatalogs(
        builder, overrideCatalogName, getOpenHouseLocalServerURI());
    for (Map.Entry<String, String> entry : overrides.entrySet()) {
      builder.config(entry.getKey(), entry.getValue());
    }
    return TestSparkSessionUtil.createSparkSession(builder);
  }

  protected SparkSession.Builder getBuilder() throws Exception {
    if (openHouseLocalServer == null) {
      openHouseLocalServer = new OpenHouseLocalServer();
      openHouseLocalServer.start();
    }
    SparkSession.Builder builder = TestSparkSessionUtil.getBaseBuilder(URI.create(LOCAL_FS));
    TestSparkSessionUtil.configureCatalogs(builder, "openhouse", getOpenHouseLocalServerURI());
    // default_iceberg catalog need to point to the custom one, required by Iceberg maintenance
    // operations
    // otherwise, Hive default catalog will be set as per
    // https://github.com/apache/iceberg/blob/957ea6d1462fa5b494f443f88bff167e4ec11e11/
    // spark/v3.0/spark/src/main/java/org/apache/iceberg/spark/source/IcebergSource.java#L185
    TestSparkSessionUtil.configureCatalogs(
        builder, "default_iceberg", getOpenHouseLocalServerURI());
    return builder;
  }

  /** @return the {@link URI} of the standalone embedded OH server running locally. */
  protected URI getOpenHouseLocalServerURI() {
    if (openHouseLocalServer == null) {
      throw new RuntimeException(
          "OH server is not running locally. Start it by calling getSparkSession() method.");
    }
    return URI.create(LOCALHOST + openHouseLocalServer.getPort());
  }
}
