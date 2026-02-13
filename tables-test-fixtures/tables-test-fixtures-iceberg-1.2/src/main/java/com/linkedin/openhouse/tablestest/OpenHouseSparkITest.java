package com.linkedin.openhouse.tablestest;

import java.net.URI;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * A base class for spark-based integration tests running in local container. Instruction: - Extend
 * {@link OpenHouseSparkITest} for the test class. - Use the SparkSession returned by invoking
 * `getSparkSession`.
 *
 * <p>This class is thread-safe and supports parallel test execution. The singleton {@link
 * OpenHouseLocalServer} is lazily initialized with proper synchronization.
 *
 * <p>See com.linkedin.openhouse.jobs.spark.TestSparkApp as an example.
 */
public class OpenHouseSparkITest {
  private static final String LOCALHOST = "http://localhost:";
  private static final String LOCAL_FS = "file:///";

  /**
   * Singleton {@link OpenHouseLocalServer} that will shutdown when the JVM exits. This field is
   * declared as volatile to ensure proper publication of the initialized instance across threads
   * when using double-checked locking pattern.
   */
  private static volatile OpenHouseLocalServer openHouseLocalServer = null;

  /**
   * Get a spark session with correctly configured OpenHouse catalog. A single session will be
   * cached and reused for different unit tests.
   *
   * <p>This method is thread-safe and can be called concurrently from multiple test instances.
   *
   * <p>Do not {@link SparkSession#close()} a created SparkSession.
   *
   * @return configured SparkSession
   * @throws Exception if session creation fails
   */
  protected SparkSession getSparkSession() throws Exception {
    return TestSparkSessionUtil.createSparkSession(getBuilder());
  }

  /**
   * Get a spark session with a custom catalog configuration.
   *
   * <p>This method is thread-safe and can be called concurrently from multiple test instances.
   *
   * @param overrideCatalogName the name of the catalog to override
   * @param overrides additional spark configuration overrides
   * @return configured SparkSession
   * @throws Exception if session creation fails
   */
  protected SparkSession getSparkSession(String overrideCatalogName, Map<String, String> overrides)
      throws Exception {
    SparkSession.Builder builder = getBuilder();
    TestSparkSessionUtil.configureCatalogs(
        builder, overrideCatalogName, getOpenHouseLocalServerURI());
    for (Map.Entry<String, String> entry : overrides.entrySet()) {
      builder.config(entry.getKey(), entry.getValue());
    }
    return TestSparkSessionUtil.createSparkSession(builder);
  }

  /**
   * Initializes the singleton OpenHouseLocalServer if not already started. Uses double-checked
   * locking pattern for optimal performance - synchronization only occurs during initialization.
   *
   * @throws Exception if server initialization fails
   */
  private static void startOpenHouseLocalServer() throws Exception {
    if (openHouseLocalServer == null) { // First check (no locking) - fast path
      synchronized (OpenHouseSparkITest.class) {
        if (openHouseLocalServer == null) { // Second check (with lock) - ensures singleton
          openHouseLocalServer = new OpenHouseLocalServer();
          openHouseLocalServer.start();
        }
      }
    }
  }

  /**
   * Build a SparkSession.Builder with OpenHouse catalog configured. This method is thread-safe and
   * can be overridden by subclasses that need custom builder configuration.
   *
   * <p>Subclasses overriding this method should call super.getBuilder() to leverage the thread-safe
   * server initialization.
   *
   * @return configured SparkSession.Builder
   * @throws Exception if builder creation fails
   */
  protected SparkSession.Builder getBuilder() throws Exception {
    startOpenHouseLocalServer();
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
