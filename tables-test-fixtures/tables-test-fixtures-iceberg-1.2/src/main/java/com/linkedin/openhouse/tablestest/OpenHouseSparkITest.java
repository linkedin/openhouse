package com.linkedin.openhouse.tablestest;

import java.net.URI;
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
  protected static synchronized SparkSession getSparkSession() throws Exception {
    if (openHouseLocalServer == null) {
      openHouseLocalServer = new OpenHouseLocalServer();
      openHouseLocalServer.start();
    }
    return TestSparkSessionUtil.create(
        "openhouse", getOpenHouseLocalServerURI(), URI.create(LOCAL_FS));
  }

  /** @return the {@link URI} of the standalone embedded OH server running locally. */
  protected static URI getOpenHouseLocalServerURI() {
    if (openHouseLocalServer == null) {
      throw new RuntimeException(
          "OH server is not running locally. Start it by calling getSparkSession() method.");
    }
    return URI.create(LOCALHOST + openHouseLocalServer.getPort());
  }
}
