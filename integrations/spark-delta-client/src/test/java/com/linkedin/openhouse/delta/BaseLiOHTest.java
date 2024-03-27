package com.linkedin.openhouse.delta;

import com.linkedin.openhouse.tablestest.OpenHouseLocalServer;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.net.URI;
import org.apache.spark.sql.SparkSession;

public class BaseLiOHTest extends OpenHouseSparkITest {

  private static OpenHouseLocalServer openHouseLocalServer = null;

  @Override
  protected synchronized SparkSession getSparkSession() throws Exception {
    if (openHouseLocalServer == null) {
      openHouseLocalServer = new OpenHouseLocalServer();
      openHouseLocalServer.start();
    }
    return SparkSession.builder()
        .master("local[1]")
        .config(
            "spark.sql.catalog.openhouse.uri",
            URI.create("http://localhost:" + openHouseLocalServer.getPort()).toString())
        .config("spark.sql.catalog.openhouse.cluster", "local-cluster")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.openhouse", "com.linkedin.openhouse.delta.OHCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.OHLogStore")
        .getOrCreate();
  }
}
