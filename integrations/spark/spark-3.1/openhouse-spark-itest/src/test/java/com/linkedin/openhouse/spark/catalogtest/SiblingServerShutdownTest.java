package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.openhouse.tablestest.OpenHouseLocalServer;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Stopping a separate embedded OpenHouse server must not break the Tables context backing this
 * test's Spark session.
 */
public class SiblingServerShutdownTest extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_sibling_shutdown";
  private static final String FULL_TABLE_NAME = "openhouse." + DATABASE + ".t1";

  @Test
  public void testSparkSessionSurvivesSiblingServerShutdown() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      try {
        spark.sql("CREATE TABLE " + FULL_TABLE_NAME + " (name string)");
        spark.sql("INSERT INTO " + FULL_TABLE_NAME + " VALUES ('before.sibling')");

        OpenHouseLocalServer sibling = new OpenHouseLocalServer();
        sibling.start();
        sibling.stop();

        spark.sql("INSERT INTO " + FULL_TABLE_NAME + " VALUES ('after.sibling')");

        List<String> rows =
            spark.sql("SELECT name FROM " + FULL_TABLE_NAME + " ORDER BY name").collectAsList()
                .stream()
                .map(Row::mkString)
                .collect(Collectors.toList());
        assertEquals(java.util.Arrays.asList("after.sibling", "before.sibling"), rows);
      } finally {
        spark.sql("DROP TABLE IF EXISTS " + FULL_TABLE_NAME);
      }
    }
  }
}
