package com.linkedin.openhouse.catalog.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class CatalogOperationTest extends OpenHouseSparkITest {
  @Test
  public void testCasingWithCTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      spark.sql("CREATE TABLE openhouse.d1.`tT1` (name string)");
      // testing writing behavior
      spark.sql("INSERT INTO openhouse.d1.Tt1 VALUES ('foo')");

      // Verifying by querying with all lower-cased name
      assertEquals(1, spark.sql("SELECT * from openhouse.d1.tt1").collectAsList().size());
      // ctas but referring with lower-cased name
      spark.sql("CREATE TABLE openhouse.d1.t2 AS SELECT * from openhouse.d1.tt1");
      assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t2").collectAsList().size());
    }
  }
}
