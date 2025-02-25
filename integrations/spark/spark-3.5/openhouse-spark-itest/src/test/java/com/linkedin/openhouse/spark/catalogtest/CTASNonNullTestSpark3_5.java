package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class CTASNonNullTestSpark3_5 extends OpenHouseSparkITest {
  @Test
  public void testCTASPreservesNonNull() throws Exception {
    SparkSession.Builder builder = getBuilder();
    try (SparkSession spark = createSparkSession(builder)) {
      // Create source table with NOT NULL column
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_table (id INT NOT NULL, name STRING NOT NULL, value DOUBLE NOT NULL)");
      // Create target table using CTAS, OpenHouse catalog
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_tableCtas USING iceberg AS SELECT * FROM openhouse.ctasNonNull.test_table");

      // Get schemas for both tables
      StructType sourceSchema = spark.table("openhouse.ctasNonNull.test_table").schema();
      StructType targetSchema = spark.table("openhouse.ctasNonNull.test_tableCtas").schema();

      // Verify spark catalogs have correct classes configured
      assertEquals(
          "org.apache.iceberg.spark.SparkCatalog", spark.conf().get("spark.sql.catalog.openhouse"));

      // Verify id column is preserved in good catalog, not preserved in bad catalog
      assertFalse(sourceSchema.apply("id").nullable(), "Source table id column should be required");
      assertTrue(
          targetSchema.apply("id").nullable(),
          "Target table id column required should not be preserved -- due to 1) the CTAS non-nullable preservation is off by default and 2) OS spark3.1 catalyst connector lack of support for non-null CTAS");

      // Clean up
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_table");
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_tableCtas");
    }
  }
}
