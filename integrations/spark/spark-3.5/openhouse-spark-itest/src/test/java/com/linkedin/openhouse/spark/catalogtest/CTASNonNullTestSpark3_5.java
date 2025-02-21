package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class CTASNonNullTestSpark3_5 extends OpenHouseSparkITest {
  @Test
  public void testCTASPreservesNonNull() throws Exception {
    SparkSession.Builder builder = getBuilder();
    TestSparkSessionUtil.configureCatalogs(
        builder, "default_iceberg_catalog", getOpenHouseLocalServerURI());
    builder.config(
        String.format("spark.sql.catalog.%s", "default_iceberg_catalog"),
        "org.apache.iceberg.spark.SparkCatalog");
    try (SparkSession spark = createSparkSession(builder)) {
      // Create source table with NOT NULL column
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_table (id INT NOT NULL, name STRING NOT NULL, value DOUBLE NOT NULL)");
      // Create target table using CTAS, OpenHouse catalog
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_tableCtas USING iceberg AS SELECT * FROM openhouse.ctasNonNull.test_table");
      // Create target table using CTAS, Iceberg catalog
      spark.sql(
          "CREATE TABLE default_iceberg_catalog.ctasNonNull.test_tableCtasDefaultIceberg USING iceberg AS SELECT * FROM openhouse.ctasNonNull.test_table");

      // Get schemas for both tables
      StructType sourceSchema = spark.table("openhouse.ctasNonNull.test_table").schema();
      StructType targetSchemaGood = spark.table("openhouse.ctasNonNull.test_tableCtas").schema();
      StructType targetSchemaBroken =
          spark.table("default_iceberg_catalog.ctasNonNull.test_tableCtasDefaultIceberg").schema();

      // Verify spark catalogs have correct classes configured
      assertEquals(
          "com.linkedin.openhouse.spark.SparkCatalog",
          spark.conf().get("spark.sql.catalog.openhouse"));
      assertEquals(
          "org.apache.iceberg.spark.SparkCatalog",
          spark.conf().get("spark.sql.catalog.default_iceberg_catalog"));

      // Verify id column is preserved in good catalog, not preserved in bad catalog
      assertFalse(sourceSchema.apply("id").nullable(), "Source table id column should be required");
      assertFalse(
          targetSchemaGood.apply("id").nullable(),
          "Target table id column required should not be preserved");
      assertTrue(
          targetSchemaBroken.apply("id").nullable(),
          "Target table id column required should not be preserved");

      // Clean up
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_table");
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_tableCtas");
      spark.sql("DROP TABLE default_iceberg_catalog.ctasNonNull.test_tableCtasDefaultIceberg");
    }
  }
}
