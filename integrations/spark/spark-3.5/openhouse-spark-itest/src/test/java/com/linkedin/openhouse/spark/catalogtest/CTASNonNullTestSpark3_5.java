package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class CTASNonNullTestSpark3_5 extends OpenHouseSparkITest {
  @Test
  public void testCTASPreservesNonNull() throws Exception {
    String overrideCatalogName = "opensource_iceberg_spark_catalog";
    try (SparkSession spark =
        getSparkSession(
            overrideCatalogName,
            Map.of(
                String.format("spark.sql.catalog.%s", "opensource_iceberg_spark_catalog"),
                "org.apache.iceberg.spark.SparkCatalog"))) {
      // Create source table with NOT NULL column
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_table (id INT NOT NULL, name STRING NOT NULL, value DOUBLE NOT NULL)");
      // Create target table using CTAS, OpenHouse catalog
      spark.sql(
          "CREATE TABLE openhouse.ctasNonNull.test_tableCtas USING iceberg AS SELECT * FROM openhouse.ctasNonNull.test_table");
      // Create target table using CTAS, Iceberg catalog
      spark.sql(
          "CREATE TABLE opensource_iceberg_spark_catalog.ctasNonNull.test_tableCtasDefaultIceberg USING iceberg AS SELECT * FROM openhouse.ctasNonNull.test_table");

      // Get schemas for both tables
      StructType sourceSchema = spark.table("openhouse.ctasNonNull.test_table").schema();
      StructType targetSchemaGood = spark.table("openhouse.ctasNonNull.test_tableCtas").schema();
      StructType targetSchemaBroken =
          spark
              .table("opensource_iceberg_spark_catalog.ctasNonNull.test_tableCtasDefaultIceberg")
              .schema();

      // Verify spark catalogs have correct classes configured
      assertEquals(
          "com.linkedin.openhouse.spark.SparkCatalog",
          spark.conf().get("spark.sql.catalog.openhouse"));
      assertEquals(
          "org.apache.iceberg.spark.SparkCatalog",
          spark.conf().get("spark.sql.catalog.opensource_iceberg_spark_catalog"));

      // Verify id column is preserved in good catalog, not preserved in bad catalog
      assertFalse(sourceSchema.apply("id").nullable(), "Source table id column should be required");
      assertTrue(
          targetSchemaBroken.apply("id").nullable(),
          "Target table id column required should not be preserved -- due to 1) the CTAS non-nullable preservation is off by default");
      assertFalse(
          targetSchemaGood.apply("id").nullable(),
          "Target table id column required should be preserved.");

      // Clean up
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_table");
      spark.sql("DROP TABLE openhouse.ctasNonNull.test_tableCtas");
      spark.sql(
          "DROP TABLE opensource_iceberg_spark_catalog.ctasNonNull.test_tableCtasDefaultIceberg");
    }
  }
}
