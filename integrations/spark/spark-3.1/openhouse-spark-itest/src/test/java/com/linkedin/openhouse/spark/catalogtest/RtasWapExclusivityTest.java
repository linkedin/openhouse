package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Black-box tests (real embedded OpenHouse server + real Spark SQL) that RTAS ({@code
 * replace.enabled}) and WAP ({@code write.wap.enabled}) cannot be enabled on the same table at the
 * same time. They are mutually exclusive: a staged WAP write and a whole-table replace do not
 * compose.
 */
public class RtasWapExclusivityTest extends OpenHouseSparkITest {

  @Test
  public void testCannotEnableRtasWhenWapEnabled() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasWap.wapFirst";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string) USING iceberg "
              + "TBLPROPERTIES ('write.wap.enabled'='true')");

      BadRequestException exception =
          assertThrows(
              BadRequestException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')"));
      assertTrue(
          exception.getMessage().contains("mutually exclusive"),
          "Expected an RTAS/WAP exclusivity error but got: " + exception.getMessage());

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testCannotEnableWapWhenRtasEnabled() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasWap.rtasFirst";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string) USING iceberg "
              + "TBLPROPERTIES ('replace.enabled'='true')");

      BadRequestException exception =
          assertThrows(
              BadRequestException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')"));
      assertTrue(
          exception.getMessage().contains("mutually exclusive"),
          "Expected an RTAS/WAP exclusivity error but got: " + exception.getMessage());

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testCannotCreateWithBothRtasAndWap() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasWap.both";
      spark.sql("DROP TABLE IF EXISTS " + table);

      BadRequestException exception =
          assertThrows(
              BadRequestException.class,
              () ->
                  spark.sql(
                      "CREATE TABLE "
                          + table
                          + " (id bigint, data string) USING iceberg "
                          + "TBLPROPERTIES ('replace.enabled'='true', 'write.wap.enabled'='true')"));
      assertTrue(
          exception.getMessage().contains("mutually exclusive"),
          "Expected an RTAS/WAP exclusivity error but got: " + exception.getMessage());

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }
}
