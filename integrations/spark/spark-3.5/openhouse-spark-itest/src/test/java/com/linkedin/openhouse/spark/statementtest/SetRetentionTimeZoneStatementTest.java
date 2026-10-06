package com.linkedin.openhouse.spark.statementtest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Integration test for the retention time-zone SQL surface. Each case runs {@code ALTER TABLE ...
 * SET POLICY (RETENTION=... WITH TIMEZONE ...)} through the OpenHouse catalog against the embedded
 * tables service, then reads the stored policy back and parses it into the typed {@link Policies}
 * model to assert the zone round-trips through the service.
 */
public class SetRetentionTimeZoneStatementTest extends OpenHouseSparkITest {

  private static final String DATABASE = "db_retention_tz";

  @Test
  public void testSetRetentionWithIanaTimeZone() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "iana");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles'"
              + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd')");
      Policies policies = storedPolicies(spark, table);
      Assertions.assertNotNull(policies.getRetention());
      Assertions.assertEquals("America/Los_Angeles", policies.getRetention().getTimeZone());
      Assertions.assertEquals(
          "yyyy-MM-dd", policies.getRetention().getColumnPattern().getPattern());
      Assertions.assertEquals("name", policies.getRetention().getColumnPattern().getColumnName());
    }
  }

  @Test
  public void testSetRetentionWithFixedOffsetTimeZone() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "offset");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=12h WITH TIMEZONE '+05:30'"
              + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd-HH')");
      Policies policies = storedPolicies(spark, table);
      Assertions.assertEquals("+05:30", policies.getRetention().getTimeZone());
    }
  }

  @Test
  public void testSetRetentionWithoutTimeZoneStoresNoZone() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "notz");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=30d ON COLUMN name WHERE PATTERN='yyyy-MM-dd')");
      Policies policies = storedPolicies(spark, table);
      Assertions.assertNotNull(policies.getRetention());
      Assertions.assertNull(policies.getRetention().getTimeZone());
    }
  }

  @Test
  public void testSetRetentionRejectsTimeZoneOnNativeTimestampColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".native_tz";
      spark.sql("CREATE TABLE " + table + " (name string, ts timestamp) PARTITIONED BY (days(ts))");
      spark.sql("ALTER TABLE " + table + " SET POLICY (RETENTION=30d)");
      Policies policies = storedPolicies(spark, table);
      Assertions.assertThrows(
          AnalysisException.class,
          () ->
              spark.sql(
                  "ALTER TABLE "
                      + table
                      + " SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles')"));
      Assertions.assertEquals(policies, storedPolicies(spark, table));
      Assertions.assertNull(policies.getRetention().getTimeZone());
      Assertions.assertNull(policies.getRetention().getColumnPattern());
    }
  }

  @Test
  public void testSetRetentionWithZoneEncodingPatternAndTimeZoneIsRejected() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "zonepattern");
      spark.sql("ALTER TABLE " + table + " SET POLICY (RETENTION=7d ON COLUMN name)");
      Policies originalPolicies = storedPolicies(spark, table);
      AnalysisException thrown =
          Assertions.assertThrows(
              AnalysisException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + table
                          + " SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles'"
                          + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd-X')"));
      Assertions.assertTrue(
          thrown.getMessage().contains("already encodes a time zone"), thrown.getMessage());
      Assertions.assertEquals(originalPolicies, storedPolicies(spark, table));
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {"invalid,Not/AZone", "empty,''", "blank,' '"},
      ignoreLeadingAndTrailingWhitespace = false)
  public void testSetRetentionWithInvalidTimeZoneIsRejected(String tableSuffix, String timeZone)
      throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, tableSuffix);
      spark.sql("ALTER TABLE " + table + " SET POLICY (RETENTION=7d ON COLUMN name)");
      Policies originalPolicies = storedPolicies(spark, table);
      AnalysisException thrown =
          Assertions.assertThrows(
              AnalysisException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + table
                          + " SET POLICY (RETENTION=30d WITH TIMEZONE '"
                          + timeZone
                          + "'"
                          + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd')"));
      Assertions.assertTrue(
          thrown.getMessage().contains("Invalid retention time zone"), thrown.getMessage());
      Assertions.assertEquals(originalPolicies, storedPolicies(spark, table));
    }
  }

  @Test
  public void testSetRetentionUpdatesAndRemovesTimeZone() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "updates");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=3d WITH TIMEZONE 'America/Los_Angeles' ON COLUMN name)");
      Assertions.assertEquals(
          "yyyy-MM-dd",
          storedPolicies(spark, table).getRetention().getColumnPattern().getPattern());
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=3d WITH TIMEZONE '+05:30' ON COLUMN name)");
      Assertions.assertEquals("+05:30", storedPolicies(spark, table).getRetention().getTimeZone());
      spark.sql("ALTER TABLE " + table + " SET POLICY (RETENTION=3d ON COLUMN name)");
      Assertions.assertNull(storedPolicies(spark, table).getRetention().getTimeZone());
    }
  }

  @Test
  public void testSetRetentionRejectsTimeZoneOnNumericColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".numeric";
      spark.sql("CREATE TABLE " + table + " (number int)");
      AnalysisException failure =
          Assertions.assertThrows(
              AnalysisException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + table
                          + " SET POLICY (RETENTION=3d WITH TIMEZONE 'UTC' ON COLUMN number)"));
      Assertions.assertTrue(failure.getMessage().contains("requires a string retention column"));
    }
  }

  private static String createStringColumnTable(SparkSession spark, String name) {
    String table = "openhouse." + DATABASE + "." + name;
    spark.sql("CREATE TABLE " + table + " (name string)");
    return table;
  }

  private static Policies storedPolicies(SparkSession spark, String table) {
    List<Row> properties = spark.sql("SHOW TBLPROPERTIES " + table).collectAsList();
    Map<String, String> propertyByKey =
        properties.stream()
            .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
    Gson gson = new GsonBuilder().create();
    return gson.fromJson(propertyByKey.get("policies"), Policies.class);
  }
}
