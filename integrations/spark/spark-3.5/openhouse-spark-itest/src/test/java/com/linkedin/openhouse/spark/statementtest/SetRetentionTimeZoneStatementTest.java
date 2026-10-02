package com.linkedin.openhouse.spark.statementtest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
  public void testSetRetentionWithTimeZoneOnNativeTimestampColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".native_tz";
      spark.sql("CREATE TABLE " + table + " (name string, ts timestamp) PARTITIONED BY (days(ts))");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles')");
      Policies policies = storedPolicies(spark, table);
      Assertions.assertNotNull(policies.getRetention());
      Assertions.assertEquals("America/Los_Angeles", policies.getRetention().getTimeZone());
      Assertions.assertNull(policies.getRetention().getColumnPattern());
    }
  }

  @Test
  public void testSetRetentionWithZoneEncodingPatternAndTimeZoneIsRejected() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "zonepattern");
      Exception thrown =
          Assertions.assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + table
                          + " SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles'"
                          + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd-X')"));
      Assertions.assertTrue(
          messageChain(thrown).contains("already encodes a time zone"),
          "Expected the zone-encoding-pattern rejection, got: " + messageChain(thrown));
      Policies policies = storedPolicies(spark, table);
      Assertions.assertTrue(policies == null || policies.getRetention() == null);
    }
  }

  @Test
  public void testSetRetentionWithInvalidTimeZoneIsRejected() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = createStringColumnTable(spark, "invalid");
      Exception thrown =
          Assertions.assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + table
                          + " SET POLICY (RETENTION=30d WITH TIMEZONE 'Not/AZone'"
                          + " ON COLUMN name WHERE PATTERN='yyyy-MM-dd')"));
      Assertions.assertTrue(
          messageChain(thrown).contains("Invalid retention time zone 'Not/AZone'"),
          "Expected the invalid-zone validation message, got: " + messageChain(thrown));
      Policies policies = storedPolicies(spark, table);
      Assertions.assertTrue(policies == null || policies.getRetention() == null);
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

  private static String messageChain(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        builder.append(current.getMessage()).append(" | ");
      }
    }
    return builder.toString();
  }
}
