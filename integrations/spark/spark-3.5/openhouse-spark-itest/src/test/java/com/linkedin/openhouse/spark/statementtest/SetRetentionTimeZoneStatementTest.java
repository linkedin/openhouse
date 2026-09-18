package com.linkedin.openhouse.spark.statementtest;

import java.nio.file.Files;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Verifies the Spark 3.5 SQL extension parses the retention time-zone clause {@code WITH TIMEZONE
 * '<zone>'} on {@code ALTER TABLE ... SET POLICY (RETENTION=...)} and serializes the zone into the
 * stored retention policy.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetRetentionTimeZoneStatementTest {
  private static SparkSession spark;

  @BeforeAll
  public void setupSpark() throws Exception {
    Path unittest = new Path(Files.createTempDirectory("unittest_settzpolicy").toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.openhouse.type", "hadoop")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    spark.sql("CREATE TABLE openhouse.db.table (id bigint, data string) USING iceberg").show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE openhouse.db.table").show();
  }

  @Test
  public void testSetRetentionWithTimeZone() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles')")
        .show();
    String policy = storedPolicy("openhouse.db.table");
    Assertions.assertTrue(policy.contains("\"timeZone\":\"America/Los_Angeles\""), policy);
    Assertions.assertTrue(policy.contains("\"granularity\":\"DAY\""), policy);
  }

  @Test
  public void testSetRetentionWithTimeZoneAndColumnPattern() {
    spark
        .sql(
            "ALTER TABLE openhouse.db.table SET POLICY (RETENTION=30d WITH TIMEZONE 'America/Los_Angeles'"
                + " ON COLUMN ts WHERE PATTERN='yyyy-MM-dd')")
        .show();
    String policy = storedPolicy("openhouse.db.table");
    Assertions.assertTrue(policy.contains("\"timeZone\":\"America/Los_Angeles\""), policy);
    Assertions.assertTrue(policy.contains("\"columnName\":\"ts\""), policy);
  }

  @Test
  public void testSetRetentionWithFixedOffsetTimeZone() {
    spark
        .sql("ALTER TABLE openhouse.db.table SET POLICY (RETENTION=12h WITH TIMEZONE '+05:30')")
        .show();
    String policy = storedPolicy("openhouse.db.table");
    Assertions.assertTrue(policy.contains("\"timeZone\":\"+05:30\""), policy);
    Assertions.assertTrue(policy.contains("\"granularity\":\"HOUR\""), policy);
  }

  @Test
  public void testSetRetentionWithoutTimeZoneOmitsField() {
    spark.sql("ALTER TABLE openhouse.db.table SET POLICY (RETENTION=30d)").show();
    String policy = storedPolicy("openhouse.db.table");
    Assertions.assertTrue(policy.contains("\"granularity\":\"DAY\""), policy);
    Assertions.assertFalse(policy.contains("timeZone"), policy);
  }

  @Test
  public void testSetRetentionWithInvalidTimeZoneIsRejected() {
    // An unresolvable zone is rejected when the statement runs rather than persisted, so the
    // retention
    // job never receives a policy it cannot evaluate.
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (RETENTION=30d WITH TIMEZONE 'Not/AZone')")
                .collectAsList());
    String policy = storedPolicy("openhouse.db.table");
    Assertions.assertFalse(policy.contains("Not/AZone"), policy);
  }

  private String storedPolicy(String table) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .map(row -> row.getString(0) + "=" + row.getString(1))
        .collect(Collectors.joining("\n", "", "\n"));
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }
}
