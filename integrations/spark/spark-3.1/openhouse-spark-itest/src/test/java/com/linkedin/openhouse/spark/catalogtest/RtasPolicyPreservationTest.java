package com.linkedin.openhouse.spark.catalogtest;

import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.gen.tables.client.model.PolicyTag;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Black-box tests (real embedded OpenHouse server + real Spark SQL) that CREATE OR REPLACE TABLE AS
 * SELECT (RTAS) preserves a table's policies. Spark SQL has no policy clause on RTAS, so a replace
 * always arrives at the server with no policies; the server must carry the existing table's
 * policies forward rather than wipe them. Policies are set with {@code ALTER TABLE ... SET POLICY
 * ...} and read back with {@code SHOW TBLPROPERTIES}, i.e. entirely through the customer SQL
 * surface.
 */
public class RtasPolicyPreservationTest extends OpenHouseSparkITest {

  @Test
  public void testRtasPreservesRetentionSharingAndColumnTag() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasPolicy.multiPlane";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql("CREATE TABLE " + table + " (id bigint, name string) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, '2024-01-01'), (2, '2024-01-02')");

      // Set three independent policy planes via SQL.
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET POLICY (RETENTION = 30d ON COLUMN name WHERE pattern = 'yyyy-MM-dd')");
      spark.sql("ALTER TABLE " + table + " SET POLICY (SHARING=TRUE)");
      spark.sql("ALTER TABLE " + table + " MODIFY COLUMN name SET TAG = (PII)");

      // Pre-condition: the policies are present before the replace.
      Policies before = getPolicies(spark, table);
      Assertions.assertNotNull(before.getRetention(), "retention should be set before RTAS");
      Assertions.assertTrue(before.getSharingEnabled(), "sharing should be set before RTAS");
      Assertions.assertTrue(
          columnTagContains(before, "name", PolicyTag.TagsEnum.PII),
          "column tag should be set before RTAS");

      // Opt the table into RTAS, then replace it via SQL (no policy clause exists on RTAS).
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, name FROM " + table);

      // Every policy plane must survive the replace.
      Policies after = getPolicies(spark, table);
      Assertions.assertNotNull(
          after, "RTAS wiped the entire policies plane (expected it to be preserved)");
      Assertions.assertNotNull(
          after.getRetention(), "RTAS wiped the retention policy (expected it to be preserved)");
      Assertions.assertEquals(
          "'yyyy-MM-dd'",
          after.getRetention().getColumnPattern().getPattern(),
          "RTAS changed the retention policy");
      Assertions.assertTrue(
          after.getSharingEnabled(), "RTAS wiped the sharing policy (expected it to be preserved)");
      Assertions.assertTrue(
          columnTagContains(after, "name", PolicyTag.TagsEnum.PII),
          "RTAS wiped the PII column tag (expected it to be preserved)");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasPreservesHistoryPolicy() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasPolicy.history";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql("CREATE TABLE " + table + " (id bigint, data string) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("ALTER TABLE " + table + " SET POLICY (HISTORY MAX_AGE=24H)");

      Policies before = getPolicies(spark, table);
      Assertions.assertNotNull(before.getHistory(), "history should be set before RTAS");

      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, data FROM " + table);

      Policies after = getPolicies(spark, table);
      Assertions.assertNotNull(
          after, "RTAS wiped the entire policies plane (expected it to be preserved)");
      Assertions.assertNotNull(
          after.getHistory(), "RTAS wiped the history policy (expected it to be preserved)");
      Assertions.assertEquals(
          before.getHistory().getMaxAge(),
          after.getHistory().getMaxAge(),
          "RTAS changed the history policy");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  /** Reads a table's policies through the SQL surface ({@code SHOW TBLPROPERTIES}). */
  private Policies getPolicies(SparkSession spark, String tableName) {
    List<Row> rows = spark.sql("SHOW TBLPROPERTIES " + tableName).collectAsList();
    Map<String, String> props =
        rows.stream().collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
    return new GsonBuilder()
        .setPrettyPrinting()
        .create()
        .fromJson(props.get("policies"), Policies.class);
  }

  private boolean columnTagContains(Policies policies, String column, PolicyTag.TagsEnum tag) {
    if (policies.getColumnTags() == null || !policies.getColumnTags().containsKey(column)) {
      return false;
    }
    PolicyTag policyTag = policies.getColumnTags().get(column);
    return policyTag.getTags() != null && policyTag.getTags().contains(tag);
  }
}
