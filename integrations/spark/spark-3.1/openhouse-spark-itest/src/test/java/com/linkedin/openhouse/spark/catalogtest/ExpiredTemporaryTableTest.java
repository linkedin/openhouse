package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Black-box tests (embedded OpenHouse server and Spark SQL) that an expired temporary table keeps
 * normal reads and writes until it is deleted, and that DROP TABLE deletes it.
 */
public class ExpiredTemporaryTableTest extends OpenHouseSparkITest {

  @ParameterizedTest
  @ValueSource(strings = {"DROP TABLE", "DROP TABLE IF EXISTS"})
  public void testDropDeletesExpiredTemporaryTable(String dropStatement) throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String tableName = dropStatement.endsWith("EXISTS") ? "dropIfExists" : "drop";
      String table = "openhouse.dbExpiredTemporary." + tableName;
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string) USING iceberg TBLPROPERTIES ("
              + "'openhouse.temporary'='true', 'openhouse.expirationTime'='2020-01-01T00:00:00Z')");

      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");
      Assertions.assertEquals(1L, spark.sql("SELECT * FROM " + table).count());

      spark.sql(dropStatement + " " + table);
      Assertions.assertTrue(
          spark.sql("SHOW TABLES IN openhouse.dbExpiredTemporary").collectAsList().stream()
              .noneMatch(row -> row.getString(1).equalsIgnoreCase(tableName)));
    }
  }
}
