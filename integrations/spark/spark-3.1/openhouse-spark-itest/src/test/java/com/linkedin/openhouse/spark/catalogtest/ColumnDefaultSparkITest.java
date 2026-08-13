package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import com.linkedin.openhouse.tablestest.SparkItestColumnDefaults;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Catalog → ReadBridge overlay → Spark's Iceberg table. Iceberg 1.2.0.20's Spark readers do not
 * fill {@code initial-default}; SQL fill/filter coverage is disabled until OpenHouse picks up
 * linkedin/iceberg#267–269.
 */
public class ColumnDefaultSparkITest extends OpenHouseSparkITest {

  private static final String DATABASE = SparkItestColumnDefaults.DATABASE;
  private static final String ENABLED_PROP = "read-bridge.column-default.enabled";

  @Test
  public void readBridgeOverlaysInitialDefaultAfterSparkAddColumn() throws Exception {
    String fqtn = "openhouse." + DATABASE + ".members_overlay";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("DROP TABLE IF EXISTS " + fqtn);
      spark.sql(
          "CREATE TABLE "
              + fqtn
              + " (id bigint, name string) USING iceberg TBLPROPERTIES ('"
              + ENABLED_PROP
              + "'='true')");
      spark.sql("INSERT INTO " + fqtn + " VALUES (1, 'Alice')");
      spark.sql("ALTER TABLE " + fqtn + " ADD COLUMN country string");
      spark.sql("ALTER TABLE " + fqtn + " ADD COLUMN tier int");
      spark.sql("REFRESH TABLE " + fqtn);

      Table table = Spark3Util.loadIcebergTable(spark, fqtn);
      assertEquals(
          "true",
          table.properties().get(ENABLED_PROP),
          "ramp property missing; props=" + table.properties());
      assertEquals(
          "US",
          table.schema().findField("country").initialDefault(),
          "ReadBridge overlay missing on country");
      assertEquals(
          1,
          table.schema().findField("tier").initialDefault(),
          "ReadBridge overlay missing on tier");

      spark.sql("DROP TABLE " + fqtn);
    }
  }

  /**
   * Enable after bumping {@code iceberg_1_2_version} to a build that fills {@code initial-default}
   * in Spark parquet/ORC readers (linkedin/iceberg#267–269).
   */
  @Disabled("Spark readers in iceberg 1.2.0.20 do not fill initial-default")
  @ParameterizedTest(name = "format={0}, vectorized={1}")
  @CsvSource({"parquet, false", "parquet, true", "orc, false", "orc, true"})
  public void columnDefaultBackfillViaReadBridge(String fileFormat, boolean vectorized)
      throws Exception {
    String vectorizationProp =
        "parquet".equals(fileFormat)
            ? "read.parquet.vectorization.enabled"
            : "read.orc.vectorization.enabled";
    String tableName = String.format("members_%s_%s", fileFormat, vectorized);
    String fqtn = String.format("openhouse.%s.%s", DATABASE, tableName);

    try (SparkSession spark = getSparkSession()) {
      spark.sql("DROP TABLE IF EXISTS " + fqtn);
      spark.sql(
          String.format(
              "CREATE TABLE %s (id bigint, name string) USING iceberg TBLPROPERTIES ("
                  + "'write.format.default'='%s', '%s'='%b', '%s'='true')",
              fqtn, fileFormat, vectorizationProp, vectorized, ENABLED_PROP));
      spark.sql("INSERT INTO " + fqtn + " VALUES (1, 'Alice')");
      spark.sql("ALTER TABLE " + fqtn + " ADD COLUMN country string");
      spark.sql("ALTER TABLE " + fqtn + " ADD COLUMN tier int");
      spark.sql("INSERT INTO " + fqtn + " VALUES (2, 'Bob', 'CA', 5)");
      spark.sql(
          "INSERT INTO " + fqtn + " VALUES (3, 'Cleo', CAST(NULL AS string), CAST(NULL AS int))");

      List<Object[]> full =
          rows(spark, "SELECT id, name, country, tier FROM " + fqtn + " ORDER BY id");
      assertRowsEqual(
          Arrays.asList(
              row(1L, "Alice", "US", 1), row(2L, "Bob", "CA", 5), row(3L, "Cleo", null, null)),
          full);

      assertIds(spark, fqtn, "country = 'US'", 1L);
      assertIds(spark, fqtn, "country = 'CA'", 2L);
      assertIds(spark, fqtn, "upper(country) = 'US'", 1L);
      assertIds(spark, fqtn, "tier = 1", 1L);
      assertIds(spark, fqtn, "country IS NOT NULL", 1L, 2L);
      assertIds(spark, fqtn, "country IS NULL", 3L);

      List<Object[]> projected =
          rows(spark, "SELECT country FROM " + fqtn + " WHERE country = 'US'");
      assertRowsEqual(Collections.singletonList(row("US")), projected);

      spark.sql("DROP TABLE " + fqtn);
    }
  }

  private static void assertRowsEqual(List<Object[]> expectedRows, List<Object[]> actualRows) {
    assertEquals(expectedRows.size(), actualRows.size(), "row count");
    for (int row = 0; row < expectedRows.size(); row++) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      assertEquals(expected.length, actual.length, "column count at row " + row);
      for (int col = 0; col < expected.length; col++) {
        assertEquals(expected[col], actual[col], "row " + row + " col " + col);
      }
    }
  }

  private static List<Object[]> rows(SparkSession spark, String sql) {
    return spark.sql(sql).collectAsList().stream()
        .map(
            row -> {
              Object[] values = new Object[row.size()];
              for (int i = 0; i < row.size(); i++) {
                values[i] = row.isNullAt(i) ? null : row.get(i);
              }
              return values;
            })
        .collect(Collectors.toList());
  }

  private static Object[] row(Object... values) {
    return values;
  }

  private static void assertIds(
      SparkSession spark, String fqtn, String predicate, Object... expectedIds) {
    List<Object[]> result =
        rows(spark, "SELECT id FROM " + fqtn + " WHERE " + predicate + " ORDER BY id");
    assertEquals(
        Arrays.asList(expectedIds),
        result.stream().map(r -> r[0]).collect(Collectors.toList()),
        "filter: " + predicate);
  }
}
