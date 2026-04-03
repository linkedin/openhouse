package com.linkedin.openhouse.spark.catalogtest;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class RTASTest extends OpenHouseSparkITest {

  private static final String tableName = "openhouse.dbRtas.rtastable";
  private static final String sourceName = "openhouse.dbRtas.source";
  private static final TableIdentifier tableIdent = TableIdentifier.of("dbRtas", "rtastable");

  public RTASTest() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) "
                  + "USING iceberg PARTITIONED BY (truncate(id, 3))",
              sourceName));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", sourceName));
    }
  }

  @AfterEach
  void cleanup() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
    }
  }

  @Test
  public void testRTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);

      // create table
      spark.sql(
          String.format(
              "CREATE TABLE %s USING iceberg TBLPROPERTIES ('prop1'='val1', 'prop2'='val2') "
                  + "AS SELECT * FROM %s",
              tableName, sourceName));

      spark.sql(String.format("ALTER TABLE %s SET POLICY (HISTORY MAX_AGE=24H)", tableName));

      String expectedTableLocation = catalog.loadTable(tableIdent).location();

      // replace table
      spark.sql(
          String.format(
              "REPLACE TABLE %s USING iceberg PARTITIONED BY (part) "
                  + "TBLPROPERTIES ('prop1'='newval1', 'prop3'='val3') AS "
                  + "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                  + "FROM %s ORDER BY 3, 1",
              tableName, sourceName));

      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "part", Types.StringType.get()));

      PartitionSpec expectedSpec =
          PartitionSpec.builderFor(expectedSchema).identity("part").withSpecId(1).build();

      Table rtasTable = catalog.loadTable(tableIdent);

      // verify table location is unchanged
      assertEquals(
          stripPathScheme(expectedTableLocation),
          stripPathScheme(rtasTable.location()),
          "Should have same table location");
      // verify schema and spec are changed
      assertEquals(
          expectedSchema.asStruct(),
          rtasTable.schema().asStruct(),
          "Should have expected nullable schema");
      assertEquals(expectedSpec, rtasTable.spec(), "Should be partitioned by part");
      // verify snapshots are preserved and main branch is unreferenced
      assertEquals(
          2, Iterables.size(rtasTable.snapshots()), "Table should have expected snapshots");
      assertNull(rtasTable.currentSnapshot().parentId(), "Current snapshot should have no parent");
      // verify table properties
      assertEquals(
          "newval1", rtasTable.properties().get("prop1"), "Should have updated table property");
      assertEquals(
          "val2", rtasTable.properties().get("prop2"), "Should have preserved table property");
      assertEquals("val3", rtasTable.properties().get("prop3"), "Should have new table property");
      // verify policies are removed
      assertEquals("", rtasTable.properties().get("policies"));
      // verify data is readable
      List<Row> rows =
          spark
              .sql(String.format("SELECT id, data, part FROM %s ORDER BY part, id", tableName))
              .collectAsList();
      assertEquals(3, rows.size());
    }
  }

  @Test
  public void testCreateRTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      // create or replace table should create the table
      spark.sql(
          String.format(
              "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part) AS "
                  + "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                  + "FROM %s ORDER BY 3, 1",
              tableName, sourceName));

      String expectedTableLocation = catalog.loadTable(tableIdent).location();

      // create or replace table should replace the table
      spark.sql(
          String.format(
              "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part) AS "
                  + "SELECT 2 * id as id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                  + "FROM %s ORDER BY 3, 1",
              tableName, sourceName));

      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "part", Types.StringType.get()));

      PartitionSpec expectedSpec =
          PartitionSpec.builderFor(expectedSchema)
              .identity("part")
              .withSpecId(0) // the spec is identical and should be reused
              .build();

      Table rtasTable = catalog.loadTable(tableIdent);

      // verify table location is unchanged
      assertEquals(
          stripPathScheme(expectedTableLocation),
          stripPathScheme(rtasTable.location()),
          "Should have same table location");
      // verify schema and spec are changed
      assertEquals(
          expectedSchema.asStruct(),
          rtasTable.schema().asStruct(),
          "Should have expected nullable schema");
      assertEquals(expectedSpec, rtasTable.spec(), "Should be partitioned by part");
      // verify snapshots are preserved and main branch is unreferenced
      assertEquals(
          2, Iterables.size(rtasTable.snapshots()), "Table should have expected snapshots");
      assertNull(rtasTable.currentSnapshot().parentId(), "Current snapshot should have no parent");
    }
  }

  @Test
  public void testDataFrameV2Replace() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);

      spark.table(sourceName).writeTo(tableName).using("iceberg").create();

      String expectedTableLocation = catalog.loadTable(tableIdent).location();

      spark
          .table(sourceName)
          .select(
              col("id"),
              col("data"),
              when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
          .orderBy("part", "id")
          .writeTo(tableName)
          .partitionedBy(col("part"))
          .using("iceberg")
          .replace();

      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "part", Types.StringType.get()));

      PartitionSpec expectedSpec =
          PartitionSpec.builderFor(expectedSchema).identity("part").withSpecId(1).build();

      Table rtasTable = catalog.loadTable(tableIdent);

      // verify table location is unchanged
      assertEquals(
          stripPathScheme(expectedTableLocation),
          stripPathScheme(rtasTable.location()),
          "Should have same table location");
      // verify schema and spec are changed
      assertEquals(
          expectedSchema.asStruct(),
          rtasTable.schema().asStruct(),
          "Should have expected nullable schema");
      assertEquals(expectedSpec, rtasTable.spec(), "Should be partitioned by part");
      // verify snapshots are preserved and main branch is unreferenced
      assertEquals(
          2, Iterables.size(rtasTable.snapshots()), "Table should have expected snapshots");
      assertNull(rtasTable.currentSnapshot().parentId(), "Current snapshot should have no parent");
    }
  }

  @Test
  public void testDataFrameV2CreateOrReplace() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);

      spark
          .table(sourceName)
          .select(
              col("id"),
              col("data"),
              when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
          .orderBy("part", "id")
          .writeTo(tableName)
          .partitionedBy(col("part"))
          .using("iceberg")
          .createOrReplace();

      String expectedTableLocation = catalog.loadTable(tableIdent).location();

      spark
          .table(sourceName)
          .select(col("id").multiply(lit(2)).as("id"), col("data"))
          .select(
              col("id"),
              col("data"),
              when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
          .orderBy("part", "id")
          .writeTo(tableName)
          .partitionedBy(col("part"))
          .using("iceberg")
          .createOrReplace();

      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "part", Types.StringType.get()));

      PartitionSpec expectedSpec =
          PartitionSpec.builderFor(expectedSchema)
              .identity("part")
              .withSpecId(0) // the spec is identical and should be reused
              .build();

      Table rtasTable = catalog.loadTable(tableIdent);

      // verify table location is unchanged
      assertEquals(
          stripPathScheme(expectedTableLocation),
          stripPathScheme(rtasTable.location()),
          "Should have same table location");
      // verify schema and spec are changed
      assertEquals(
          expectedSchema.asStruct(),
          rtasTable.schema().asStruct(),
          "Should have expected nullable schema");
      assertEquals(expectedSpec, rtasTable.spec(), "Should be partitioned by part");
      // verify snapshots are preserved and main branch is unreferenced
      assertEquals(
          2, Iterables.size(rtasTable.snapshots()), "Table should have expected snapshots");
      assertNull(rtasTable.currentSnapshot().parentId(), "Current snapshot should have no parent");
    }
  }
}
