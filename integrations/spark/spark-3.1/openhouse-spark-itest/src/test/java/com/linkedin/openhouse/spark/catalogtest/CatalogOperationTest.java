package com.linkedin.openhouse.spark.catalogtest;

import com.google.common.collect.Sets;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

public class CatalogOperationTest extends OpenHouseSparkITest {
  @Test
  public void testCasingWithCTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      spark.sql("CREATE TABLE openhouse.d1.`tT1` (name string)");
      // testing writing behavior, note the casing of tt1 is intentionally changed.
      spark.sql("INSERT INTO openhouse.d1.Tt1 VALUES ('foo')");

      // Verifying by querying with all lower-cased name
      Assertions.assertEquals(
          1, spark.sql("SELECT * from openhouse.d1.tt1").collectAsList().size());
      // ctas but referring with lower-cased name
      spark.sql("CREATE TABLE openhouse.d1.t2 AS SELECT * from openhouse.d1.tt1");
      Assertions.assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t2").collectAsList().size());
    }
  }

  @Test
  public void testCatalogWriteAPI() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      // Create a table
      Schema schema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
      TableIdentifier tableIdentifier = TableIdentifier.of("db", "aaa");
      icebergCatalog.createTable(tableIdentifier, schema);

      // Write into data with intentionally changed casing in name
      TableIdentifier tableIdentifierUpperTblName = TableIdentifier.of("db", "AAA");

      DataFile fooDataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/data-a.parquet")
              .withFileSizeInBytes(10)
              .withRecordCount(1)
              .build();
      AtomicReference<Table> tableRef = new AtomicReference<>();
      Assertions.assertDoesNotThrow(
          () -> {
            Table loadedTable = icebergCatalog.loadTable(tableIdentifierUpperTblName);
            tableRef.set(loadedTable);
          });
      Table table = tableRef.get();
      Assertions.assertDoesNotThrow(
          () -> {
            table.newAppend().appendFile(fooDataFile).commit();
          });
    }
  }

  @Test
  public void testCreateReplicaUnPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);

      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));

      // Field ids reassigned (Status quo)
      TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
      Map<String, String> props = new HashMap<>();
      Table table =
          icebergCatalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), props);
      Schema schemaAfterCreation = table.schema();
      Assertions.assertFalse(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(schemaAfterCreation.findField("a").fieldId(), 1);
      Assertions.assertEquals(schemaAfterCreation.findField("a.b").fieldId(), 3);
      Assertions.assertEquals(schemaAfterCreation.findField("c").fieldId(), 2);
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("d").fieldId(), 4);
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("a.e").fieldId(), 5);

      // Field ids not reassigned
      tableIdentifier = TableIdentifier.of("db", "table1");
      props.put("source.table.schema", SchemaParser.toJson(schema));
      table =
          icebergCatalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), props);
      schemaAfterCreation = table.schema();
      Assertions.assertTrue(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(schemaAfterCreation.findField("a").fieldId(), 1);
      Assertions.assertEquals(schemaAfterCreation.findField("a.b").fieldId(), 2);
      Assertions.assertEquals(schemaAfterCreation.findField("c").fieldId(), 3);
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("d").fieldId(), 4);
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("a.e").fieldId(), 5);
    }
  }

  @Test
  public void testCreateReplicaPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);

      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));
      // Parititon spec with identity partitioning on "c"
      PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("c").build();

      // Field ids reassigned (Status quo)
      TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
      Map<String, String> props = new HashMap<>();
      Table table = icebergCatalog.createTable(tableIdentifier, schema, partitionSpec, props);
      Schema schemaAfterCreation = table.schema();
      Assertions.assertFalse(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(schemaAfterCreation.findField("a").fieldId(), 1);
      Assertions.assertEquals(schemaAfterCreation.findField("a.b").fieldId(), 3);
      Assertions.assertEquals(schemaAfterCreation.findField("c").fieldId(), 2);
      PartitionSpec pspecAfterCreation = table.spec();
      // pspec on c changes to 2
      Assertions.assertEquals(pspecAfterCreation.identitySourceIds(), Sets.newHashSet(2));
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("d").fieldId(), 4);
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("a.e").fieldId(), 5);

      // Field ids not reassigned (new changes)
      tableIdentifier = TableIdentifier.of("db", "table1");
      props.put("source.table.schema", SchemaParser.toJson(schema));
      table = icebergCatalog.createTable(tableIdentifier, schema, partitionSpec, props);
      schemaAfterCreation = table.schema();
      Assertions.assertTrue(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(schemaAfterCreation.findField("a").fieldId(), 1);
      Assertions.assertEquals(schemaAfterCreation.findField("a.b").fieldId(), 2);
      Assertions.assertEquals(schemaAfterCreation.findField("c").fieldId(), 3);
      pspecAfterCreation = table.spec();
      // pspec on c changes remains 3
      Assertions.assertEquals(pspecAfterCreation.identitySourceIds(), Sets.newHashSet(3));
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("d").fieldId(), 4);
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(table.schema().findField("a.e").fieldId(), 5);
    }
  }

  /**
   * This is a copy of com.linkedin.openhouse.jobs.spark.Operations#getCatalog() temporarily.
   * Refactoring these pieces require deployment coordination, thus we shall create an artifact
   * module that can be pulled by :apps module.
   */
  private Catalog getOpenHouseCatalog(SparkSession spark) {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = String.format("spark.sql.catalog.openhouse.");
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    // this initializes the catalog based on runtime Catalog class passed in catalog-impl conf.
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        "openhouse",
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }
}
