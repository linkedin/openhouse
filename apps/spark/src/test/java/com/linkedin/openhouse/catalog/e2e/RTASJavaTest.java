package com.linkedin.openhouse.catalog.e2e;

import static org.apache.iceberg.types.Types.NestedField.*;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class RTASJavaTest extends OpenHouseSparkITest {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("dbRtas", "rtastable");

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final Schema REPLACE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "part", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final PartitionSpec REPLACE_SPEC =
      PartitionSpec.builderFor(REPLACE_SCHEMA).identity("part").build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();

  @AfterEach
  void cleanup() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      if (catalog.tableExists(TABLE_IDENT)) {
        catalog.dropTable(TABLE_IDENT);
      }
    }
  }

  @Test
  void testReplaceTransaction() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);

      // create table and append data
      Table table = catalog.buildTable(TABLE_IDENT, SCHEMA).withPartitionSpec(SPEC).create();
      table.newAppend().appendFile(FILE_A).commit();

      String originalLocation = table.location();
      String originalMetadataLocation = getMetadataLocation(table);
      long originalSnapshotId = table.currentSnapshot().snapshotId();

      // create a replace transaction
      Transaction txn =
          catalog
              .buildTable(TABLE_IDENT, REPLACE_SCHEMA)
              .withPartitionSpec(REPLACE_SPEC)
              .replaceTransaction();

      Table existingTable = catalog.loadTable(TABLE_IDENT);

      // verify underlying table is unchanged
      assertEquals(
          originalMetadataLocation,
          getMetadataLocation(existingTable),
          "Table location should be unchanged before commit");

      // verify the transaction holds the new metadata
      assertEquals(
          REPLACE_SCHEMA.asStruct(),
          txn.table().schema().asStruct(),
          "Transaction should have the new schema");

      // commit the transaction
      DataFile fileB = buildFile(txn.table().spec());
      txn.newAppend().appendFile(fileB).commit();
      txn.commitTransaction();

      Table replacedTable = catalog.loadTable(TABLE_IDENT);

      // verify location is preserved
      assertEquals(
          stripPathScheme(originalLocation),
          stripPathScheme(replacedTable.location()),
          "Table location should be preserved after replace");
      // verify schema is updated
      assertEquals(
          REPLACE_SCHEMA.asStruct(),
          replacedTable.schema().asStruct(),
          "Schema should be updated after replace");
      // verify spec is updated
      assertEquals(
          "part",
          replacedTable.spec().fields().get(0).name(),
          "Partition spec should be updated after replace");
      // verify snapshots are preserved and main branch is reset
      assertEquals(2, Iterables.size(replacedTable.snapshots()), "Should have two snapshots");
      assertNull(
          replacedTable.currentSnapshot().parentId(),
          "Current snapshot should have no parent after replace");
      assertNotEquals(
          originalSnapshotId,
          replacedTable.currentSnapshot().snapshotId(),
          "Current snapshot should be the new one");
    }
  }

  @Test
  void testCreateOrReplaceTransaction() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);

      // create or replace on non-existent table should create it
      Transaction createTxn =
          catalog
              .buildTable(TABLE_IDENT, SCHEMA)
              .withPartitionSpec(SPEC)
              .createOrReplaceTransaction();
      createTxn.newAppend().appendFile(FILE_A).commit();
      createTxn.commitTransaction();

      Table table = catalog.loadTable(TABLE_IDENT);
      String originalLocation = table.location();
      assertEquals(1, Iterables.size(table.snapshots()), "Should have one snapshot after create");

      long originalSnapshotId = table.currentSnapshot().snapshotId();

      // create or replace on existing table should replace it
      Transaction replaceTxn =
          catalog
              .buildTable(TABLE_IDENT, REPLACE_SCHEMA)
              .withPartitionSpec(REPLACE_SPEC)
              .createOrReplaceTransaction();

      // commit the transaction
      DataFile fileB = buildFile(replaceTxn.table().spec());
      replaceTxn.newAppend().appendFile(fileB).commit();
      replaceTxn.commitTransaction();

      Table replacedTable = catalog.loadTable(TABLE_IDENT);

      // verify location is preserved
      assertEquals(
          stripPathScheme(originalLocation),
          stripPathScheme(replacedTable.location()),
          "Table location should be preserved after create or replace");
      // verify schema is updated
      assertEquals(
          REPLACE_SCHEMA.asStruct(),
          replacedTable.schema().asStruct(),
          "Schema should be updated after create or replace");
      // verify spec is updated
      assertEquals(
          "part",
          replacedTable.spec().fields().get(0).name(),
          "Partition spec should be updated after create or replace");
      // verify snapshots are preserved and main branch is reset
      assertEquals(2, Iterables.size(replacedTable.snapshots()), "Should have two snapshots");
      assertNull(
          replacedTable.currentSnapshot().parentId(),
          "Current snapshot should have no parent after replace");
      assertNotEquals(
          originalSnapshotId,
          replacedTable.currentSnapshot().snapshotId(),
          "Current snapshot should be the new one");
    }
  }

  /**
   * Builds a DataFile using the given spec rather than a static spec. This is necessary because the
   * spec must carry the correct spec ID assigned by the transaction; a standalone spec defaults to
   * specId=0 which collides with the original table's spec and causes type-mismatch validation
   * errors (e.g. bucket expects Integer but identity partition supplies String).
   */
  private static DataFile buildFile(PartitionSpec spec) {
    return DataFiles.builder(spec)
        .withPath("/path/to/data-b.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("part=odd")
        .withRecordCount(1)
        .build();
  }

  private String getMetadataLocation(Table table) {
    return table.properties().get("openhouse.tableLocation");
  }
}
