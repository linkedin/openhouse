package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class IncrementalReadTest {

  @Test
  public void testIncrementalReadBetweenSnapshots() {
    TableIdentifier tableId = TableIdentifier.of("dbIncr", "tbl");

    // Create table with first batch of data: ('1', 'a'), ('2', 'b') → snapshot 1
    mockTableLocationDefaultSchema(tableId, true);
    // Insert second batch: ('3', 'c'), ('4', 'd') → snapshot 2
    String tableLocation =
        mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('3', 'c'), ('4', 'd')");

    Object mockResponseBody =
        mockGetTableResponseBody(
            "dbIncr",
            "tbl",
            "testCluster",
            "dbIncr.tbl",
            "ABCD",
            tableLocation,
            "V1",
            baseSchema,
            null,
            null);

    // Mock for querying .snapshots metadata table
    mockTableService.enqueue(mockResponse(200, mockResponseBody));

    List<Row> snapshots =
        spark
            .sql("SELECT * FROM openhouse.dbIncr.tbl.snapshots ORDER BY committed_at")
            .collectAsList();
    Assertions.assertEquals(2, snapshots.size(), "Should have exactly 2 snapshots");

    long startSnapshotId = snapshots.get(0).getLong(snapshots.get(0).fieldIndex("snapshot_id"));
    long endSnapshotId = snapshots.get(1).getLong(snapshots.get(1).fieldIndex("snapshot_id"));

    // Mock for incremental read via DataFrame API (doRefresh calls)
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh
    mockTableService.enqueue(mockResponse(200, mockResponseBody)); // doRefresh

    // Incremental read: should return only data added between snapshot 1 (exclusive) and
    // snapshot 2 (inclusive), i.e. only batch 2
    List<String> incrementalRows =
        spark.read().format("iceberg").option("start-snapshot-id", String.valueOf(startSnapshotId))
            .option("end-snapshot-id", String.valueOf(endSnapshotId)).load("openhouse.dbIncr.tbl")
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());

    Assertions.assertEquals(2, incrementalRows.size(), "Incremental read should return 2 rows");
    Assertions.assertTrue(
        incrementalRows.containsAll(ImmutableList.of("3.c", "4.d")),
        "Incremental read should contain only the second batch of data");
    Assertions.assertFalse(
        incrementalRows.contains("1.a"),
        "Incremental read should NOT contain data from the first batch");
  }

  @Test
  public void testIncrementalReadSingleSnapshotRange() {
    TableIdentifier tableId = TableIdentifier.of("dbIncr", "tblSingle");

    // Create table with first batch: ('1', 'a'), ('2', 'b') → snapshot 1
    mockTableLocationDefaultSchema(tableId, true);
    // Insert second batch: ('3', 'c') → snapshot 2
    mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('3', 'c')");
    // Insert third batch: ('4', 'd') → snapshot 3
    String tableLocation =
        mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('4', 'd')");

    Object mockResponseBody =
        mockGetTableResponseBody(
            "dbIncr",
            "tblSingle",
            "testCluster",
            "dbIncr.tblSingle",
            "ABCD",
            tableLocation,
            "V1",
            baseSchema,
            null,
            null);

    // Mock for querying .snapshots metadata table
    mockTableService.enqueue(mockResponse(200, mockResponseBody));

    List<Row> snapshots =
        spark
            .sql("SELECT * FROM openhouse.dbIncr.tblSingle.snapshots ORDER BY committed_at")
            .collectAsList();
    Assertions.assertEquals(3, snapshots.size());

    long snap1 = snapshots.get(0).getLong(snapshots.get(0).fieldIndex("snapshot_id"));
    long snap2 = snapshots.get(1).getLong(snapshots.get(1).fieldIndex("snapshot_id"));

    // Mock for incremental read from snap1 (exclusive) to snap2 (inclusive)
    // Should return only the single row added in snapshot 2
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh
    mockTableService.enqueue(mockResponse(200, mockResponseBody)); // doRefresh

    List<String> incrementalRows =
        spark.read().format("iceberg").option("start-snapshot-id", String.valueOf(snap1))
            .option("end-snapshot-id", String.valueOf(snap2)).load("openhouse.dbIncr.tblSingle")
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());

    Assertions.assertEquals(
        1, incrementalRows.size(), "Should return exactly 1 row from snapshot 2");
    Assertions.assertTrue(
        incrementalRows.contains("3.c"), "Should contain only the row added in snapshot 2");
  }

  @Test
  public void testIncrementalReadWithThreeSnapshots() {
    TableIdentifier tableId = TableIdentifier.of("dbIncr", "tbl3");

    // Create table with batch 1: ('1', 'a'), ('2', 'b') → snapshot 1
    mockTableLocationDefaultSchema(tableId, true);
    // Insert batch 2: ('3', 'c'), ('4', 'd') → snapshot 2
    mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('3', 'c'), ('4', 'd')");
    // Insert batch 3: ('5', 'e'), ('6', 'f') → snapshot 3
    String tableLocation =
        mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('5', 'e'), ('6', 'f')");

    Object mockResponseBody =
        mockGetTableResponseBody(
            "dbIncr",
            "tbl3",
            "testCluster",
            "dbIncr.tbl3",
            "ABCD",
            tableLocation,
            "V1",
            baseSchema,
            null,
            null);

    // Mock for querying .snapshots metadata table
    mockTableService.enqueue(mockResponse(200, mockResponseBody));

    List<Row> snapshots =
        spark
            .sql("SELECT * FROM openhouse.dbIncr.tbl3.snapshots ORDER BY committed_at")
            .collectAsList();
    Assertions.assertEquals(3, snapshots.size(), "Should have exactly 3 snapshots");

    long snap1 = snapshots.get(0).getLong(snapshots.get(0).fieldIndex("snapshot_id"));
    long snap3 = snapshots.get(2).getLong(snapshots.get(2).fieldIndex("snapshot_id"));

    // Mock for incremental read spanning snapshots 1 to 3
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh
    mockTableService.enqueue(mockResponse(200, mockResponseBody)); // doRefresh

    // Incremental read from snapshot 1 (exclusive) to snapshot 3 (inclusive)
    // Should return batches 2 and 3
    List<String> incrementalRows =
        spark.read().format("iceberg").option("start-snapshot-id", String.valueOf(snap1))
            .option("end-snapshot-id", String.valueOf(snap3)).load("openhouse.dbIncr.tbl3")
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());

    Assertions.assertEquals(4, incrementalRows.size(), "Should return 4 rows from batches 2 and 3");
    Assertions.assertTrue(
        incrementalRows.containsAll(ImmutableList.of("3.c", "4.d", "5.e", "6.f")),
        "Should contain data from batches 2 and 3");
    Assertions.assertFalse(incrementalRows.contains("1.a"), "Should NOT contain data from batch 1");
  }

  @Test
  public void testIncrementalReadWithOverwriteInRange() {
    TableIdentifier tableId = TableIdentifier.of("dbIncr", "tblOvw");

    // Create table with batch 1: ('1', 'a'), ('2', 'b') → append snapshot 1
    mockTableLocationDefaultSchema(tableId, true);
    // Overwrite all data: ('3', 'c') → overwrite snapshot 2
    mockTableLocationAfterOperation(tableId, "INSERT OVERWRITE %t VALUES ('3', 'c')");
    // Insert batch 3: ('4', 'd') → append snapshot 3
    String tableLocation =
        mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('4', 'd')");

    Object mockResponseBody =
        mockGetTableResponseBody(
            "dbIncr",
            "tblOvw",
            "testCluster",
            "dbIncr.tblOvw",
            "ABCD",
            tableLocation,
            "V1",
            baseSchema,
            null,
            null);

    // Mock for querying .snapshots metadata table
    mockTableService.enqueue(mockResponse(200, mockResponseBody));

    List<Row> snapshots =
        spark
            .sql("SELECT * FROM openhouse.dbIncr.tblOvw.snapshots ORDER BY committed_at")
            .collectAsList();
    Assertions.assertEquals(3, snapshots.size(), "Should have exactly 3 snapshots");

    long snap1 = snapshots.get(0).getLong(snapshots.get(0).fieldIndex("snapshot_id"));
    long snap3 = snapshots.get(2).getLong(snapshots.get(2).fieldIndex("snapshot_id"));

    // Verify the middle snapshot is an overwrite operation
    String snap2Operation = snapshots.get(1).getString(snapshots.get(1).fieldIndex("operation"));
    Assertions.assertEquals("overwrite", snap2Operation, "Middle snapshot should be an overwrite");

    // Mock for incremental read attempt
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh
    mockTableService.enqueue(mockResponse(200, mockResponseBody)); // doRefresh

    // Iceberg 1.2 (Spark 3.1): IncrementalAppendScan rejects non-append snapshots in range
    UnsupportedOperationException e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option("start-snapshot-id", String.valueOf(snap1))
                    .option("end-snapshot-id", String.valueOf(snap3))
                    .load("openhouse.dbIncr.tblOvw")
                    .collectAsList());
    Assertions.assertTrue(
        e.getMessage().contains("overwrite"), "Error should mention the non-append operation type");
  }
}
