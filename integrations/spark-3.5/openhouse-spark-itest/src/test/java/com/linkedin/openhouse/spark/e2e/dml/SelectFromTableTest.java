package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.MockResponse;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class SelectFromTableTest {

  @Test
  public void testSelectTables() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbSelect",
                "tbl",
                "testCluster",
                "dbSelect.tb1",
                "ABCD",
                mockTableLocationDefaultSchema(TableIdentifier.of("dbSelect", "tbl"), true),
                "V1",
                baseSchema,
                null,
                null)));

    List<String> actualRows =
        spark.sql("SELECT * FROM openhouse.dbSelect.tbl").collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(actualRows.containsAll(ImmutableList.of("1.a", "2.b")));
  }

  @Test
  public void testSelectTablesTimeTravel() {
    MockResponse mockResponse =
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbSelect",
                "tbl",
                "testCluster",
                "dbSelect.tb1",
                "ABCD",
                mockTableLocationDefaultSchema(TableIdentifier.of("dbSelect", "tbl"), true),
                "V1",
                baseSchema,
                null,
                null));
    mockTableService.enqueue(mockResponse);

    List<Row> actualRows =
        spark.sql("SELECT * FROM openhouse.dbSelect.tbl.snapshots").collectAsList();
    Assertions.assertEquals(1, actualRows.size());

    Timestamp committedAt =
        actualRows.get(0).getTimestamp(actualRows.get(0).fieldIndex("committed_at"));
    Long snapshotId = actualRows.get(0).getLong(actualRows.get(0).fieldIndex("snapshot_id"));

    // doRefresh()
    mockTableService.enqueue(
        mockResponse(
            404,
            mockGetAllTableResponseBody())); // Verify table to create doesn't exist, in doRefresh()

    // doRefresh()
    mockTableService.enqueue(mockResponse);

    List<String> returnedRowsQuery1 =
        spark.sql("SELECT * FROM openhouse.dbSelect.tbl.snapshot_id_" + snapshotId).collectAsList()
            .stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(returnedRowsQuery1.containsAll(ImmutableList.of("1.a", "2.b")));

    // doRefresh()
    mockTableService.enqueue(
        mockResponse(
            404,
            mockGetAllTableResponseBody())); // Verify table to create doesn't exist, in doRefresh()

    // doRefresh()
    mockTableService.enqueue(mockResponse);

    List<String> returnedRowsQuery2 =
        spark.sql("SELECT * FROM openhouse.dbSelect.tbl.at_timestamp_" + committedAt.getTime())
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(returnedRowsQuery2.containsAll(ImmutableList.of("1.a", "2.b")));
  }

  @Test
  public void testSelectTablesFromEmptyTable() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbSelect",
                "emptyTbl",
                "testCluster",
                "dbSelect.emptyTbl",
                "ABCD",
                mockTableLocationDefaultSchema(TableIdentifier.of("dbSelect", "emptyTbl")),
                "V1",
                baseSchema,
                null,
                null)));

    Assertions.assertTrue(
        spark.sql("SELECT * FROM openhouse.dbSelect.emptyTbl").collectAsList().isEmpty());
  }

  @Test
  public void testAuthzError() {
    mockTableService.enqueue(
        mockResponse(
            403,
            "{\"status\":\"FORBIDDEN\",\"error\":\"forbidden\",\"message\":\"Operation on table dbSelect.errorTbl failed as user sraikar is unauthorized\"}"));
    WebClientResponseException exception =
        Assertions.assertThrows(
            WebClientResponseException.class,
            () -> spark.sql("SELECT * FROM openhouse.dbSelect.errorTbl"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Operation on table dbSelect.errorTbl failed as user sraikar is unauthorized"));
  }
}
