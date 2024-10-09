package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.MockHelpers.mockGetAllTableResponseBody;
import static com.linkedin.openhouse.spark.MockHelpers.mockResponse;
import static com.linkedin.openhouse.spark.SparkTestBase.baseSchema;
import static com.linkedin.openhouse.spark.SparkTestBase.mockTableService;
import static com.linkedin.openhouse.spark.SparkTestBase.spark;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class CTASTest {

  @Test
  public void testCTAS() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbCtas",
            "tbl3",
            "testCluster",
            "dbCtas.tbl3",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCtas", "tbl3"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(
        mockResponse(200, existingTable)); /* Verify table that select from exists, in doRefresh */

    mockTableService.enqueue(
        mockResponse(
            404,
            mockGetAllTableResponseBody())); // Verify table to create doesn't exist, in doRefresh()
    mockTableService.enqueue(
        mockResponse(
            404, mockGetAllTableResponseBody())); // Verify database to create doesn't exist, in
    // doRefresh()
    mockTableService.enqueue(
        mockResponse(
            404,
            mockGetAllTableResponseBody())); // Verify table to create doesn't exist, in doRefresh()
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbCtas",
                "tbl4",
                "testCluster",
                "dbCtas.tbl4",
                "ABCDE",
                mockTableLocationAfterOperation(
                    TableIdentifier.of("dbCtas", "tbl4"),
                    "CREATE TABLE %t (col1 string, col2 string) TBLPROPERTIES('openhouse.tableId'='t1')"),
                "V1",
                baseSchema,
                null,
                null)));
    mockTableService.enqueue(
        mockResponse(
            201,
            mockGetTableResponseBody(
                "dbCtas",
                "tbl4",
                "testCluster",
                "dbCtas.tbl4",
                "ABCDE",
                mockTableLocationDefaultSchema(
                    TableIdentifier.of("dbCtas", "tbl4"), true), // putSnapshot
                "V1",
                baseSchema,
                null,
                null)));
    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                "CREATE TABLE openhouse.dbCtas.tbl4 USING iceberg AS SELECT * from openhouse.dbCtas.tbl3"));
  }

  @Test
  public void testCTASTableExists() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbCtas",
            "tbl3",
            "testCluster",
            "dbCtas.tbl3",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCtas", "tbl3"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(
        mockResponse(200, existingTable)); /* Verify table that select from exists, in doRefresh */
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbCtas",
                "tbl4",
                "testCluster",
                "dbCtas.tbl4",
                "ABCDE",
                mockTableLocationDefaultSchema(
                    TableIdentifier.of("dbCtas", "tbl4"), false), // Table already exists
                "V1",
                baseSchema,
                null,
                null)));
    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () ->
            spark.sql("CREATE TABLE openhouse.dbCtas.tbl4 AS SELECT * from openhouse.dbCtas.tbl3"));
  }
}
