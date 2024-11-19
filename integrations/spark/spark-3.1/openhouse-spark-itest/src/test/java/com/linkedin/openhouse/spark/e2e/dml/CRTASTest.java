package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class CRTASTest {
  @Test
  public void testCRTASNonExistingTable() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbCRTAS",
            "tbl3",
            "testCluster",
            "dbCRTAS.tbl3",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCRTAS", "tbl3"), true), // Set true
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

    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark.sql(
                    "CREATE OR REPLACE TABLE openhouse.dbCRTAS.tbl4 USING iceberg TBLPROPERTIES ('key'='value') AS SELECT * from openhouse.dbCRTAS.tbl3"));
    Assertions.assertTrue(
        unsupportedOperationException
            .getMessage()
            .contains("Replace table is not supported for OpenHouse tables"));
  }

  @Test
  public void testCRTASExistingTable() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbCRTAS",
            "tbl3",
            "testCluster",
            "dbCRTAS.tbl3",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCRTAS", "tbl3"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);

    Object rtasTable =
        mockGetTableResponseBody(
            "dbCRTAS",
            "tbl4",
            "testCluster",
            "dbCRTAS.tbl4",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCRTAS", "tbl4"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);

    mockTableService.enqueue(
        mockResponse(200, existingTable)); /* Verify table that select from exists, in doRefresh */
    mockTableService.enqueue(
        mockResponse(
            200, rtasTable)); /* Verify table that table being replace from exists, in doRefresh */

    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark.sql(
                    "CREATE OR REPLACE TABLE openhouse.dbCRTAS.tbl4 USING iceberg TBLPROPERTIES ('key'='value') AS SELECT * from openhouse.dbCRTAS.tbl3"));
    Assertions.assertTrue(
        unsupportedOperationException
            .getMessage()
            .contains("Replace table is not supported for OpenHouse tables"));
  }
}
