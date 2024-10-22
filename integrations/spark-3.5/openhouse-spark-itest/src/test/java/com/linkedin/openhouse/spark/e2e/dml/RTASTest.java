package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.baseSchema;
import static com.linkedin.openhouse.spark.SparkTestBase.mockTableService;
import static com.linkedin.openhouse.spark.SparkTestBase.spark;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class RTASTest {
  @Test
  public void testRTAS() throws Exception {
    Object existingTable =
        mockGetTableResponseBody(
            "dbRtas",
            "tbl5",
            "testCluster",
            "dbRtas.tbl5",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbRtas", "tbl5"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);

    Object toReplaceTable =
        mockGetTableResponseBody(
            "dbRtas",
            "tbl6",
            "testCluster",
            "dbRtas.tbl6",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbRtas", "tbl6"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);

    mockTableService.enqueue(
        mockResponse(200, existingTable)); /* Verify table that select from exists, in doRefresh */

    mockTableService.enqueue(
        mockResponse(200, toReplaceTable)); /* Verify table to replace exists, in doRefresh */

    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark.sql(
                    "REPLACE TABLE openhouse.dbRtas.tbl6 USING iceberg TBLPROPERTIES ('key'='value') AS SELECT * from openhouse.dbRtas.tbl5"));
    Assertions.assertTrue(
        unsupportedOperationException
            .getMessage()
            .contains("Replace table is not supported for OpenHouse tables"));
  }
}
