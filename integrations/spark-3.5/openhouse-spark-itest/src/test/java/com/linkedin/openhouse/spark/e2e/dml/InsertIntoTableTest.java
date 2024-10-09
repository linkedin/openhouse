package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class InsertIntoTableTest {

  @Test
  public void testInsertIntoFreshTable() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbInsert",
            "tbl",
            "testCluster",
            "dbInsert.tb1",
            "ABCD",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbInsert", "tbl")),
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    Object tableAfterInsert =
        mockGetTableResponseBody(
            "dbInsert",
            "tbl",
            "testCluster",
            "dbInsert.tb1",
            "ABCD",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsert", "tbl"),
                "INSERT INTO %t VALUES ('1', 'a'), ('2', 'b')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doCommit
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh

    Assertions.assertDoesNotThrow(
        () -> spark.sql("INSERT INTO openhouse.dbInsert.tbl VALUES ('1', 'a'), ('2', 'b')"));
  }

  @Test
  public void testInsertIntoTableWithData() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbInsert",
            "tbl2",
            "testCluster",
            "dbInsert.tbl2",
            "ABCD",
            mockTableLocationDefaultSchema(
                TableIdentifier.of("dbInsert", "tbl2"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    Object tableAfterInsert =
        mockGetTableResponseBody(
            "dbInsert",
            "tbl2",
            "testCluster",
            "dbInsert.tbl2",
            "ABCD",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsert", "tbl2"),
                "INSERT INTO %t VALUES ('3', 'c'), ('4', 'd')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doCommit
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh

    Assertions.assertDoesNotThrow(
        () -> spark.sql("INSERT INTO openhouse.dbInsert.tbl2 VALUES ('3', 'c'), ('4', 'd')"));
  }

  @Test
  public void testAuthZFailure() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbInsert",
            "tblError",
            "testCluster",
            "dbInsert.tblError",
            "ABCD",
            mockTableLocationDefaultSchema(
                TableIdentifier.of("dbInsert", "tblError"), true), // Set true
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh
    mockTableService.enqueue(
        mockResponse(
            403,
            "{\"status\":\"FORBIDDEN\",\"error\":\"forbidden\",\"message\":\"Operation on table dbInsert.tblError failed as user sraikar is unauthorized\"}"));
    SparkException exception =
        Assertions.assertThrows(
            SparkException.class,
            () ->
                spark.sql("INSERT INTO openhouse.dbInsert.tblError VALUES ('3', 'c'), ('4', 'd')"));
    Assertions.assertTrue(
        ExceptionUtils.getStackTrace(exception)
            .contains(
                "Operation on table dbInsert.tblError failed as user sraikar is unauthorized"));
  }
}
