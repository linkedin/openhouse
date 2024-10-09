package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.baseSchema;
import static com.linkedin.openhouse.spark.SparkTestBase.mockTableService;
import static com.linkedin.openhouse.spark.SparkTestBase.spark;

import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class CreateTableTest {
  @Test
  public void testCreateTable() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    mockTableService.enqueue(
        mockResponse(
            201,
            mockGetTableResponseBody(
                "dbCreate",
                "tb1",
                "c1",
                "dbCreate.tb1.c1",
                "UUID",
                mockTableLocationDefaultSchema(TableIdentifier.of("dbCreate", "tb1")),
                "v1",
                baseSchema,
                null,
                null))); // doCommit()

    String ddlWithSchema =
        "CREATE TABLE openhouse.dbCreate.tb1 (" + convertSchemaToDDLComponent(baseSchema) + ")";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
    // TODO: When we are out of mock, we should verify the created schema as well.
  }

  @Test
  public void testCreateTableAlreadyExists() {
    mockTableService.enqueue(
        mockResponse(
            201,
            mockGetTableResponseBody(
                "dbCreate",
                "tbExists",
                "c1",
                "dbCreate.tbExists.c1",
                "UUID",
                mockTableLocationDefaultSchema(TableIdentifier.of("dbCreate", "tbExists")),
                "v1",
                baseSchema,
                null,
                null))); // doRefresh()

    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () -> spark.sql("CREATE TABLE openhouse.dbCreate.tbExists (col1 string, col2 string)"));
  }

  @Test
  public void testAuthZFailure() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(
        mockResponse(
            403,
            "{\"status\":\"FORBIDDEN\",\"error\":\"forbidden\",\"message\":\"Operation on database dbCreate failed as user sraikar is unauthorized\"}"));
    WebClientResponseException exception =
        Assertions.assertThrows(
            WebClientResponseException.class,
            () -> spark.sql("CREATE TABLE openhouse.dbCreate.tbError (col1 string, col2 string)"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Operation on database dbCreate failed as user sraikar is unauthorized"));
  }
}
