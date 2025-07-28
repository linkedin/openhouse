package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class DropTableTest {
  @Test
  public void testDropTable() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbDrop",
            "t1",
            "c1",
            "dbDrop.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbDrop", "t1"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(204, null)); // doRefresh()
    mockTableService.enqueue(
        mockResponse(404, mockGetAllTableResponseBody())); // doRefresh() for describe
    mockTableService.enqueue(
        mockResponse(404, mockGetAllTableResponseBody())); // doRefresh() for describe

    String ddl = "DROP TABLE openhouse.dbDrop.t1";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddl));

    AnalysisException ex =
        Assertions.assertThrows(
            AnalysisException.class, () -> spark.sql("DESCRIBE TABLE openhouse.dbDrop.t1").show());

    Assertions.assertEquals(
        "Table or view not found for 'DESCRIBE TABLE': openhouse.dbDrop.t1; line 1 pos 0;\n"
            + "'DescribeRelation false\n"
            + "+- 'UnresolvedTableOrView [openhouse, dbDrop, t1], DESCRIBE TABLE, true\n",
        ex.getMessage());
  }

  @Test
  public void testDropTableNotExist() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    String ddl = "DROP TABLE openhouse.dbDrop.t1";
    AnalysisException ex =
        Assertions.assertThrows(AnalysisException.class, () -> spark.sql(ddl).show());

    Assertions.assertEquals(
        "Table or view not found for 'DROP TABLE': openhouse.dbDrop.t1; line 1 pos 0;\n"
            + "'DropTable false, false\n"
            + "+- 'UnresolvedTableOrView [openhouse, dbDrop, t1], DROP TABLE, true\n",
        ex.getMessage());
  }

  @Test
  public void testDropTableCheckExist() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    String ddl = "DROP TABLE IF EXISTS openhouse.dbDrop.t1";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddl));
  }

  @Test
  public void test503Error() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbDrop",
            "t2",
            "c1",
            "dbDrop.t2",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbDrop", "t2"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(
        mockResponse(200, existingTable)); // doRefresh() initially returns the table
    mockTableService.enqueue(
        mockResponse(
            503,
            "{\"status\":\"SERVICE_UNAVAILABLE\",\"error\":\"Service Unavailable\",\"message\":\"Drop table failed as service is unavailable\"}"));
    WebClientResponseWithMessageException exception =
        Assertions.assertThrows(
            WebClientResponseWithMessageException.class,
            () -> spark.sql("DROP TABLE openhouse.dbDrop.t2"));
    Assertions.assertTrue(
        exception.getMessage().contains("\"Drop table failed as service is unavailable"));
    Assertions.assertEquals(exception.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE.value());
  }

  @Test
  public void testConcurrentDropError() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbDrop",
            "t3",
            "c1",
            "dbDrop.t3",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbDrop", "t3"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(
        mockResponse(200, existingTable)); // doRefresh() initially returns the table
    mockTableService.enqueue(
        mockResponse(404, null)); // returns 404 as concurrent deletion has happened

    String ddl = "DROP TABLE IF EXISTS openhouse.dbDrop.t3";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddl));
  }
}
