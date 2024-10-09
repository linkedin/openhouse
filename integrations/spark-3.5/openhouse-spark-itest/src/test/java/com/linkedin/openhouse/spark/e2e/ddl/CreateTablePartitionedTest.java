package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class CreateTablePartitionedTest {

  @Test
  public void testSimpleCreateTimePartitionAndClusteredTable() {
    String tbName = "tbpartitionedclustered";
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    mockTableService.enqueue(
        mockResponse(
            201,
            mockGetTableResponseBody(
                "dbCreate",
                tbName,
                "c1",
                "dbCreate.tbpartitionedclustered.c1",
                "UUID",
                mockTableLocation(
                    TableIdentifier.of("dbDesc", tbName),
                    convertSchemaToDDLComponent(baseSchema),
                    "PARTITIONED BY (days(timestampCol), name, id, count)"),
                "v1",
                baseSchema,
                null,
                null))); // doCommit()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                "CREATE TABLE openhouse.dbCreate.$TB_NAME ($SCHEMA) USING ICEBERG PARTITIONED BY (days(timestampCol), name, id, count)"
                    .replace("$TB_NAME", tbName)
                    .replace("$SCHEMA", convertSchemaToDDLComponent(baseSchema))));
  }

  @Test
  public void testCreateTimePartitionAndTransformClusteredTable() {
    String tbName = "tbpartitionedtransformclustered";
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    mockTableService.enqueue(
        mockResponse(
            201,
            mockGetTableResponseBody(
                "dbCreate",
                tbName,
                "c1",
                "dbCreate.tbpartitionedclustered.c1",
                "UUID",
                mockTableLocation(
                    TableIdentifier.of("dbDesc", tbName),
                    convertSchemaToDDLComponent(baseSchema),
                    "PARTITIONED BY (days(timestampCol), truncate(100, name))"),
                "v1",
                baseSchema,
                null,
                null))); // doCommit()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                "CREATE TABLE openhouse.dbCreate.$TB_NAME ($SCHEMA) USING ICEBERG PARTITIONED BY (days(timestampCol), truncate(100, name))"
                    .replace("$TB_NAME", tbName)
                    .replace("$SCHEMA", convertSchemaToDDLComponent(baseSchema))));
  }

  @Test
  public void testCreateTimePartitionedTableSuccessful() {
    for (String transform : ImmutableList.of("days", "months", "hours", "years")) {
      String tbName = "tbpartitioned" + transform;
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

      mockTableService.enqueue(
          mockResponse(
              201,
              mockGetTableResponseBody(
                  "dbCreate",
                  tbName,
                  "c1",
                  "dbCreate.tbpartitioned.c1",
                  "UUID",
                  mockTableLocation(
                      TableIdentifier.of("dbDesc", tbName),
                      convertSchemaToDDLComponent(baseSchema),
                      String.format("PARTITIONED BY (%s(timestampCol))", transform)),
                  "v1",
                  baseSchema,
                  null,
                  null))); // doCommit()

      Assertions.assertDoesNotThrow(
          () ->
              spark.sql(
                  "CREATE TABLE openhouse.dbCreate.$TB_NAME ($SCHEMA) PARTITIONED BY ($TRANSFORM(timestampCol))"
                      .replace("$TB_NAME", tbName)
                      .replace("$SCHEMA", convertSchemaToDDLComponent(baseSchema))
                      .replace("$TRANSFORM", transform)));
    }
  }

  @Test
  public void testCreateStringClusteringTableSuccessful() {
    for (String transform :
        ImmutableList.of(
            "identity(name)", "name", "identity(count)", "count", "truncate(10, timeLong)")) {
      String tbName =
          "tbclustered_" + transform.replace('(', '_').replace(')', '_').replace(", ", "_");
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

      mockTableService.enqueue(
          mockResponse(
              201,
              mockGetTableResponseBody(
                  "dbCreate",
                  tbName,
                  "c1",
                  "dbCreate.tbpartitioned.c1",
                  "UUID",
                  mockTableLocation(
                      TableIdentifier.of("dbDesc", tbName),
                      convertSchemaToDDLComponent(baseSchema),
                      String.format("PARTITIONED BY (%s)", transform)),
                  "v1",
                  baseSchema,
                  null,
                  null))); // doCommit()

      Assertions.assertDoesNotThrow(
          () ->
              spark.sql(
                  "CREATE TABLE openhouse.dbCreate.$TB_NAME ($SCHEMA) PARTITIONED BY ($TRANSFORM)"
                      .replace("$TB_NAME", tbName)
                      .replace("$SCHEMA", convertSchemaToDDLComponent(baseSchema))
                      .replace("$TRANSFORM", transform)));
    }
  }

  @Test
  public void testCreatePartitionedTableUnsupported() {
    for (String transform : ImmutableList.of("bucket(2, name)", "bucket(2, count)")) {
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
      mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  spark.sql(
                      "CREATE TABLE openhouse.dbCreate.tb_bad_partitioned ($SCHEMA) PARTITIONED BY ($TRANSFORM)"
                          .replace("$SCHEMA", convertSchemaToDDLComponent(baseSchema))
                          .replace("$TRANSFORM", transform)));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains(
                  "please provide one of the following transforms (hour,day,month,year), for example: PARTITIONED BY category"));
    }
  }

  @Test
  public void testCreatePartitionedTableMultipleTimePartitioning() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark.sql(
                    "CREATE TABLE openhouse.dbCreate.tb_bad_partitioned (timestampCol1 timestamp, timestampCol2 timestamp) PARTITIONED BY (days(timestampCol1), months(timestampCol2))"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "OpenHouse only supports 1 timestamp-based column partitioning, 2 were provided: timestampCol1_day, timestampCol2_month"));
  }
}
