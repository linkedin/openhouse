package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class InsertOverwriteTableTest {
  @BeforeAll
  static void setup() {}

  @Test
  public void testInsertOverwriteWithoutPartition() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t1",
            "c1",
            "dbInsertOverwrite.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbInsertOverwrite", "t1"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterInsert =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t1",
            "c1",
            "dbInsertOverwrite.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsertOverwrite", "t1"),
                "INSERT INTO %t VALUES ('1', 'a', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
                    + "('2', 'b', CAST('2022-02-02' AS TIMESTAMP), 1.0, 10, 102)"),
            "V2",
            baseSchema,
            null,
            null);
    Object tableAfterInsertOverwrite =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t1",
            "c1",
            "dbInsertOverwrite.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsertOverwrite", "t1"),
                "INSERT OVERWRITE %t VALUES ('1', 'c', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
                    + "('3', 'a', CAST('2022-03-03' AS TIMESTAMP), 1.0, 10, 102)"),
            "V3",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterInsertOverwrite)); // doCommit()
    mockTableService.enqueue(mockResponse(200, tableAfterInsertOverwrite)); // doRefresh()
    mockTableService.enqueue(
        mockResponse(200, tableAfterInsertOverwrite)); // doRefresh() for select

    String ddlWithSchema =
        "INSERT OVERWRITE openhouse.dbInsertOverwrite.t1 "
            + "VALUES ('1', 'c', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
            + "('3', 'a', CAST('2022-03-03' AS TIMESTAMP), 1.0, 10, 102)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));

    List<String> actualRows =
        spark.sql("SELECT id, name, CAST(timestampCol AS DATE) FROM openhouse.dbInsertOverwrite.t1")
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        actualRows.size() == 2
            && actualRows.containsAll(ImmutableList.of("1.c.2022-01-01", "3.a.2022-03-03")));
  }

  @Test
  public void testInsertOverwriteWithPartition() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t2",
            "c1",
            "dbInsertOverwrite.t2",
            "u2",
            mockTableLocation(
                TableIdentifier.of("dbInsertOverwrite", "t2"),
                convertSchemaToDDLComponent(baseSchema),
                "PARTITIONED BY (days(timestampCol))"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterInsert =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t2",
            "c1",
            "dbInsertOverwrite.t2",
            "u2",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsertOverwrite", "t2"),
                "INSERT INTO %t VALUES ('1', 'a', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
                    + "('2', 'b', CAST('2022-02-02' AS TIMESTAMP), 1.0, 10, 102)"),
            "V2",
            baseSchema,
            null,
            null);
    Object tableAfterInsertOverwrite =
        mockGetTableResponseBody(
            "dbInsertOverwrite",
            "t2",
            "c1",
            "dbInsertOverwrite.t2",
            "u2",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbInsertOverwrite", "t2"),
                "INSERT OVERWRITE %t VALUES ('1', 'c', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
                    + "('3', 'a', CAST('2022-03-03' AS TIMESTAMP), 1.0, 10, 102)"),
            "V3",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterInsertOverwrite)); // doCommit()
    mockTableService.enqueue(mockResponse(200, tableAfterInsertOverwrite)); // doRefresh()
    mockTableService.enqueue(
        mockResponse(200, tableAfterInsertOverwrite)); // doRefresh() for select

    String ddlWithSchema =
        "INSERT OVERWRITE openhouse.dbInsertOverwrite.t2 "
            + "VALUES ('1', 'c', CAST('2022-01-01' AS TIMESTAMP), 1.0, 20, 101), "
            + "('3', 'a', CAST('2022-03-03' AS TIMESTAMP), 1.0, 10, 102)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));

    List<String> actualRows =
        spark.sql("SELECT id, name, CAST(timestampCol AS DATE) FROM openhouse.dbInsertOverwrite.t2")
            .collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        actualRows.size() == 3
            && actualRows.containsAll(
                ImmutableList.of("1.c.2022-01-01", "2.b.2022-02-02", "3.a.2022-03-03")));
  }
}
