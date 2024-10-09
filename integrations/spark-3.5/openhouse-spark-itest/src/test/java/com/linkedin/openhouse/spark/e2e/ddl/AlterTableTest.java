package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.gen.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class AlterTableTest {

  @Test
  public void testSetTableProps() {
    final String key = "key";
    final String value = "value";

    GetTableResponseBody existingTable =
        mockGetTableResponseBody(
            "dbAlter",
            "tb1",
            "c1",
            "dbAlter.tb1.c1",
            "UUID",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbAlter", "tb1")),
            "v1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()

    Map<String, String> tblProps = new HashMap<>();
    tblProps.put(key, value);
    mockTableService.enqueue(
        mockResponse(200, decorateResponse(existingTable, tblProps))); // doCommit()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                String.format(
                    "ALTER TABLE openhouse.dbAlter.tb1 SET TBLPROPERTIES('%s'='%s')", key, value)));
  }

  @Test
  public void testUnsetTableProps() {
    final String key = "key";
    final String value = "value";
    Map<String, String> tblProps = new HashMap<>();
    tblProps.put(key, value);

    GetTableResponseBody existingTable =
        mockGetTableResponseBody(
            "dbUnset",
            "tb1",
            "c1",
            "dbUnset.tb1.c1",
            "UUID",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbUnset", "tb1")),
            "v1",
            baseSchema,
            null,
            null);
    GetTableResponseBody decoratedTable = decorateResponse(existingTable, tblProps);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, decoratedTable)); // doRefresh()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                String.format("ALTER TABLE openhouse.dbUnset.tb1 UNSET TBLPROPERTIES('%s')", key)));
  }
}
