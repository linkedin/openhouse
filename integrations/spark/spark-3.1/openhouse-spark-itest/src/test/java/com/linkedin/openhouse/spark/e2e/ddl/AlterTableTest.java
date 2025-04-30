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

  @Test
  public void testAlterEncryptedProperty() {
    // Setup: Create an encrypted table
    String dbName = "dbEncryptAlter";
    String tableName = "tbEncryptAlter";
    TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);
    String fqtn = String.format("openhouse.%s.%s", dbName, tableName);

    // Mock responses for table creation
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // exists check
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // exists check
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // exists check

    GetTableResponseBody createResponseMock =
        mockGetTableResponseBody(
            dbName,
            tableName,
            "c1",
            fqtn,
            "UUID-Encrypt-Alter",
            mockTableLocationDefaultSchema(tableIdentifier),
            "v1",
            baseSchema,
            null,
            null);
    Map<String, String> encryptedProps = new HashMap<>();
    encryptedProps.put("encrypted", "true");
    GetTableResponseBody responseWithEncryption =
        decorateResponse(createResponseMock, encryptedProps);
    mockTableService.enqueue(mockResponse(201, responseWithEncryption)); // doCommit for CREATE

    spark.sql(
        String.format(
            "CREATE TABLE %s (id int) USING iceberg TBLPROPERTIES ('encrypted' = 'true')", fqtn));

    // --- Test Case 1: Try to set encrypted to false ---
    mockTableService.enqueue(mockResponse(200, responseWithEncryption)); // doRefresh for ALTER
    mockTableService.enqueue(mockResponse(200, responseWithEncryption)); // loadTable in strategy

    UnsupportedOperationException thrownException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark.sql(
                    String.format("ALTER TABLE %s SET TBLPROPERTIES('encrypted' = 'false')", fqtn)),
            "Expected ALTER TABLE setting encrypted=false to fail");
    Assertions.assertTrue(
        thrownException.getMessage().contains("Cannot modify or unset the 'encrypted' property"));

    // --- Test Case 2: Try to set another property (should succeed) ---
    mockTableService.enqueue(mockResponse(200, responseWithEncryption)); // doRefresh for ALTER
    mockTableService.enqueue(mockResponse(200, responseWithEncryption)); // loadTable in strategy

    // Mock the successful commit response with the new property added
    Map<String, String> propsWithOther = new HashMap<>(encryptedProps);
    propsWithOther.put("other_prop", "other_value");
    GetTableResponseBody responseWithOther = decorateResponse(createResponseMock, propsWithOther);
    mockTableService.enqueue(mockResponse(200, responseWithOther)); // doCommit for ALTER

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                String.format(
                    "ALTER TABLE %s SET TBLPROPERTIES('other_prop' = 'other_value')", fqtn)),
        "Expected ALTER TABLE setting other property to succeed");

    // --- Test Case 3: Try to UNSET the encrypted property ---
    mockTableService.enqueue(
        mockResponse(
            200, responseWithOther)); // doRefresh for UNSET (use state after setting other_prop)
    mockTableService.enqueue(mockResponse(200, responseWithOther)); // loadTable in strategy

    UnsupportedOperationException thrownExceptionUnset =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> spark.sql(String.format("ALTER TABLE %s UNSET TBLPROPERTIES('encrypted')", fqtn)),
            "Expected ALTER TABLE unsetting encrypted property to fail");
    Assertions.assertTrue(
        thrownExceptionUnset
            .getMessage()
            .contains("Cannot modify or unset the 'encrypted' property"));

    // Optional: Verify the final state if necessary by querying table properties or checking mock
    // service requests
  }
}
