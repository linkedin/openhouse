package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.baseSchema;
import static com.linkedin.openhouse.spark.SparkTestBase.mockTableService;
import static com.linkedin.openhouse.spark.SparkTestBase.spark;

import com.linkedin.openhouse.gen.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class CreateTableWithPropsTest {
  @Test
  public void testCreateTableWithPropsSuccessful() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    GetTableResponseBody mockResponse =
        mockGetTableResponseBody(
            "dbCreate",
            "tbprop",
            "c1",
            "dbCreate.tbprop",
            "UUID",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbCreate", "tbprop")),
            "v1",
            baseSchema,
            null,
            null);

    Map<String, String> tblProps = new HashMap<>();
    tblProps.put("k", "v");
    GetTableResponseBody responseWithProp = decorateResponse(mockResponse, tblProps);
    mockTableService.enqueue(mockResponse(201, responseWithProp)); // doCommit()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                "CREATE TABLE openhouse.dbCreate.tbprop (col1 string, col2 string) USING iceberg TBLPROPERTIES ('k' = 'v')"));
  }

  @Test
  public void testCreateTableWithEncryptionPropSuccessful() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody())); // doRefresh()

    GetTableResponseBody mockResponse =
        mockGetTableResponseBody(
            "dbEncrypt",
            "tbEncrypt",
            "c1",
            "dbEncrypt.tbEncrypt",
            "UUID-Encrypt",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbEncrypt", "tbEncrypt")),
            "v1",
            baseSchema,
            null,
            null);

    // Expect the 'encrypted' property to be passed to the table service
    Map<String, String> tblProps = new HashMap<>();
    tblProps.put("encrypted", "true");
    GetTableResponseBody responseWithEncryption = decorateResponse(mockResponse, tblProps);
    mockTableService.enqueue(mockResponse(201, responseWithEncryption)); // doCommit()

    Assertions.assertDoesNotThrow(
        () ->
            spark.sql(
                "CREATE TABLE openhouse.dbEncrypt.tbEncrypt (id int) USING iceberg TBLPROPERTIES ('encrypted' = 'true')"));

    // Optional: Add assertions here to verify the mocked table service received the correct
    // properties
    // This would require inspecting the RecordedRequest in the mockTableService,
    // similar to how other tests might verify request bodies.
  }
}
