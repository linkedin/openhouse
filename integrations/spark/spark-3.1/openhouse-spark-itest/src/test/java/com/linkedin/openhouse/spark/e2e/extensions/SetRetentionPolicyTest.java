package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class SetRetentionPolicyTest {
  @Test
  public void testSetRetentionPolicy() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t1",
            "c1",
            "dbSetRetention.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetRetention", "t1"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t1",
            "c1",
            "dbSetRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES('openhouse.tableId'='t1')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetRetention =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t1",
            "c1",
            "dbSetRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"retention\":{\"count\":300,\"granularity\":\"DAY\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetRetention)); // doCommit()
    String ddlWithSchema = "ALTER TABLE openhouse.dbSetRetention.t1 SET POLICY (RETENTION=300d)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyIdentifierWithLeadingDigits() {
    Object existingTable =
        mockGetTableResponseBody(
            "0_",
            "0_",
            "c1",
            "0_.0_",
            "u1",
            mockTableLocation(
                TableIdentifier.of("0_", "0_"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "0_",
            "0_",
            "c1",
            "0_.0_",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("0_", "0_"),
                "ALTER TABLE %t SET TBLPROPERTIES('openhouse.tableId'='0_')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetRetention =
        mockGetTableResponseBody(
            "0_",
            "0_",
            "c1",
            "0_.0_",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("0_", "0_"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"retention\":{\"count\":300,\"granularity\":\"DAY\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetRetention)); // doCommit()
    String ddlWithSchema = "ALTER TABLE openhouse.0_.0_ SET POLICY (RETENTION=300d)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyWhiteSpaceAndLowerCase() throws Exception {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t2",
            "c1",
            "dbSetRetention.t2",
            "u2",
            mockTableLocation(
                TableIdentifier.of("dbSetRetention", "t2"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t2",
            "c1",
            "dbSetRetention.t2",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES('openhouse.tableId'='t2')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetRetention =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t2",
            "c1",
            "dbSetRetention.t2",
            "u2",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{retention:{count:300,granularity:day}}','openhouse.tableId'='t2')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetRetention)); // doCommit()
    String ddlWithSchema =
        "\n" + "alter table openhouse.dbSetRetention.t2 set  policy ( retention = 300d )\n";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyNonOpenHouseTable() throws Exception {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t3",
            "c1",
            "dbSetRetention.t3",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetRetention", "t3"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    String ddlWithSchema = "ALTER TABLE openhouse.dbSetRetention.t3 SET POLICY (RETENTION=300d)";
    Assertions.assertThrows(UnsupportedOperationException.class, () -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyQuotedIdentifier() throws Exception {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t4",
            "c1",
            "dbSetRetention.t4",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetRetention", "t4"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t4",
            "c1",
            "dbSetRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES('openhouse.tableId'='t4')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetRetention =
        mockGetTableResponseBody(
            "dbSetRetention",
            "t4",
            "c1",
            "dbSetRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"retention\":{\"count\":300,\"granularity\":\"DAY\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetRetention)); // doCommit()
    String ddlWithSchema = "ALTER TABLE openhouse.`dbSetRetention`.t4 SET POLICY (RETENTION=300d)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyBadDurationWhiteSpace() throws Exception {
    String ddlWithSchema = "ALTER TABLE openhouse.dbSetRetention.t1 SET POLICY (RETENTION=30 d)";
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyBadDurationNonNumeric() throws Exception {
    String ddlWithSchema = "ALTER TABLE openhouse.dbSetRetention.t1 SET POLICY (RETENTION=random)";
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyBadDurationNegative() throws Exception {
    String ddlWithSchema = "ALTER TABLE openhouse.dbSetRetention.t1 SET POLICY (RETENTION=-30d)";
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyBadTableName() throws Exception {
    String ddlWithSchema = "ALTER TABLE openhouse.10y.t1 SET POLICY (RETENTION=30d)";
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetRetentionPolicyUnsupportedPolicy() throws Exception {
    String ddlWithSchema = "ALTER TABLE openhouse.10y.t1 SET POLICY (OPTIMIZATION=30d)";
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql(ddlWithSchema));
  }
}
