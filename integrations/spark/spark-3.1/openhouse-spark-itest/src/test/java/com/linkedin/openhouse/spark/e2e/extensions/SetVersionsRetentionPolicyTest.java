package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class SetVersionsRetentionPolicyTest {
  @Test
  public void testSetVersionsRetentionPolicyTimeAndCount() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t1",
            "c1",
            "dbSetVersionsRetention.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetVersionsRetention", "t1"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t1",
            "c1",
            "dbSetVersionsRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t1')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetversionsRetention =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t1",
            "c1",
            "dbSetVersionsRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"versionsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\",\"count\":10, \"logicalOperator\": "
                    + "\"AND\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetversionsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetversionsRetention.t1 SET POLICY (VERSIONS TIME=24H AND COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetversionsRetentionPolicyTimeOrCount() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t2",
            "c1",
            "dbSetVersionsRetention.t2",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetVersionsRetention", "t2"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t2",
            "c1",
            "dbSetVersionsRetention.t2",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t2')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetversionsRetention =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t1",
            "c1",
            "dbSetVersionsRetention.t2",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"versionsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\",\"count\":10, \"logicalOperator\": "
                    + "\"OR\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetversionsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetversionsRetention.t2 SET POLICY (VERSIONS TIME=24H OR COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetversionsRetentionPolicyTimeOnly() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t3",
            "c1",
            "dbSetVersionsRetention.t3",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetVersionsRetention", "t3"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t3",
            "c1",
            "dbSetVersionsRetention.t3",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t3"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t3')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetversionsRetention =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t3",
            "c1",
            "dbSetVersionsRetention.t3",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t3"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"versionsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetversionsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetversionsRetention.t3 SET POLICY (VERSIONS TIME=24H)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetversionsRetentionPolicyCountOnly() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t4",
            "c1",
            "dbSetVersionsRetention.t4",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetVersionsRetention", "t4"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t4",
            "c1",
            "dbSetVersionsRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t4')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetversionsRetention =
        mockGetTableResponseBody(
            "dbSetVersionsRetention",
            "t4",
            "c1",
            "dbSetVersionsRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetVersionsRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"versionsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetversionsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetversionsRetention.t4 SET POLICY (VERSIONS COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }
}
