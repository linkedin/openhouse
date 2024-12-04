package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class SetSnapshotsRetentionPolicyTest {
  @Test
  public void testSetSnapshotsRetentionPolicyTimeAndCount() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t1",
            "c1",
            "dbSetSnapshotsRetention.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t1"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t1",
            "c1",
            "dbSetSnapshotsRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t1')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetSnapshotsRetention =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t1",
            "c1",
            "dbSetSnapshotsRetention.t1",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t1"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"snapshotsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\",\"count\":10 }}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetSnapshotsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetSnapshotsRetention.t1 SET POLICY (SNAPSHOT_RETENTION TIME=24H COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetSnapshotsRetentionPolicyTimeOrCount() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t2",
            "c1",
            "dbSetSnapshotsRetention.t2",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t2"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t2",
            "c1",
            "dbSetSnapshotsRetention.t2",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t2')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetSnapshotsRetention =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t1",
            "c1",
            "dbSetSnapshotsRetention.t2",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t2"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"snapshotsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\",\"count\":10 }}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetSnapshotsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetSnapshotsRetention.t2 SET POLICY (VERSIONS TIME=24H COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetSnapshotsRetentionPolicyTimeOnly() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t3",
            "c1",
            "dbSetSnapshotsRetention.t3",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t3"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t3",
            "c1",
            "dbSetSnapshotsRetention.t3",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t3"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t3')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetSnapshotsRetention =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t3",
            "c1",
            "dbSetSnapshotsRetention.t3",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t3"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"snapshotsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetSnapshotsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetSnapshotsRetention.t3 SET POLICY (VERSIONS TIME=24H)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }

  @Test
  public void testSetSnapshotsRetentionPolicyCountOnly() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t4",
            "c1",
            "dbSetSnapshotsRetention.t4",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t4"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);
    Object existingOpenhouseTable =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t4",
            "c1",
            "dbSetSnapshotsRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES ('openhouse.tableId'='t4')"),
            "V1",
            baseSchema,
            null,
            null);
    Object tableAfterSetSnapshotsRetention =
        mockGetTableResponseBody(
            "dbSetSnapshotsRetention",
            "t4",
            "c1",
            "dbSetSnapshotsRetention.t4",
            "u1",
            mockTableLocationAfterOperation(
                TableIdentifier.of("dbSetSnapshotsRetention", "t4"),
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"snapshotsRetention\":{\"timeCount\":24,\"granularity\":"
                    + "\"HOUR\"}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetSnapshotsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetSnapshotsRetention.t4 SET POLICY (VERSIONS COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }
}
