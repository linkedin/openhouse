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
  public void testSetSnapshotsRetentionPolicy() {
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
                "ALTER TABLE %t SET TBLPROPERTIES('policies'='{\"snapshotsRetention\":{\"timeCount\":24,\"granularity\":\"HOUR\",\"count\":10}}')"),
            "V2",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingOpenhouseTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(201, tableAfterSetSnapshotsRetention)); // doCommit()
    String ddlWithSchema =
        "ALTER TABLE openhouse.dbSetSnapshotsRetention.t1 SET POLICY (SNAPSHOTS_RETENTION TTL=24H COUNT=10)";
    Assertions.assertDoesNotThrow(() -> spark.sql(ddlWithSchema));
  }
}
