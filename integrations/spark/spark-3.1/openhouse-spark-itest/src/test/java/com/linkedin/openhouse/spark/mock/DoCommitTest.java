package com.linkedin.openhouse.spark.mock;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.OpenHouseTableOperations;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import okhttp3.mockwebserver.MockResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SparkTestBase.class)
public class DoCommitTest {

  private OpenHouseTableOperations ops;

  private TableMetadata base;

  private TableMetadata baseWithReplicaTableWithoutReplicaProp;

  private TableMetadata baseWithReplicaTable;

  private TableMetadata baseWithActiveReplicaTable;

  private TableMetadata baseWithPrimaryTable;

  private TableMetadata baseWithSchemaChange;

  private TableMetadata baseWithPropsChange;

  private TableMetadata baseWithPartitionChange;

  private TableMetadata baseWithInvalidPartitionChange;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    ops =
        OpenHouseTableOperations.builder()
            .tableApi(getTableApiClient())
            .snapshotApi(getSnapshotApiClient())
            .fileIO(new HadoopFileIO(new Configuration()))
            .tableIdentifier(TableIdentifier.of("db", "tbl"))
            .build();
    base =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of());

    baseWithReplicaTableWithoutReplicaProp =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of(
                "openhouse.tableType",
                "REPLICA_TABLE",
                "openhouse.tableId",
                "testTable",
                "openhouse.clusterId",
                "replica_cluster"));

    baseWithReplicaTable =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of(
                "openhouse.tableType",
                "REPLICA_TABLE",
                "openhouse.tableId",
                "testTable",
                "openhouse.clusterId",
                "replica_cluster",
                "openhouse.isTableReplicated",
                "true"));

    baseWithActiveReplicaTable =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of(
                "openhouse.tableType",
                "PRIMARY_TABLE",
                "openhouse.tableId",
                "testTable",
                "openhouse.clusterId",
                "replica_cluster",
                "openhouse.isTableReplicated",
                "true"));

    baseWithPrimaryTable =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of(
                "openhouse.tableType",
                "PRIMARY_TABLE",
                "openhouse.tableId",
                "testTable",
                "openhouse.clusterId",
                "primary_cluster"));

    baseWithSchemaChange =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(
                    2, "timestampCol", Types.TimestampType.withZone())), /*change*/
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of());

    baseWithPropsChange =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of("k", "v"));

    baseWithPartitionChange =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone())),
            PartitionSpec.builderFor(
                    new Schema(
                        Types.NestedField.required(1, "stringId", Types.StringType.get()),
                        Types.NestedField.required(
                            2, "timestampCol", Types.TimestampType.withoutZone())))
                .truncate("stringId", 10)
                .build(),
            UUID.randomUUID().toString(),
            ImmutableMap.of());

    baseWithInvalidPartitionChange =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "stringId", Types.StringType.get()),
                Types.NestedField.required(2, "timestampCol", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "doubleCol", Types.DoubleType.get())),
            PartitionSpec.builderFor(
                    new Schema(
                        Types.NestedField.required(1, "stringId", Types.StringType.get()),
                        Types.NestedField.required(
                            2, "timestampCol", Types.TimestampType.withoutZone()),
                        Types.NestedField.required(3, "doubleCol", Types.DoubleType.get())))
                .identity("doubleCol") // Double type is not supported for partitioning
                .build(),
            UUID.randomUUID().toString(),
            ImmutableMap.of());
  }

  @Test
  public void testCommitStateUnknownException() {
    mockTableService.enqueue(mockResponse(504, "{\"message\":\"State Unknown\"}"));
    Assertions.assertThrows(CommitStateUnknownException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommitFailedException() {
    mockTableService.enqueue(mockResponse(409, "{\"message\":\"Concurrent Update\"}"));
    Assertions.assertThrows(CommitFailedException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testSurfaceRestExceptions() {
    mockTableService.enqueue(mockResponse(500, "{\"message\":\"Internal Server Error\"}"));
    Assertions.assertThrows(CommitStateUnknownException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testBadRequestExceptions() {
    mockTableService.enqueue(mockResponse(400, "{\"message\":\"Invalid Arguments\"}"));
    Assertions.assertThrows(BadRequestException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testSuccessfulCreate() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "db1",
                "tbl2",
                "",
                "",
                "",
                mockTableLocationDefaultSchema(TableIdentifier.of("db1", "tbl2")),
                "",
                baseSchema,
                null,
                null)));
    Assertions.assertDoesNotThrow(() -> ops.doCommit(null, base));
  }

  /** Assumption of this test: Whenever metadata change is detected will a request to made. */
  @Test
  public void testMetadataChange() throws Exception {
    // Only partition-spec change: Triggers a commit but since this is a invalid partition spec
    // (unsupported double type), it
    // throws exception. If metadata change is not detected, exception won't be thrown.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ops.doCommit(base, baseWithInvalidPartitionChange));

    // A random response should be good enough as the payload is not used.
    MockResponse validResponse =
        mockResponse(
            200,
            mockGetTableResponseBody(
                "db_mc",
                "tbl_mc",
                "",
                "",
                "",
                mockTableLocationDefaultSchema(TableIdentifier.of("db_mc", "tbl_mc")),
                "",
                baseSchema,
                null,
                null));
    mockTableService.enqueue(validResponse);
    ops.doCommit(base, baseWithSchemaChange);
    // If schema change is not detected, the takeRequest method will block.
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());

    mockTableService.enqueue(validResponse);
    ops.doCommit(base, baseWithPropsChange);
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());

    mockTableService.enqueue(validResponse);
    ops.doCommit(base, baseWithPartitionChange);
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());
  }

  @Test
  public void testMetadataWithDataChange() throws Exception {
    FileAttribute<Set<PosixFilePermission>> attr =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));
    Path tempFile1 = Files.createFile(tempDir.resolve("data1.parquet"), attr);

    // Craft a dummy snapshot to indicate data change.
    // Value of each pass-in parameter doesn't matter as the failure occur before those value makes
    // sense.
    mockTableLocationDefaultSchema(TableIdentifier.of("dbN", "tblN"));
    Snapshot dummySnapshot =
        mockDummySnapshot(
            TableIdentifier.of("dbN", "tblN"),
            tempFile1.toString(),
            PartitionSpec.unpartitioned(),
            "openhouse");
    TableMetadata dataPlusMetaDataChangeOffBase =
        TableMetadata.buildFrom(baseWithSchemaChange).addSnapshot(dummySnapshot).build();

    // A dummy response for snapshot API to return properly.
    mockTableService.enqueue(mockResponse(200, mockGetAllTableResponseBody()));

    Assertions.assertDoesNotThrow(() -> ops.doCommit(base, dataPlusMetaDataChangeOffBase));
  }

  /** Assumption of this test: Whenever metadata change is detected will a request to made. */
  @Test
  public void testReplicationRequestWithTblPropertiesDiff() throws Exception {

    // A random response should be good enough as the payload is not used.
    MockResponse validResponse =
        mockResponse(
            200,
            mockGetTableResponseBody(
                "db_mc",
                "tbl_mc",
                "replica_cluster",
                "",
                "",
                mockTableLocationDefaultSchema(TableIdentifier.of("db_mc", "tbl_mc")),
                "",
                baseSchema,
                null,
                null));
    mockTableService.enqueue(validResponse);
    mockTableService.enqueue(validResponse);
    ops.doCommit(baseWithReplicaTable, baseWithPrimaryTable);
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());

    // If tableType property differs, request is considered originating from replication flow
    ops.doCommit(baseWithReplicaTableWithoutReplicaProp, baseWithPrimaryTable);
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());
  }

  /**
   * To support active-active replica table copies, ensure that different table properties are
   * ignored
   */
  @Test
  public void testReplicationRequestWithTblPropertiesDiffOnActiveReplica() throws Exception {

    // A random response should be good enough as the payload is not used.
    MockResponse validResponse =
        mockResponse(
            200,
            mockGetTableResponseBody(
                "db_mc",
                "tbl_mc",
                "replica_cluster",
                "",
                "",
                mockTableLocationDefaultSchema(TableIdentifier.of("db_mc", "tbl_mc")),
                "",
                baseSchema,
                null,
                null));
    mockTableService.enqueue(validResponse);

    ops.doCommit(baseWithActiveReplicaTable, baseWithPrimaryTable);
    // If tableType property differs, request is considered originating from replication flow
    Assertions.assertTimeout(Duration.ofSeconds(1), () -> mockTableService.takeRequest());
  }
}
