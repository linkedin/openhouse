package com.linkedin.openhouse.tables.mock.uuid;

import static com.linkedin.openhouse.tables.mock.RequestConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TableUUIDGeneratorTest {

  @Autowired private StorageManager storageManager;

  @Autowired private TableUUIDGenerator tableUUIDGenerator;

  @Test
  public void testUUIDExtractedFromSnapshotSuccessfulPutSnapshot() {
    UUID expectedUUID = UUID.randomUUID();
    UUID existingUUID =
        tableUUIDGenerator.generateUUID(
            IcebergSnapshotsRequestBody.builder()
                .baseTableVersion("v1")
                .createUpdateTableRequestBody(
                    CreateUpdateTableRequestBody.builder()
                        .tableId("t")
                        .databaseId("db")
                        .clusterId(CLUSTER_NAME)
                        .tableProperties(
                            ImmutableMap.of(
                                "openhouse.tableUUID",
                                expectedUUID.toString(),
                                "openhouse.tableId",
                                "t",
                                "openhouse.databaseId",
                                "db"))
                        .build())
                .jsonSnapshots(
                    Collections.singletonList(
                        getIcebergSnapshot("db", "t", expectedUUID, "/manifest.avro")))
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  @SneakyThrows
  @Test
  public void testUUIDExtractedFromTablePropertySuccessfulPutSnapshot() {
    UUID expectedUUID = UUID.randomUUID();
    FileSystem fsClient =
        (FileSystem) storageManager.getDefaultStorage().getClient().getNativeClient();
    fsClient.create(
        new Path(
            InternalRepositoryUtils.constructTablePath(
                    storageManager, "db", "t", expectedUUID.toString())
                .toString()));
    UUID existingUUID =
        tableUUIDGenerator.generateUUID(
            IcebergSnapshotsRequestBody.builder()
                .baseTableVersion("v1")
                .createUpdateTableRequestBody(
                    CreateUpdateTableRequestBody.builder()
                        .tableId("t")
                        .databaseId("db")
                        .clusterId(CLUSTER_NAME)
                        .tableProperties(
                            ImmutableMap.of(
                                "openhouse.tableUUID",
                                expectedUUID.toString(),
                                "openhouse.tableId",
                                "t",
                                "openhouse.databaseId",
                                "db"))
                        .build())
                .jsonSnapshots(null)
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  @SneakyThrows
  @Test
  public void testUUIDExtractedFromTablePropertySuccessfulCreateTable() {
    UUID expectedUUID = UUID.randomUUID();
    FileSystem fsClient =
        (FileSystem) storageManager.getDefaultStorage().getClient().getNativeClient();
    fsClient.create(
        new Path(
            InternalRepositoryUtils.constructTablePath(
                    storageManager, "db", "t", expectedUUID.toString())
                .toString()));
    UUID existingUUID =
        tableUUIDGenerator.generateUUID(
            CreateUpdateTableRequestBody.builder()
                .tableId("t")
                .databaseId("db")
                .clusterId(CLUSTER_NAME)
                .tableProperties(
                    ImmutableMap.of(
                        "openhouse.tableUUID",
                        expectedUUID.toString(),
                        "openhouse.tableId",
                        "t",
                        "openhouse.databaseId",
                        "db"))
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  @SneakyThrows
  @Test
  public void testFreshUUIDCreated() {
    Assertions.assertDoesNotThrow(
        () ->
            tableUUIDGenerator.generateUUID(
                CreateUpdateTableRequestBody.builder()
                    .tableId("t")
                    .databaseId("db")
                    .clusterId(CLUSTER_NAME)
                    .build()));
  }

  @Test
  public void testUUIDFailsForInvalidSnapshot() {
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () ->
            tableUUIDGenerator.generateUUID(
                IcebergSnapshotsRequestBody.builder()
                    .baseTableVersion("v1")
                    .createUpdateTableRequestBody(
                        CreateUpdateTableRequestBody.builder()
                            .tableId("t")
                            .databaseId("db")
                            .tableProperties(generateMinimalTestProps("db", "t"))
                            .clusterId(CLUSTER_NAME)
                            .build())
                    .jsonSnapshots(
                        Collections.singletonList(
                            getIcebergSnapshot("db", "RANDOM", UUID.randomUUID(), "")))
                    .build()));
  }

  @Test
  public void testUUIDFailsForNonExistingOpenhouseDotPropertyPath() {
    RequestValidationFailureException exception =
        Assertions.assertThrows(
            RequestValidationFailureException.class,
            () ->
                tableUUIDGenerator.generateUUID(
                    CreateUpdateTableRequestBody.builder()
                        .tableId("t")
                        .databaseId("db")
                        .clusterId(CLUSTER_NAME)
                        .tableProperties(
                            ImmutableMap.of(
                                "openhouse.tableUUID",
                                UUID.randomUUID().toString(),
                                "openhouse.tableId",
                                "t",
                                "openhouse.databaseId",
                                "db"))
                        .build()));
    Assertions.assertTrue(exception.getMessage().contains("Provided snapshot is invalid"));
  }

  @Test
  public void testUUIDFailsForInvalidSnapshotShortManifestList() {
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () ->
            tableUUIDGenerator.generateUUID(
                IcebergSnapshotsRequestBody.builder()
                    .baseTableVersion("v1")
                    .createUpdateTableRequestBody(
                        CreateUpdateTableRequestBody.builder()
                            .tableId("t")
                            .databaseId("db")
                            .tableProperties(generateMinimalTestProps("db", "t"))
                            .clusterId(CLUSTER_NAME)
                            .build())
                    .jsonSnapshots(
                        Collections.singletonList(
                            getIcebergSnapshot(
                                storageManager.getDefaultStorage().getClient().getRootPath()
                                    + "/db")))
                    .build()));
  }

  @Test
  public void testUUIDFailsForInvalidSnapshotWithoutUUID() {
    RequestValidationFailureException exception =
        Assertions.assertThrows(
            RequestValidationFailureException.class,
            () ->
                tableUUIDGenerator.generateUUID(
                    IcebergSnapshotsRequestBody.builder()
                        .baseTableVersion("v1")
                        .createUpdateTableRequestBody(
                            CreateUpdateTableRequestBody.builder()
                                .tableId("t")
                                .databaseId("db")
                                .tableProperties(generateMinimalTestProps("db", "t"))
                                .clusterId(CLUSTER_NAME)
                                .build())
                        .jsonSnapshots(
                            Collections.singletonList(
                                getIcebergSnapshot(
                                    storageManager.getDefaultStorage().getClient().getRootPath()
                                        + "/db/t-NOTUUID/maniffest-list")))
                        .build()));
    Assertions.assertTrue(exception.getMessage().contains("contains invalid UUID"));
  }

  @Test
  public void testUUIDFailsForInvalidTableProperty() {
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () ->
            tableUUIDGenerator.generateUUID(
                CreateUpdateTableRequestBody.builder()
                    .tableId("t")
                    .databaseId("db")
                    .clusterId(CLUSTER_NAME)
                    .tableProperties(
                        ImmutableMap.of(
                            "openhouse.tableUUID",
                            "NOTUUID",
                            "openhouse.tableId",
                            "t",
                            "openhouse.databaseId",
                            "db"))
                    .build()));
  }

  @Test
  public void testUUIDFromTablePropertyForReplicaTable() {
    UUID expectedUUID = UUID.randomUUID();
    UUID actualUUID =
        Assertions.assertDoesNotThrow(
            () ->
                tableUUIDGenerator.generateUUID(
                    CreateUpdateTableRequestBody.builder()
                        .tableId("t")
                        .databaseId("db")
                        .clusterId(CLUSTER_NAME)
                        .tableType(TableType.REPLICA_TABLE)
                        .tableProperties(
                            ImmutableMap.of(
                                CatalogConstants.OPENHOUSE_UUID_KEY,
                                expectedUUID.toString(),
                                "openhouse.tableId",
                                "t",
                                "openhouse.databaseId",
                                "db"))
                        .build()));
    Assertions.assertEquals(expectedUUID, actualUUID);
  }

  @Test
  public void testUUIDFailsForMissingIdentifiers() {
    UUID expectedUUID = UUID.randomUUID();

    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () ->
            tableUUIDGenerator.generateUUID(
                CreateUpdateTableRequestBody.builder()
                    .tableId("t")
                    .databaseId("db")
                    .clusterId(CLUSTER_NAME)
                    .tableType(TableType.REPLICA_TABLE)
                    .tableProperties(
                        ImmutableMap.of(
                            CatalogConstants.OPENHOUSE_UUID_KEY, expectedUUID.toString()))
                    .build()));
  }

  private String getIcebergSnapshot(
      String databaseId, String tableId, UUID tableUUID, String appendedPath) {
    return getIcebergSnapshot(
        InternalRepositoryUtils.constructTablePath(
                storageManager, databaseId, tableId, tableUUID.toString())
            + appendedPath);
  }

  private String getIcebergSnapshot(String manifestListValue) {
    String key = "manifest-list";
    JsonObject jsonObject = new Gson().fromJson(TEST_ICEBERG_SNAPSHOT_JSON, JsonObject.class);
    jsonObject.addProperty(key, manifestListValue);
    return jsonObject.toString();
  }

  private Map<String, String> generateMinimalTestProps(String databaseId, String tableId) {
    return new HashMap<String, String>() {
      {
        put("openhouse.databaseId", databaseId);
        put("openhouse.tableId", tableId);
      }
    };
  }
}
