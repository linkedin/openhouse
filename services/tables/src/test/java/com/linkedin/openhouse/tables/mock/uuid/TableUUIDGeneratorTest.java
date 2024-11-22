package com.linkedin.openhouse.tables.mock.uuid;

import static com.linkedin.openhouse.tables.mock.RequestConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TableUUIDGeneratorTest {
  @InjectMocks private TableUUIDGenerator tableUUIDGenerator;
  @Mock private Storage storage;
  @Mock private StorageClient storageClient;
  @Mock private StorageManager storageManager;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    // Mock storage and catalog behavior
    when(storageManager.getStorageFromPath(any())).thenReturn(storage);
    when(storage.getClient()).thenReturn(storageClient);
    when(storageClient.getRootPrefix()).thenReturn("/tmp");
    when(storageClient.fileExists(any())).thenReturn(true);
  }

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
                        getIcebergSnapshot("/tmp", "db", "t", expectedUUID, "/manifest.avro")))
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  @SneakyThrows
  @Test
  public void testUUIDExtractedFromTablePropertySuccessfulPutSnapshot() {
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
                .jsonSnapshots(null)
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  @SneakyThrows
  @Test
  public void testUUIDExtractedFromTablePropertySuccessfulCreateTable() {
    UUID expectedUUID = UUID.randomUUID();
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
                            getIcebergSnapshot("/tmp", "db", "RANDOM", UUID.randomUUID(), "")))
                    .build()));
  }

  @Test
  public void testUUIDFailsForNonExistingOpenhouseDotPropertyPath() {
    when(storage.getClient().fileExists(any())).thenReturn(false);

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
                    .jsonSnapshots(Collections.singletonList(getIcebergSnapshot("/tmp" + "db")))
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
                                getIcebergSnapshot("/tmp" + "/db/t-NOTUUID/maniffest-list")))
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

  private String getTableLocation(
      String rootPrefix, String databaseId, String tableId, UUID tableUUID) {
    return String.format("%s/%s/%s-%s", rootPrefix, databaseId, tableId, tableUUID.toString());
  }

  private String getIcebergSnapshot(
      String rootPrefix, String databaseId, String tableId, UUID tableUUID, String appendedPath) {
    String tableLocation = getTableLocation(rootPrefix, databaseId, tableId, tableUUID);
    String manifestListValue = String.format("%s/%s", tableLocation, appendedPath);
    String key = "manifest-list";
    JsonObject jsonObject = new Gson().fromJson(TEST_ICEBERG_SNAPSHOT_JSON, JsonObject.class);
    jsonObject.addProperty(key, manifestListValue);
    return jsonObject.toString();
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
