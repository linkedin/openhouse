package com.linkedin.openhouse.tables.mock.uuid;

import static com.linkedin.openhouse.tables.mock.RequestConstants.*;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Collections;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TableUUIDGeneratorMultiStorageTest {
  @Mock private OpenHouseInternalCatalog catalog;
  @InjectMocks private TableUUIDGenerator tableUUIDGenerator;
  @Mock private Storage defaultStorage;
  @Mock private StorageClient defaultStorageClient;
  @Mock private Storage resolvedStorage;
  @Mock private StorageClient resolvedStorageClient;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSuccessfullSpanshotPutMultiStorage() {
    // Stub behavior for default storage
    when(defaultStorage.getClient()).thenReturn(defaultStorageClient);
    when(defaultStorageClient.getEndpoint()).thenReturn("");
    when(defaultStorageClient.getRootPrefix()).thenReturn("/tmp");

    // Stub behavior for resolved storage
    when(resolvedStorage.getClient()).thenReturn(resolvedStorageClient);
    when(resolvedStorageClient.getEndpoint()).thenReturn("s3://");
    when(resolvedStorageClient.getRootPrefix()).thenReturn("bucket");
    // storage selector returns resolved storage
    when(catalog.resolveStorage(any())).thenReturn(resolvedStorage);

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
                        getIcebergSnapshot(
                            "s3://", "bucket", "db", "t", expectedUUID, "/manifest.avro")))
                .build());
    Assertions.assertEquals(expectedUUID, existingUUID);
  }

  private String getTableLocation(
      String endPoint, String rootPrefix, String databaseId, String tableId, UUID tableUUID) {
    String tableLocationWithoutEndpoint =
        String.format("%s/%s/%s-%s", rootPrefix, databaseId, tableId, tableUUID.toString());
    if (StringUtils.isNotEmpty(endPoint)) {
      return String.format("%s/%s", endPoint, tableLocationWithoutEndpoint);
    }

    return tableLocationWithoutEndpoint;
  }

  private String getIcebergSnapshot(
      String endpoint,
      String rootPrefix,
      String databaseId,
      String tableId,
      UUID tableUUID,
      String appendedPath) {
    String tableLocation = getTableLocation(endpoint, rootPrefix, databaseId, tableId, tableUUID);
    String manifestListValue = String.format("%s/%s", tableLocation, appendedPath);
    String key = "manifest-list";
    JsonObject jsonObject = new Gson().fromJson(TEST_ICEBERG_SNAPSHOT_JSON, JsonObject.class);
    jsonObject.addProperty(key, manifestListValue);
    return jsonObject.toString();
  }
}
