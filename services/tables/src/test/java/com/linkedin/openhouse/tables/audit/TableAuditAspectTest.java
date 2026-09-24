package com.linkedin.openhouse.tables.audit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.internal.catalog.model.MetadataUpdateResult;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange;
import com.linkedin.openhouse.internal.catalog.utils.MetadataUpdateCommit;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import io.swagger.v3.core.converter.ModelConverters;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

/** Exercises audit behavior at the publication boundary, not repository transaction execution. */
@ExtendWith(MockitoExtension.class)
class TableAuditAspectTest {
  @InjectMocks private TableAuditAspect aspect;
  @Mock private ClusterProperties clusterProperties;
  @Mock private AuditHandler<TableAuditEvent> tableAuditHandler;
  @Mock private ProceedingJoinPoint point;
  @Captor private ArgumentCaptor<TableAuditEvent> events;

  @Test
  void successfulAuditPreservesOrderedTransitionsAndUsesCommittedMain() throws Throwable {
    Map<String, Object> advanceMain = setRef("main", 200L, "branch");
    advanceMain.put("min-snapshots-to-keep", 3);
    advanceMain.put("max-snapshot-age-ms", 5000L);
    advanceMain.put("max-ref-age-ms", 10000L);
    Map<String, Object> createTag = setRef("release", 200L, "tag");
    createTag.put("max-ref-age-ms", 20000L);
    MetadataUpdateResult result =
        MetadataUpdateCommit.apply(
            baseMetadata(),
            Arrays.asList(
                advanceMain,
                createTag,
                Map.of("action", "set-properties", "updates", Map.of("user.key", "value")),
                setRef("main", 100L, "branch"),
                removeRef("release")));

    // The request fixture claims a different main snapshot and different updates. Only the
    // successful response is authoritative for what was actually committed.
    TableAuditEvent event = auditSuccessfulCommit(result);
    assertEquals(OperationStatus.SUCCESS, event.getOperationStatus());
    assertEquals(100L, event.getCurrentSnapshotId().longValue());
    assertEquals(1000L, event.getCurrentSnapshotTimestampMs().longValue());
    List<SnapshotRefChange> changes = event.getRefChanges();
    assertEquals(4, changes.size());
    assertTransition(changes.get(0), 0, "set-snapshot-ref", "main", 100L, 200L);
    assertEquals("branch", changes.get(0).getAfter().getType());
    assertEquals(3, changes.get(0).getAfter().getMinSnapshotsToKeep().intValue());
    assertEquals(5000L, changes.get(0).getAfter().getMaxSnapshotAgeMs().longValue());
    assertEquals(10000L, changes.get(0).getAfter().getMaxRefAgeMs().longValue());
    assertTransition(changes.get(1), 1, "set-snapshot-ref", "release", null, 200L);
    assertEquals("tag", changes.get(1).getAfter().getType());
    assertEquals(20000L, changes.get(1).getAfter().getMaxRefAgeMs().longValue());
    assertTransition(changes.get(2), 3, "set-snapshot-ref", "main", 200L, 100L);
    assertEquals(changes.get(0).getAfter(), changes.get(2).getBefore());
    assertTransition(changes.get(3), 4, "remove-snapshot-ref", "release", 200L, null);
    assertEquals(changes.get(1).getAfter(), changes.get(3).getBefore());
  }

  @Test
  void committedMainRemovalClearsStaleRequestSnapshotInfo() throws Throwable {
    MetadataUpdateResult result =
        MetadataUpdateCommit.apply(baseMetadata(), Collections.singletonList(removeRef("main")));

    TableAuditEvent event = auditSuccessfulCommit(result);
    assertNull(event.getCurrentSnapshotId());
    assertNull(event.getCurrentSnapshotTimestampMs());
    assertEquals(1, event.getRefChanges().size());
    assertTransition(event.getRefChanges().get(0), 0, "remove-snapshot-ref", "main", 100L, null);
  }

  @Test
  void committedEmptyTransactionDoesNotFallBackToRequestMutations() throws Throwable {
    TableAuditEvent event =
        auditSuccessfulCommit(MetadataUpdateCommit.apply(baseMetadata(), Collections.emptyList()));

    assertEquals(Collections.emptyList(), event.getRefChanges());
    assertEquals(100L, event.getCurrentSnapshotId().longValue());
    assertEquals(1000L, event.getCurrentSnapshotTimestampMs().longValue());
  }

  @Test
  void failedPublicationDoesNotClaimStagedRefTransitions() throws Throwable {
    when(clusterProperties.getClusterName()).thenReturn("test-cluster");
    IllegalStateException failure = new IllegalStateException("publication failed");
    when(point.proceed())
        .thenAnswer(
            invocation -> {
              MetadataUpdateCommit.apply(
                  baseMetadata(), Collections.singletonList(setRef("main", 200L, "branch")));
              throw failure;
            });

    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                aspect.auditPutIcebergSnapshots(
                    point,
                    "db",
                    "table",
                    RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY,
                    "owner")));
    verify(tableAuditHandler).audit(events.capture());
    TableAuditEvent event = events.getValue();
    assertEquals(OperationStatus.FAILED, event.getOperationStatus());
    assertNull(event.getRefChanges());
    assertNull(event.getAuditedTableProperties());
    assertNull(event.getCurrentTableRoot());
  }

  @Test
  void internalCommitResultIsExcludedFromBothJsonSerializersAndOpenApi() throws Exception {
    GetTableResponseBody response =
        GetTableResponseBody.builder()
            .tableId("table")
            .commitResult(
                MetadataUpdateCommit.apply(
                    baseMetadata(), Collections.singletonList(setRef("main", 200L, "branch"))))
            .build();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jackson = mapper.readTree(mapper.writeValueAsString(response));
    JsonNode gson = mapper.readTree(response.toJson());
    assertEquals("table", jackson.get("tableId").asText());
    assertEquals("table", gson.get("tableId").asText());
    assertFalse(jackson.has("commitResult"));
    assertFalse(gson.has("commitResult"));
    Map<String, io.swagger.v3.oas.models.media.Schema> models =
        ModelConverters.getInstance().read(GetTableResponseBody.class);
    Map<String, io.swagger.v3.oas.models.media.Schema> properties =
        models.get("GetTableResponseBody").getProperties();
    assertTrue(properties.containsKey("tableId"));
    assertFalse(properties.containsKey("commitResult"));
  }

  private TableAuditEvent auditSuccessfulCommit(MetadataUpdateResult result) throws Throwable {
    when(clusterProperties.getClusterName()).thenReturn("test-cluster");
    when(point.proceed())
        .thenReturn(
            ApiResponse.<GetTableResponseBody>builder()
                .httpStatus(HttpStatus.OK)
                .responseBody(GetTableResponseBody.builder().commitResult(result).build())
                .build());
    aspect.auditPutIcebergSnapshots(
        point, "db", "table", RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY, "owner");
    verify(tableAuditHandler).audit(events.capture());
    return events.getValue();
  }

  private static void assertTransition(
      SnapshotRefChange change,
      int index,
      String action,
      String name,
      Long beforeSnapshot,
      Long afterSnapshot) {
    assertEquals(index, change.getUpdateIndex());
    assertEquals(action, change.getAction());
    assertEquals(name, change.getRefName());
    if (beforeSnapshot == null) {
      assertNull(change.getBefore());
    } else {
      assertEquals(beforeSnapshot.longValue(), change.getBefore().getSnapshotId());
    }
    if (afterSnapshot == null) {
      assertNull(change.getAfter());
    } else {
      assertEquals(afterSnapshot.longValue(), change.getAfter().getSnapshotId());
    }
  }

  private static TableMetadata baseMetadata() {
    TableMetadata empty =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            "file:///audit-table",
            Collections.singletonMap("format-version", "2"));
    return TableMetadata.buildFrom(empty)
        .setBranchSnapshot(snapshot(100L, 1L, 1000L), "main")
        .addSnapshot(snapshot(200L, 2L, 2000L))
        .build();
  }

  private static Snapshot snapshot(long id, long sequence, long timestamp) {
    return SnapshotParser.fromJson(
        String.format(
            "{\"snapshot-id\":%d,\"sequence-number\":%d,\"timestamp-ms\":%d,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"file:///snapshot-%d.avro\",\"schema-id\":0}",
            id, sequence, timestamp, id));
  }

  private static Map<String, Object> setRef(String name, long snapshotId, String type) {
    Map<String, Object> update = new LinkedHashMap<>();
    update.put("action", "set-snapshot-ref");
    update.put("ref-name", name);
    update.put("snapshot-id", snapshotId);
    update.put("type", type);
    return update;
  }

  private static Map<String, Object> removeRef(String name) {
    return Map.of("action", "remove-snapshot-ref", "ref-name", name);
  }
}
