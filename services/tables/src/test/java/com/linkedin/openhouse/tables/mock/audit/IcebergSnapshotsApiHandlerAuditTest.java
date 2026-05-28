package com.linkedin.openhouse.tables.mock.audit;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.*;
import static com.linkedin.openhouse.tables.model.TableAuditModelConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration
@TestPropertySource(
    properties = {
      "cluster.iceberg.tables.audit.table-properties-allowlist[0]=openhouse.watermark",
      "cluster.iceberg.tables.audit.table-properties-allowlist[1]=openhouse.tableType"
    })
@WithMockUser(username = "testUser")
public class IcebergSnapshotsApiHandlerAuditTest {
  @Autowired private MockMvc mvc;

  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptor;

  @Test
  public void testPutIcebergSnapshotsSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testPutIcebergSnapshotsFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d400/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testPutIcebergSnapshotsContainsSnapshotInfo() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertEquals(2151407017102313398L, actualEvent.getCurrentSnapshotId().longValue());
    assertEquals(1669126937912L, actualEvent.getCurrentSnapshotTimestampMs().longValue());
  }

  @Test
  public void testPutIcebergSnapshotsFailedPathStillHasSnapshotInfo() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d400/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    // Snapshot info is extracted from request body before execution, so it's present even on
    // failure
    assertEquals(2151407017102313398L, actualEvent.getCurrentSnapshotId().longValue());
    assertEquals(1669126937912L, actualEvent.getCurrentSnapshotTimestampMs().longValue());
  }

  @Test
  public void testPutIcebergSnapshotsMainCommitSetsBranchRefNameToMain() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    assertEquals("main", argCaptor.getValue().getBranchRefName());
  }

  @Test
  public void testPutIcebergSnapshotsNamedBranchCommitSetsBranchRefName() throws Exception {
    // Realistic named-branch commit: main ref exists but its snapshot is NOT in jsonSnapshots
    // (main didn't advance). Only the feature branch got a new snapshot.
    String newSnapshotJson =
        "{\n"
            + "  \"snapshot-id\" : 999,\n"
            + "  \"timestamp-ms\" : 5000,\n"
            + "  \"summary\" : {\"operation\": \"append\"},\n"
            + "  \"manifest-list\" : \"/tmp/feature.avro\",\n"
            + "  \"schema-id\" : 0\n"
            + "}";
    Map<String, String> refs = new HashMap<>();
    refs.put("main", "{\"snapshot-id\":100,\"type\":\"branch\"}"); // main stayed at old snapshot
    refs.put("feature", "{\"snapshot-id\":999,\"type\":\"branch\"}"); // feature got new snapshot

    IcebergSnapshotsRequestBody requestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion("v1")
            .jsonSnapshots(Collections.singletonList(newSnapshotJson))
            .snapshotRefs(refs)
            .createUpdateTableRequestBody(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY)
            .build();

    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(requestBody.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertEquals("feature", actualEvent.getBranchRefName());
    // main didn't advance, so currentSnapshotId is main's old snapshot and timestamp is null
    assertEquals(100L, actualEvent.getCurrentSnapshotId().longValue());
    assertNull(actualEvent.getCurrentSnapshotTimestampMs());
  }

  @Test
  public void testPutIcebergSnapshotsBranchOnlyCommitLeavesSnapshotInfoNull() throws Exception {
    // Simulate a branch-only commit where main is absent from snapshotRefs entirely.
    // currentSnapshotId / currentSnapshotTimestampMs are null (no main), but branchRefName
    // is still populated from the ref that received the new snapshot.
    IcebergSnapshotsRequestBody branchOnlyRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion("v1")
            .jsonSnapshots(Collections.singletonList(RequestConstants.TEST_ICEBERG_SNAPSHOT_JSON))
            .snapshotRefs(
                Collections.singletonMap(
                    "my_branch", "{\"snapshot-id\":2151407017102313398,\"type\":\"branch\"}"))
            .createUpdateTableRequestBody(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY)
            .build();

    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(branchOnlyRequestBody.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertEquals("my_branch", actualEvent.getBranchRefName());
    assertNull(actualEvent.getCurrentSnapshotId());
    assertNull(actualEvent.getCurrentSnapshotTimestampMs());
  }

  @Test
  public void testPutIcebergSnapshotsMainPointsToOlderSnapshot() throws Exception {
    // Simulate a branch-write where jsonSnapshots has 2 snapshots but main still points to the
    // older one.
    // Verifies we pick the main snapshot, not the last snapshot in the list.
    String olderSnapshotJson =
        "{\n"
            + "  \"snapshot-id\" : 100,\n"
            + "  \"timestamp-ms\" : 1000,\n"
            + "  \"summary\" : {\"operation\": \"append\"},\n"
            + "  \"manifest-list\" : \"/tmp/old.avro\",\n"
            + "  \"schema-id\" : 0\n"
            + "}";
    String newerSnapshotJson =
        "{\n"
            + "  \"snapshot-id\" : 200,\n"
            + "  \"parent-snapshot-id\" : 100,\n"
            + "  \"timestamp-ms\" : 2000,\n"
            + "  \"summary\" : {\"operation\": \"append\"},\n"
            + "  \"manifest-list\" : \"/tmp/new.avro\",\n"
            + "  \"schema-id\" : 0\n"
            + "}";
    Map<String, String> refs = new HashMap<>();
    refs.put("main", "{\"snapshot-id\":100,\"type\":\"branch\"}"); // main stayed at older snapshot
    refs.put("feature", "{\"snapshot-id\":200,\"type\":\"branch\"}"); // branch has newer snapshot

    IcebergSnapshotsRequestBody branchWriteRequestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion("v1")
            .jsonSnapshots(Arrays.asList(olderSnapshotJson, newerSnapshotJson))
            .snapshotRefs(refs)
            .createUpdateTableRequestBody(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY)
            .build();

    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(branchWriteRequestBody.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertEquals(100L, actualEvent.getCurrentSnapshotId().longValue());
    assertEquals(1000L, actualEvent.getCurrentSnapshotTimestampMs().longValue());
    // The last snapshot (200) belongs to feature — that is the committed branch.
    assertEquals("feature", actualEvent.getBranchRefName());
  }

  @Test
  public void testPutIcebergSnapshotsFiltersTablePropertiesToAllowlist() throws Exception {
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.watermark", "100");
    requestProperties.put("openhouse.tableType", "PRIMARY_TABLE");
    requestProperties.put("user.custom.key", "v");
    IcebergSnapshotsRequestBody base = RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY;
    IcebergSnapshotsRequestBody requestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.getBaseTableVersion())
            .jsonSnapshots(base.getJsonSnapshots())
            .snapshotRefs(base.getSnapshotRefs())
            .createUpdateTableRequestBody(
                base.getCreateUpdateTableRequestBody()
                    .toBuilder()
                    .tableProperties(requestProperties)
                    .build())
            .build();
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(requestBody.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    Map<String, String> expected = new HashMap<>();
    expected.put("openhouse.watermark", "100");
    expected.put("openhouse.tableType", "PRIMARY_TABLE");
    assertEquals(expected, actualEvent.getTableProperties());
  }

  @Test
  public void testCTASCommitPhase() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d200/tables/tb1/iceberg/v2/snapshots"))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                RequestConstants.TEST_ICEBERG_SNAPSHOTS_INITIAL_VERSION_REQUEST_BODY.toJson()));
    Mockito.verify(tableAuditHandler, atLeastOnce()).audit(argCaptor.capture());
    TableAuditEvent actualEvent = argCaptor.getValue();
    assertTrue(
        new ReflectionEquals(TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_CTAS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }
}
