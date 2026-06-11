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
      // Java-regex allowlist entries. The four backslashes in the Java source survive javac +
      // Properties.load() unescaping as a single backslash in the bound value, i.e. a literal dot.
      //   [0] openhouse\..*    every key beginning with "openhouse."
      //   [1] replication\..*  a second valid pattern, to exercise OR-across-patterns
      //   [2] [unclosed(       syntactically invalid: must be logged-and-skipped, never blocking
      //   [3] exact\.key       an exact (non-.*) pattern, to exercise full-match anchoring
      "cluster.iceberg.tables.audit.table-properties-allowlist[0]=openhouse\\\\..*",
      "cluster.iceberg.tables.audit.table-properties-allowlist[1]=replication\\\\..*",
      "cluster.iceberg.tables.audit.table-properties-allowlist[2]=[unclosed(",
      "cluster.iceberg.tables.audit.table-properties-allowlist[3]=exact\\\\.key",
      // Small caps (bytes) so the size-limit tests stay readable; production defaults are
      // 256KB/512KB.
      "cluster.iceberg.tables.audit.table-property-value-max-size=256B",
      "cluster.iceberg.tables.audit.table-properties-total-max-size=512B"
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
  public void testPutIcebergSnapshotsBranchOnlyCommitLeavesSnapshotInfoNull() throws Exception {
    // Simulate a branch-only commit where main is absent from snapshotRefs.
    // In this case the main branch ref doesn't exist, so currentSnapshotId /
    // currentSnapshotTimestampMs should be null.
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
  }

  @Test
  public void testPutIcebergSnapshotsFiltersTablePropertiesByRegexAllowlist() throws Exception {
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.watermark", "100");
    requestProperties.put("openhouse.tableType", "PRIMARY_TABLE");
    requestProperties.put("openhouse.replication.config", "{\"target\":\"war\"}");
    requestProperties.put("user.custom.key", "v");
    // No dot after "openhouse", so the literal-dot regex (openhouse\..*) rejects it.
    requestProperties.put("openhousewatermark", "should-not-match");
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    Map<String, String> expected = new HashMap<>();
    expected.put("openhouse.watermark", "100");
    expected.put("openhouse.tableType", "PRIMARY_TABLE");
    expected.put("openhouse.replication.config", "{\"target\":\"war\"}");
    assertEquals(expected, actualEvent.getAuditedTableProperties());
  }

  @Test
  public void testPutIcebergSnapshotsSkipsPropertyExceedingPerValueCap() throws Exception {
    // Per-value cap is 256B (class-level). A 300-byte value is skipped; the small one survives.
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.watermark", "100");
    requestProperties.put("openhouse.a", "x".repeat(300));
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    assertEquals(
        Collections.singletonMap("openhouse.watermark", "100"),
        actualEvent.getAuditedTableProperties());
  }

  @Test
  public void testPutIcebergSnapshotsSkipsPropertiesExceedingTotalCap() throws Exception {
    // All three keys match the allowlist regex and pass the 256B per-value cap, but the 512B total
    // cap admits only the first two. Source keys are visited in sorted order (openhouse.a,
    // openhouse.b, openhouse.c), so 200 + 200 = 400 <= 512; adding the third (600) exceeds, and
    // openhouse.c is skipped.
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.a", "x".repeat(200));
    requestProperties.put("openhouse.b", "y".repeat(200));
    requestProperties.put("openhouse.c", "z".repeat(200));
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    Map<String, String> emitted = actualEvent.getAuditedTableProperties();
    assertEquals(2, emitted.size());
    assertEquals("x".repeat(200), emitted.get("openhouse.a"));
    assertEquals("y".repeat(200), emitted.get("openhouse.b"));
    assertNull(emitted.get("openhouse.c"));
  }

  @Test
  public void testPutIcebergSnapshotsNoMatchingPropertiesEmitsNullNotEmptyMap() throws Exception {
    // Source is non-empty but nothing matches the allowlist regex, so filterTableProperties must
    // return null (not an empty map) — downstream handlers skip the field on null.
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("user.custom.key", "v");
    requestProperties.put("foo", "bar");
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    assertNull(actualEvent.getAuditedTableProperties());
  }

  @Test
  public void testInvalidRegexIsSkippedAndValidPatternsMatchAcrossOr() throws Exception {
    // The invalid pattern [2] ([unclosed() must be logged-and-skipped at compile time without
    // aborting audit emission, and a key matching only the second valid pattern [1] must still be
    // emitted (OR semantics).
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.watermark", "100"); // matches pattern [0]
    requestProperties.put("replication.target", "war"); // matches only pattern [1]
    requestProperties.put("user.custom.key", "v"); // matches nothing
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    Map<String, String> expected = new HashMap<>();
    expected.put("openhouse.watermark", "100");
    expected.put("replication.target", "war");
    assertEquals(expected, actualEvent.getAuditedTableProperties());
  }

  @Test
  public void testExactPatternFullyAnchorsKey() throws Exception {
    // Pattern [3] (exact\.key) has no .*, so Pattern.matches must anchor the whole key: a longer or
    // prefixed key must not match.
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("exact.key", "1"); // matches pattern [3] exactly
    requestProperties.put("exact.key.suffix", "2"); // longer -> rejected by the end anchor
    requestProperties.put("prefix.exact.key", "3"); // prefixed -> rejected by the start anchor
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    assertEquals(
        Collections.singletonMap("exact.key", "1"), actualEvent.getAuditedTableProperties());
  }

  private TableAuditEvent putSnapshotsAndCapture(Map<String, String> tableProperties)
      throws Exception {
    IcebergSnapshotsRequestBody base = RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY;
    IcebergSnapshotsRequestBody requestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.getBaseTableVersion())
            .jsonSnapshots(base.getJsonSnapshots())
            .snapshotRefs(base.getSnapshotRefs())
            .createUpdateTableRequestBody(
                base.getCreateUpdateTableRequestBody()
                    .toBuilder()
                    .tableProperties(tableProperties)
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
    return argCaptor.getValue();
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

/**
 * Verifies the privacy-safe default: with no {@code table-properties-allowlist} configured (the
 * field defaults to {@link java.util.Collections#emptyList()}), nothing is emitted regardless of
 * the committed properties. A separate top-level class (not a method on {@link
 * IcebergSnapshotsApiHandlerAuditTest}) because it must bind an empty allowlist, which requires its
 * own Spring context — the enclosing class fixes a non-empty allowlist for all of its tests.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration
@WithMockUser(username = "testUser")
class IcebergSnapshotsApiHandlerAuditEmptyAllowlistTest {
  @Autowired private MockMvc mvc;

  @MockBean private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Captor private ArgumentCaptor<TableAuditEvent> argCaptor;

  @Test
  public void testEmptyAllowlistEmitsNoTableProperties() throws Exception {
    Map<String, String> requestProperties = new HashMap<>();
    requestProperties.put("openhouse.watermark", "100");
    requestProperties.put("foo", "bar");
    TableAuditEvent actualEvent = putSnapshotsAndCapture(requestProperties);
    assertNull(actualEvent.getAuditedTableProperties());
  }

  private TableAuditEvent putSnapshotsAndCapture(Map<String, String> tableProperties)
      throws Exception {
    IcebergSnapshotsRequestBody base = RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY;
    IcebergSnapshotsRequestBody requestBody =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(base.getBaseTableVersion())
            .jsonSnapshots(base.getJsonSnapshots())
            .snapshotRefs(base.getSnapshotRefs())
            .createUpdateTableRequestBody(
                base.getCreateUpdateTableRequestBody()
                    .toBuilder()
                    .tableProperties(tableProperties)
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
    return argCaptor.getValue();
  }
}
