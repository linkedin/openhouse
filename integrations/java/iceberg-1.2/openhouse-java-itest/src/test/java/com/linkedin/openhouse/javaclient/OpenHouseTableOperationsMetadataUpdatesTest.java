package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies the authoritative Iceberg REST action sequence and lossless JSON numeric tokens sent by
 * the client, including actions that cannot be reconstructed from the final ref state.
 */
public class OpenHouseTableOperationsMetadataUpdatesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final String SNAPSHOT_JSON =
      "{\"snapshot-id\":42,"
          + "\"timestamp-ms\":1669126937912,"
          + "\"summary\":{\"operation\":\"append\"},"
          + "\"manifest-list\":\"/tmp/snap-42.avro\","
          + "\"schema-id\":0}";

  /**
   * A table with one snapshot on main, with the construction history discarded.
   *
   * <p>{@code discardChanges()} matters: {@link TableMetadata#changes()} accumulates across builds
   * within a session, so metadata assembled in-test would otherwise still carry its {@code
   * assign-uuid} / {@code add-schema} / {@code add-spec} creation updates. In production the base
   * comes from {@code doRefresh}, i.e. parsed off disk with no changes attached, so each commit's
   * {@code changes()} is exactly that commit's delta. This reproduces that starting condition.
   */
  private static TableMetadata tableWithOneSnapshot() {
    TableMetadata empty =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "/tmp/tbl",
            Collections.emptyMap());
    Snapshot snapshot = SnapshotParser.fromJson(SNAPSHOT_JSON);
    // setBranchSnapshot adds the snapshot and points the ref at it in one step.
    return TableMetadata.buildFrom(empty)
        .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
        .discardChanges()
        .build();
  }

  private static JsonNode asNode(Map<String, Object> update) {
    return MAPPER.valueToTree(update);
  }

  /**
   * CREATE BRANCH adds a ref at the existing head and commits no snapshot. The resulting table
   * state is ambiguous — main and the new branch point at the same snapshot — but the update list
   * names the branch explicitly and contains no {@code add-snapshot}.
   */
  @Test
  public void testCreateBranchEmitsOnlySetSnapshotRefNamingTheNewBranch() {
    TableMetadata base = tableWithOneSnapshot();
    TableMetadata afterCreateBranch =
        TableMetadata.buildFrom(base)
            .setRef("feature_a", SnapshotRef.branchBuilder(42L).build())
            .build();

    List<Map<String, Object>> updates =
        OpenHouseTableOperations.serializeMetadataUpdates(afterCreateBranch);

    Assertions.assertNotNull(updates);
    Assertions.assertEquals(1, updates.size(), "CREATE BRANCH must not report a snapshot write");
    JsonNode update = asNode(updates.get(0));
    Assertions.assertEquals("set-snapshot-ref", update.get("action").asText());
    Assertions.assertEquals("feature_a", update.get("ref-name").asText());
    Assertions.assertEquals("branch", update.get("type").asText());
    Assertions.assertEquals(42L, update.get("snapshot-id").asLong());
    Assertions.assertTrue(update.get("snapshot-id").isIntegralNumber());
  }

  /** A tag carries {@code type: tag}, so consumers can tell it apart from a branch. */
  @Test
  public void testCreateTagEmitsTagTypedSetSnapshotRef() {
    TableMetadata base = tableWithOneSnapshot();
    TableMetadata afterCreateTag =
        TableMetadata.buildFrom(base)
            .setRef("v1_release", SnapshotRef.tagBuilder(42L).build())
            .build();

    List<Map<String, Object>> updates =
        OpenHouseTableOperations.serializeMetadataUpdates(afterCreateTag);

    Assertions.assertNotNull(updates);
    Assertions.assertEquals(1, updates.size());
    JsonNode update = asNode(updates.get(0));
    Assertions.assertEquals("set-snapshot-ref", update.get("action").asText());
    Assertions.assertEquals("v1_release", update.get("ref-name").asText());
    Assertions.assertEquals("tag", update.get("type").asText());
  }

  /** DROP BRANCH is a removal, never a write. */
  @Test
  public void testDropBranchEmitsRemoveSnapshotRef() {
    TableMetadata withBranch =
        TableMetadata.buildFrom(tableWithOneSnapshot())
            .setRef("feature_a", SnapshotRef.branchBuilder(42L).build())
            .discardChanges()
            .build();
    TableMetadata afterDropBranch =
        TableMetadata.buildFrom(withBranch).removeRef("feature_a").build();

    List<Map<String, Object>> updates =
        OpenHouseTableOperations.serializeMetadataUpdates(afterDropBranch);

    Assertions.assertNotNull(updates);
    Assertions.assertEquals(1, updates.size());
    JsonNode update = asNode(updates.get(0));
    Assertions.assertEquals("remove-snapshot-ref", update.get("action").asText());
    Assertions.assertEquals("feature_a", update.get("ref-name").asText());
  }

  /**
   * An append to a named branch reports both the new snapshot and the ref that moved, so a data
   * write remains distinguishable from the ref-only case above.
   */
  @Test
  public void testAppendToBranchEmitsAddSnapshotAndSetSnapshotRef() {
    // Iceberg 1.5 requires (1) the branch already exists and (2) the new snapshot's
    // sequence-number > last (v1 tables pin every snapshot at 0 and reject the add).
    // discardChanges so the commit under test is the append, not CREATE BRANCH + upgrade.
    TableMetadata withBranch =
        TableMetadata.buildFrom(tableWithOneSnapshot())
            .upgradeFormatVersion(2)
            .setRef("feature_a", SnapshotRef.branchBuilder(42L).build())
            .discardChanges()
            .build();
    Snapshot newSnapshot =
        SnapshotParser.fromJson(
            "{\"snapshot-id\":43,"
                + "\"parent-snapshot-id\":42,"
                + "\"sequence-number\":1,"
                + "\"timestamp-ms\":1669126937999,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/tmp/snap-43.avro\","
                + "\"schema-id\":0}");
    TableMetadata afterAppend =
        TableMetadata.buildFrom(withBranch).setBranchSnapshot(newSnapshot, "feature_a").build();

    List<Map<String, Object>> updates =
        OpenHouseTableOperations.serializeMetadataUpdates(afterAppend);

    Assertions.assertNotNull(updates);
    Assertions.assertEquals(
        2, updates.size(), "append reports exactly the new snapshot and the ref that moved");
    boolean sawAddSnapshot = false;
    boolean sawBranchRef = false;
    for (Map<String, Object> item : updates) {
      JsonNode update = asNode(item);
      String action = update.get("action").asText();
      if ("add-snapshot".equals(action)) {
        sawAddSnapshot = true;
      } else if ("set-snapshot-ref".equals(action)
          && "feature_a".equals(update.get("ref-name").asText())) {
        sawBranchRef = true;
        Assertions.assertEquals("branch", update.get("type").asText());
        Assertions.assertEquals(43L, update.get("snapshot-id").asLong());
      }
    }
    Assertions.assertTrue(sawAddSnapshot, "append must report add-snapshot");
    Assertions.assertTrue(sawBranchRef, "append must report the branch it moved");
  }

  @Test
  public void testMetadataWithNoChangesYieldsEmptyAuthoritativeList() {
    Assertions.assertEquals(
        Collections.emptyList(),
        OpenHouseTableOperations.serializeMetadataUpdates(tableWithOneSnapshot()));
  }

  /**
   * A snapshot id that does not fit in a double mantissa must survive {@code MetadataUpdateParser}
   * → Map so Jackson writes the same integer the spec parser emitted.
   */
  @Test
  public void testTableUpdateObjectPreservesLargeSnapshotId() {
    long snapshotId = 2151407017102313398L;
    Map<String, Object> update =
        OpenHouseTableOperations.tableUpdateObject(
            "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\","
                + "\"snapshot-id\":"
                + snapshotId
                + ",\"type\":\"branch\"}");
    Assertions.assertEquals(snapshotId, asNode(update).get("snapshot-id").longValue());
    Assertions.assertTrue(asNode(update).get("snapshot-id").isIntegralNumber());
  }

  @Test
  public void testNumericDefaultsKeepFloatingPointTokensAndUnboundedIntegers() {
    JsonNode update =
        asNode(
            OpenHouseTableOperations.tableUpdateObject(
                "{\"action\":\"add-schema\",\"schema\":{\"type\":\"struct\",\"fields\":["
                    + "{\"id\":1,\"name\":\"value\",\"type\":\"double\",\"required\":false,"
                    + "\"initial-default\":1.0,\"write-default\":1.0}]},"
                    + "\"large-integer\":9223372036854775808}"));
    JsonNode field = update.get("schema").get("fields").get(0);
    Assertions.assertTrue(field.get("initial-default").isFloatingPointNumber());
    Assertions.assertTrue(field.get("write-default").isFloatingPointNumber());
    Assertions.assertEquals(1.0, field.get("initial-default").doubleValue());
    Assertions.assertEquals(
        new BigInteger("9223372036854775808"), update.get("large-integer").bigIntegerValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUnknownActionRejectsWholeCommitBeforeHttp(boolean stagedCreate) {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.changes())
        .thenReturn(
            Arrays.asList(
                new MetadataUpdate.RemoveSnapshotRef("branch"), mock(MetadataUpdate.class)));
    TableApi tableApi = mock(TableApi.class);
    SnapshotApi snapshotApi = mock(SnapshotApi.class);
    OpenHouseTableOperations operations =
        OpenHouseTableOperations.builder()
            .tableIdentifier(TableIdentifier.of("db", "table"))
            .tableApi(tableApi)
            .snapshotApi(snapshotApi)
            .build();
    if (stagedCreate) {
      TableMetadata staged =
          TableMetadata.newTableMetadata(
              SCHEMA,
              PartitionSpec.unpartitioned(),
              SortOrder.unsorted(),
              "/tmp/staged",
              Collections.emptyMap());
      operations.beginCreate(staged, staged.properties());
    }

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.doCommit(stagedCreate ? null : tableWithOneSnapshot(), metadata));
    verifyNoInteractions(tableApi, snapshotApi);
  }

  @Test
  public void testRepeatedRefChangesRemainOrderedEvenWhenFinalStateIsUnchanged() {
    TableMetadata base = tableWithOneSnapshot();
    TableMetadata changes =
        TableMetadata.buildFrom(base)
            .setRef("branch", SnapshotRef.branchBuilder(42L).build())
            .removeRef("branch")
            .setRef("tag", SnapshotRef.tagBuilder(42L).build())
            .removeRef("tag")
            .build();

    List<Map<String, Object>> updates = OpenHouseTableOperations.serializeMetadataUpdates(changes);
    Assertions.assertEquals(base.refs(), changes.refs());
    Assertions.assertEquals(4, updates.size());
    Assertions.assertEquals("set-snapshot-ref", updates.get(0).get("action"));
    Assertions.assertEquals("branch", updates.get(0).get("ref-name"));
    Assertions.assertEquals("remove-snapshot-ref", updates.get(1).get("action"));
    Assertions.assertEquals("branch", updates.get(1).get("ref-name"));
    Assertions.assertEquals("set-snapshot-ref", updates.get(2).get("action"));
    Assertions.assertEquals("tag", updates.get(2).get("ref-name"));
    Assertions.assertEquals("remove-snapshot-ref", updates.get(3).get("action"));
    Assertions.assertEquals("tag", updates.get(3).get("ref-name"));
  }
}
