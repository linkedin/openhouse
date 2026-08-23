package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link OpenHouseTableOperations#serializeMetadataUpdates} emits Iceberg REST spec
 * {@code TableUpdate} objects for the operations OpenHouse commits, and in particular that a
 * ref-only operation is distinguishable from a data write.
 *
 * <p>These assertions are the load-bearing premise of the audit path: the server can only report
 * which branch a commit wrote because the client states it here. Items are objects, not JSON
 * strings, so the request field is {@code CommitTableRequest.updates[]}.
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
    Assertions.assertTrue(
        updates.get(0).get("snapshot-id") instanceof Long,
        "integral snapshot-id must stay Long so the wire is 42, not 42.0");
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
    TableMetadata base = tableWithOneSnapshot();
    Snapshot newSnapshot =
        SnapshotParser.fromJson(
            "{\"snapshot-id\":43,"
                + "\"parent-snapshot-id\":42,"
                + "\"timestamp-ms\":1669126937999,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/tmp/snap-43.avro\","
                + "\"schema-id\":0}");
    TableMetadata afterAppend =
        TableMetadata.buildFrom(base).setBranchSnapshot(newSnapshot, "feature_a").build();

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

  /**
   * Metadata read straight off disk carries no changes. The field is omitted entirely rather than
   * reported as an empty list, so consumers see "not stated" rather than "nothing happened".
   */
  @Test
  public void testMetadataWithNoChangesYieldsNull() {
    TableMetadata noChanges =
        TableMetadata.buildFrom(tableWithOneSnapshot()).discardChanges().build();
    Assertions.assertNull(OpenHouseTableOperations.serializeMetadataUpdates(noChanges));
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
    Assertions.assertEquals(snapshotId, update.get("snapshot-id"));
    Assertions.assertTrue(update.get("snapshot-id") instanceof Long);
  }
}
