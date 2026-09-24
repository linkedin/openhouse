package com.linkedin.openhouse.internal.catalog.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.internal.catalog.model.MetadataUpdateResult;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange.RefState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class MetadataUpdateCommitTest {
  // These IDs cannot all be represented exactly as doubles.
  private static final long FIRST = 9_007_199_254_740_993L;
  private static final long SECOND = FIRST + 1;
  private static final long THIRD = FIRST + 2;
  private static final long FOURTH = FIRST + 3;
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  void preservesMultipleBranchAppendsAndSameRefRoundTrips() throws IOException {
    TableMetadata base = base();
    MetadataUpdateResult result =
        MetadataUpdateCommit.apply(
            base,
            updates(
                new MetadataUpdate.AddSnapshot(snapshot(THIRD, SECOND, 3)),
                setRef("main", THIRD, "branch"),
                new MetadataUpdate.AddSnapshot(snapshot(FOURTH, SECOND, 4)),
                setRef("dev", FOURTH, "branch"),
                setRef("main", SECOND, "branch"),
                setRef("main", THIRD, "branch")));

    assertEquals(THIRD, result.getMetadata().currentSnapshot().snapshotId());
    assertEquals(FOURTH, result.getMetadata().refs().get("dev").snapshotId());
    assertEquals(
        Arrays.asList(
            change(1, "set-snapshot-ref", "main", branch(SECOND), branch(THIRD)),
            change(3, "set-snapshot-ref", "dev", branch(SECOND), branch(FOURTH)),
            change(4, "set-snapshot-ref", "main", branch(THIRD), branch(SECOND)),
            change(5, "set-snapshot-ref", "main", branch(SECOND), branch(THIRD))),
        result.getRefChanges());
    assertEquals(SECOND, base.currentSnapshot().snapshotId());
    assertNull(base.snapshot(THIRD));
  }

  @Test
  void capturesRetentionChangesAndExplicitAndImplicitRefRemoval() throws IOException {
    RefState retainedBranch =
        RefState.builder()
            .snapshotId(SECOND)
            .type("branch")
            .minSnapshotsToKeep(3)
            .maxSnapshotAgeMs(1000L)
            .maxRefAgeMs(2000L)
            .build();
    RefState tag = RefState.builder().snapshotId(SECOND).type("tag").maxRefAgeMs(5000L).build();
    MetadataUpdateResult result =
        MetadataUpdateCommit.apply(
            base(),
            updates(
                new MetadataUpdate.SetSnapshotRef(
                    "dev",
                    SECOND,
                    SnapshotRef.branchBuilder(SECOND).build().type(),
                    3,
                    1000L,
                    2000L),
                new MetadataUpdate.SetSnapshotRef(
                    "release",
                    SECOND,
                    SnapshotRef.tagBuilder(SECOND).build().type(),
                    null,
                    null,
                    5000L),
                new MetadataUpdate.RemoveSnapshotRef("release"),
                setRef("zeta", SECOND, "branch"),
                setRef("alpha", SECOND, "tag"),
                new MetadataUpdate.RemoveSnapshot(SECOND)));

    assertEquals(
        Arrays.asList(
            change(0, "set-snapshot-ref", "dev", branch(SECOND), retainedBranch),
            change(1, "set-snapshot-ref", "release", null, tag),
            change(2, "remove-snapshot-ref", "release", tag, null),
            change(3, "set-snapshot-ref", "zeta", null, branch(SECOND)),
            change(4, "set-snapshot-ref", "alpha", null, state(SECOND, "tag")),
            change(5, "remove-snapshots", "alpha", state(SECOND, "tag"), null),
            change(5, "remove-snapshots", "dev", retainedBranch, null),
            change(5, "remove-snapshots", "main", branch(SECOND), null),
            change(5, "remove-snapshots", "zeta", branch(SECOND), null)),
        result.getRefChanges());
    assertEquals(Collections.emptyMap(), result.getMetadata().refs());
    assertNull(result.getMetadata().currentSnapshot());
    assertNull(result.getMetadata().snapshot(SECOND));
    assertEquals(FIRST, result.getMetadata().snapshot(FIRST).snapshotId());
  }

  @Test
  void appliesCreationAndLastAddedSentinelsWithoutIntermediateBuilds() throws IOException {
    MetadataUpdateResult result = MetadataUpdateCommit.apply(null, creationUpdates(1));

    assertEquals(2, result.getMetadata().formatVersion());
    assertEquals(SCHEMA.asStruct(), result.getMetadata().schema().asStruct());

    assertTrue(result.getMetadata().spec().isUnpartitioned());
    assertTrue(result.getMetadata().sortOrder().isUnsorted());
    assertEquals(FIRST, result.getMetadata().currentSnapshot().snapshotId());
    assertEquals(
        Collections.singletonList(change(8, "set-snapshot-ref", "main", null, branch(FIRST))),
        result.getRefChanges());
  }

  @Test
  void createsExplicitFormatsWithOrderedSnapshotsAndLastAddedSentinels() throws IOException {
    for (int formatVersion : new int[] {1, 2}) {
      long firstSequence = formatVersion == 1 ? 0 : 1;
      long secondSequence = formatVersion == 1 ? 0 : 2;
      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
      SortOrder order = SortOrder.builderFor(SCHEMA).asc("id").build();
      MetadataUpdateResult result =
          MetadataUpdateCommit.apply(
              null,
              updates(
                  new MetadataUpdate.UpgradeFormatVersion(formatVersion),
                  new MetadataUpdate.AddSchema(SCHEMA, 1),
                  new MetadataUpdate.SetCurrentSchema(-1),
                  new MetadataUpdate.AddPartitionSpec(spec),
                  new MetadataUpdate.SetDefaultPartitionSpec(-1),
                  new MetadataUpdate.AddSortOrder(order),
                  new MetadataUpdate.SetDefaultSortOrder(-1),
                  new MetadataUpdate.SetLocation("file:/warehouse/table"),
                  new MetadataUpdate.AddSnapshot(snapshot(FIRST, null, firstSequence)),
                  setRef("main", FIRST, "branch"),
                  new MetadataUpdate.AddSnapshot(snapshot(SECOND, FIRST, secondSequence)),
                  setRef("main", SECOND, "branch")));

      TableMetadata created = result.getMetadata();
      assertEquals(formatVersion, created.formatVersion());
      assertEquals(SCHEMA.asStruct(), created.schema().asStruct());
      assertEquals(spec.fields(), created.spec().fields());
      assertEquals(order.fields(), created.sortOrder().fields());
      assertEquals(firstSequence, created.snapshot(FIRST).sequenceNumber());
      assertNull(created.snapshot(FIRST).parentId());
      assertEquals(secondSequence, created.snapshot(SECOND).sequenceNumber());
      assertEquals(Long.valueOf(FIRST), created.snapshot(SECOND).parentId());
      assertEquals(secondSequence, created.lastSequenceNumber());
      assertEquals(SECOND, created.currentSnapshot().snapshotId());
    }
  }

  @Test
  void initializesFromFirstExplicitFormatBeforeApplyingEarlierActions() throws IOException {
    List<Map<String, Object>> batch = creationUpdates(0);
    batch.addAll(
        updates(
            new MetadataUpdate.AddSnapshot(snapshot(SECOND, FIRST, 0)),
            new MetadataUpdate.UpgradeFormatVersion(1),
            setRef("main", SECOND, "branch"),
            new MetadataUpdate.UpgradeFormatVersion(2)));

    TableMetadata created = MetadataUpdateCommit.apply(null, batch).getMetadata();
    assertEquals(2, created.formatVersion());
    assertEquals(0, created.snapshot(SECOND).sequenceNumber());
    assertEquals(Long.valueOf(FIRST), created.currentSnapshot().parentId());

    batch.addAll(updates(new MetadataUpdate.UpgradeFormatVersion(1)));
    assertThrows(BadRequestException.class, () -> MetadataUpdateCommit.apply(null, batch));
  }

  @Test
  void rejectsUnsupportedInitialFormats() throws IOException {
    for (int formatVersion : new int[] {-1, 0, 3}) {
      List<Map<String, Object>> batch = creationUpdates(0);
      batch.addAll(0, updates(new MetadataUpdate.UpgradeFormatVersion(formatVersion)));
      assertThrows(BadRequestException.class, () -> MetadataUpdateCommit.apply(null, batch));
    }
  }

  @Test
  void rejectsExistingTableDowngradesWithoutChangingBase() throws IOException {
    TableMetadata base = base();
    List<Map<String, Object>> batch =
        updates(
            new MetadataUpdate.SetProperties(Collections.singletonMap("written", "yes")),
            new MetadataUpdate.UpgradeFormatVersion(1),
            new MetadataUpdate.RemoveSnapshotRef("main"));

    assertThrows(BadRequestException.class, () -> MetadataUpdateCommit.apply(base, batch));
    assertEquals(2, base.formatVersion());
    assertFalse(base.properties().containsKey("written"));
    assertEquals(SECOND, base.currentSnapshot().snapshotId());
  }

  @Test
  void transformsAddedSchemaBeforeSnapshotsReferenceIt() throws IOException {
    Schema source = new Schema(Types.NestedField.required(1, "ID", Types.LongType.get()));
    List<Map<String, Object>> batch =
        updates(
            new MetadataUpdate.AddSchema(source, 9),
            new MetadataUpdate.SetCurrentSchema(-1),
            new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()),
            new MetadataUpdate.SetDefaultPartitionSpec(-1),
            new MetadataUpdate.AddSortOrder(SortOrder.unsorted()),
            new MetadataUpdate.SetDefaultSortOrder(-1),
            new MetadataUpdate.SetLocation("file:/warehouse/table"),
            new MetadataUpdate.AddSnapshot(snapshot(FIRST, null, 1)),
            setRef("main", FIRST, "branch"));

    TableMetadata created =
        MetadataUpdateCommit.apply(
                null, batch, schema -> new Schema(schema.schemaId(), SCHEMA.columns()))
            .getMetadata();

    assertEquals("id", created.schema().findField(1).name());
    assertEquals(
        "id", created.schemasById().get(created.currentSnapshot().schemaId()).findField(1).name());
    assertEquals(1, created.schemas().size());
    assertEquals(9, created.lastColumnId());
    assertEquals(
        "ID", MetadataUpdateCommit.apply(null, batch).getMetadata().schema().findField(1).name());
  }

  @Test
  void rejectsSchemaTransformationFailuresWithoutChangingBase() throws IOException {
    TableMetadata base = base();
    List<Map<String, Object>> batch =
        updates(
            new MetadataUpdate.SetProperties(Collections.singletonMap("written", "yes")),
            new MetadataUpdate.AddSchema(SCHEMA, 1),
            new MetadataUpdate.RemoveSnapshotRef("main"));

    assertThrows(
        BadRequestException.class,
        () ->
            MetadataUpdateCommit.apply(
                base,
                batch,
                schema -> {
                  throw new IllegalArgumentException("Rejected schema");
                }));
    assertFalse(base.properties().containsKey("written"));
    assertEquals(SECOND, base.currentSnapshot().snapshotId());
  }

  @Test
  void rejectsInvalidMiddleActionsWithoutChangingBase() throws IOException {
    List<Map<String, Object>> invalidActions =
        Arrays.asList(
            json("{\"action\":\"unknown-action\"}"),
            null,
            json("{\"ref-name\":\"main\"}"),
            json("{\"action\":\"set-current-view-version\",\"view-version-id\":1}"),
            json("{\"action\":\"set-current-schema\",\"schema-id\":0.5}"),
            json(
                "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\","
                    + "\"type\":\"branch\",\"snapshot-id\":-1}"),
            json(
                "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\","
                    + "\"type\":\"branch\",\"snapshot-id\":9007199254740994.0}"),
            json(
                "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"release\","
                    + "\"type\":\"tag\",\"snapshot-id\":9007199254740994,"
                    + "\"min-snapshots-to-keep\":3}"));
    TableMetadata base = base();
    for (Map<String, Object> invalid : invalidActions) {
      List<Map<String, Object>> batch =
          updates(
              new MetadataUpdate.SetProperties(Collections.singletonMap("written", "yes")),
              new MetadataUpdate.RemoveSnapshotRef("main"));
      batch.add(1, invalid);
      assertThrows(BadRequestException.class, () -> MetadataUpdateCommit.apply(base, batch));
      assertFalse(base.properties().containsKey("written"));
      assertEquals(SECOND, base.currentSnapshot().snapshotId());
      assertEquals(SECOND, base.refs().get("dev").snapshotId());
    }
  }

  @Test
  void keepsAnExplicitEmptyBatchAndNativeNoOpsEmpty() throws IOException {
    TableMetadata base = base();
    MetadataUpdateResult empty = MetadataUpdateCommit.apply(base, Collections.emptyList());
    assertEquals(base.refs(), empty.getMetadata().refs());
    assertEquals(Collections.emptyList(), empty.getRefChanges());

    MetadataUpdateResult unchanged =
        MetadataUpdateCommit.apply(
            base,
            updates(
                setRef("main", SECOND, "branch"), new MetadataUpdate.RemoveSnapshotRef("absent")));
    assertEquals(base.refs(), unchanged.getMetadata().refs());
    assertEquals(Collections.emptyList(), unchanged.getRefChanges());
    assertThrows(BadRequestException.class, () -> MetadataUpdateCommit.apply(base, null));
    assertThrows(
        BadRequestException.class, () -> MetadataUpdateCommit.apply(null, Collections.emptyList()));
  }

  private static List<Map<String, Object>> creationUpdates(long sequence) throws IOException {
    return updates(
        new MetadataUpdate.AddSchema(SCHEMA, 1),
        new MetadataUpdate.SetCurrentSchema(-1),
        new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()),
        new MetadataUpdate.SetDefaultPartitionSpec(-1),
        new MetadataUpdate.AddSortOrder(SortOrder.unsorted()),
        new MetadataUpdate.SetDefaultSortOrder(-1),
        new MetadataUpdate.SetLocation("file:/warehouse/table"),
        new MetadataUpdate.AddSnapshot(snapshot(FIRST, null, sequence)),
        setRef("main", FIRST, "branch"));
  }

  private static TableMetadata base() {
    TableMetadata empty =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), "file:/warehouse/table", Collections.emptyMap());
    return TableMetadata.buildFrom(empty)
        .addSnapshot(snapshot(FIRST, null, 1))
        .addSnapshot(snapshot(SECOND, FIRST, 2))
        .setRef("main", SnapshotRef.branchBuilder(SECOND).build())
        .setRef("dev", SnapshotRef.branchBuilder(SECOND).build())
        .discardChanges()
        .build();
  }

  private static Snapshot snapshot(long id, Long parent, long sequence) {
    Map<String, Object> fields = new HashMap<>();
    fields.put("snapshot-id", id);
    fields.put("sequence-number", sequence);
    fields.put("timestamp-ms", 1_700_000_000_000L + sequence);
    fields.put("schema-id", 0);
    fields.put("summary", Collections.singletonMap("operation", "append"));
    fields.put("manifest-list", "file:/warehouse/table/metadata/snap-" + id + ".avro");
    if (parent != null) {
      fields.put("parent-snapshot-id", parent);
    }
    return SnapshotParser.fromJson(new ObjectMapper().valueToTree(fields).toString());
  }

  private static MetadataUpdate.SetSnapshotRef setRef(String name, long id, String type) {
    SnapshotRef ref =
        "branch".equals(type)
            ? SnapshotRef.branchBuilder(id).build()
            : SnapshotRef.tagBuilder(id).build();
    return new MetadataUpdate.SetSnapshotRef(name, id, ref.type(), null, null, null);
  }

  private static List<Map<String, Object>> updates(MetadataUpdate... updates) throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();
    for (MetadataUpdate update : updates) {
      result.add(json(MetadataUpdateParser.toJson(update)));
    }
    return result;
  }

  private static Map<String, Object> json(String json) throws IOException {
    return new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
  }

  private static RefState branch(long snapshotId) {
    return state(snapshotId, "branch");
  }

  private static RefState state(long snapshotId, String type) {
    return RefState.builder().snapshotId(snapshotId).type(type).build();
  }

  private static SnapshotRefChange change(
      int index, String action, String name, RefState before, RefState after) {
    return SnapshotRefChange.builder()
        .updateIndex(index)
        .action(action)
        .refName(name)
        .before(before)
        .after(after)
        .build();
  }
}
