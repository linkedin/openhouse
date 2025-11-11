package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SnapshotDiffApplier}. Tests the refactored snapshot logic with multi-branch
 * support that extends the base implementation.
 */
public class SnapshotDiffApplierTest {

  private SnapshotDiffApplier snapshotDiffApplier;
  private MetricsReporter mockMetricsReporter;
  private TableMetadata baseMetadata;
  private static final String TEST_TABLE_LOCATION = getTempLocation();

  @SneakyThrows
  private static String getTempLocation() {
    return Files.createTempDirectory(UUID.randomUUID().toString()).toString();
  }

  @BeforeEach
  void setup() {
    mockMetricsReporter = Mockito.mock(MetricsReporter.class);
    snapshotDiffApplier = new SnapshotDiffApplier(mockMetricsReporter);

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    baseMetadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            TEST_TABLE_LOCATION,
            new HashMap<>());
  }

  // ========== Basic Functionality Tests ==========

  /** Verifies that when no snapshot JSON is provided, metadata is returned unmodified. */
  @Test
  void testApplySnapshots_noSnapshotsJson_returnsUnmodified() {
    TableMetadata result = snapshotDiffApplier.applySnapshots(null, baseMetadata);

    assertEquals(baseMetadata, result);
    verifyNoInteractions(mockMetricsReporter);
  }

  /** Verifies that table creation (null base) with branch references is handled correctly. */
  @Test
  void testApplySnapshots_nullBase_handlesTableCreation() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(snapshots.get(snapshots.size() - 1))));

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(null, newMetadata);

    assertNotNull(result);
    assertEquals(snapshots.size(), result.snapshots().size());
  }

  /** Verifies that new snapshots are added correctly to branches. */
  @Test
  void testApplySnapshots_addNewSnapshots_success() throws IOException {
    List<Snapshot> initialSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, initialSnapshots);

    List<Snapshot> allSnapshots = new ArrayList<>(initialSnapshots);
    allSnapshots.addAll(IcebergTestUtil.getExtraSnapshots());

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(allSnapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                allSnapshots.get(allSnapshots.size() - 1))));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertTrue(result.snapshots().size() > baseWithSnapshots.snapshots().size());

    verify(mockMetricsReporter, atLeastOnce()).count(anyString(), anyDouble());
  }

  // ========== Validation Tests ==========

  /** Verifies that deleting the current snapshot without replacements throws an exception. */
  @Test
  void testValidateCurrentSnapshotNotDeleted_whenCurrentDeleted_throwsException()
      throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY,
        SnapshotsUtil.serializedSnapshots(Collections.emptyList()));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(new HashMap<>()));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);

    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(exception.getMessage().contains("Cannot delete the current snapshot"));
  }

  /**
   * Verifies that attempting to reference the same snapshot from multiple branches in a single
   * commit throws an exception. This is a PR2-specific validation for multi-branch support.
   */
  @Test
  void testValidateNoAmbiguousCommits_whenSnapshotReferencedByMultipleBranches_throwsException()
      throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Snapshot targetSnapshot = snapshots.get(0);

    Map<String, String> snapshotRefs = new HashMap<>();
    SnapshotRef ref = SnapshotRef.branchBuilder(targetSnapshot.snapshotId()).build();
    snapshotRefs.put("branch1", org.apache.iceberg.SnapshotRefParser.toJson(ref));
    snapshotRefs.put("branch2", org.apache.iceberg.SnapshotRefParser.toJson(ref));

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(snapshotRefs));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);

    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(exception.getMessage().contains("Ambiguous commit"));
    assertTrue(exception.getMessage().contains("referenced by multiple branches"));
  }

  /**
   * Verifies that attempting to delete a snapshot that is still referenced by a branch or tag
   * throws an exception.
   */
  @Test
  void
      testValidateDeletedSnapshotsNotReferenced_whenDeletedSnapshotStillReferenced_throwsException()
          throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Snapshot snapshotToDelete = snapshots.get(0);
    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());

    Map<String, String> snapshotRefs = new HashMap<>();
    SnapshotRef ref = SnapshotRef.branchBuilder(snapshotToDelete.snapshotId()).build();
    snapshotRefs.put(SnapshotRef.MAIN_BRANCH, org.apache.iceberg.SnapshotRefParser.toJson(ref));

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(remainingSnapshots));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(snapshotRefs));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);

    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(exception.getMessage().contains("Cannot delete snapshots"));
    assertTrue(exception.getMessage().contains("still referenced"));
  }

  // ========== Metrics Tests ==========

  /** Verifies that staged (WAP) snapshots trigger the correct metrics. */
  @Test
  void testApplySnapshots_withWapSnapshots_recordsCorrectMetrics() throws IOException {
    List<Snapshot> baseSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, baseSnapshots);

    List<Snapshot> wapSnapshots = IcebergTestUtil.getWapSnapshots();
    List<Snapshot> allSnapshots = new ArrayList<>(baseSnapshots);
    allSnapshots.addAll(wapSnapshots);

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(allSnapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                baseSnapshots.get(baseSnapshots.size() - 1))));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);

    verify(mockMetricsReporter)
        .count(eq(InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR), anyDouble());
  }

  /** Verifies that deleting snapshots triggers the correct metrics. */
  @Test
  void testApplySnapshots_deleteSnapshots_recordsCorrectMetrics() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(remainingSnapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                remainingSnapshots.get(remainingSnapshots.size() - 1))));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertEquals(remainingSnapshots.size(), result.snapshots().size());

    verify(mockMetricsReporter)
        .count(eq(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR), eq(1.0));
  }

  // ========== Property Management Tests ==========

  /** Verifies that appended snapshot IDs are recorded in properties. */
  @Test
  void testApplySnapshots_recordsSnapshotIdsInProperties() throws IOException {
    List<Snapshot> baseSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, baseSnapshots);

    List<Snapshot> newSnapshotsList = IcebergTestUtil.getExtraSnapshots();
    List<Snapshot> allSnapshots = new ArrayList<>(baseSnapshots);
    allSnapshots.addAll(newSnapshotsList);

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(allSnapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                allSnapshots.get(allSnapshots.size() - 1))));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);

    String appendedSnapshots =
        result.properties().get(getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS));
    assertNotNull(appendedSnapshots, "Appended snapshots should be recorded in properties");

    assertTrue(appendedSnapshots.contains(",") || !appendedSnapshots.isEmpty());
  }

  /** Verifies that temporary snapshot processing keys are removed from final properties. */
  @Test
  void testApplySnapshots_removesSnapshotKeysFromProperties() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();

    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(snapshots.get(snapshots.size() - 1))));

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(null, newMetadata);

    assertNotNull(result);

    assertFalse(
        result.properties().containsKey(CatalogConstants.SNAPSHOTS_JSON_KEY),
        "Snapshots JSON key should be removed from final properties");
    assertFalse(
        result.properties().containsKey(CatalogConstants.SNAPSHOTS_REFS_KEY),
        "Snapshots refs key should be removed from final properties");
  }

  // ========== Branch Update Tests ==========

  /** Verifies that updating branch references works correctly. */
  @Test
  void testApplySnapshots_branchUpdates_appliesCorrectly() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Snapshot newBranchTarget = snapshots.get(1);
    Map<String, String> snapshotRefs =
        IcebergTestUtil.obtainSnapshotRefsFromSnapshot(newBranchTarget);

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(snapshotRefs));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertNotNull(result.currentSnapshot());
    assertEquals(newBranchTarget.snapshotId(), result.currentSnapshot().snapshotId());
  }

  /**
   * Verifies that multiple branch updates can be applied simultaneously. This is a PR2-specific
   * test for multi-branch support.
   */
  @Test
  void testApplySnapshots_multipleBranchUpdates_success() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Map<String, String> snapshotRefs = new HashMap<>();
    SnapshotRef mainRef = SnapshotRef.branchBuilder(snapshots.get(0).snapshotId()).build();
    SnapshotRef devRef = SnapshotRef.branchBuilder(snapshots.get(1).snapshotId()).build();
    snapshotRefs.put(SnapshotRef.MAIN_BRANCH, org.apache.iceberg.SnapshotRefParser.toJson(mainRef));
    snapshotRefs.put("dev", org.apache.iceberg.SnapshotRefParser.toJson(devRef));

    Map<String, String> properties = new HashMap<>(baseWithSnapshots.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(snapshotRefs));

    TableMetadata newMetadata = baseWithSnapshots.replaceProperties(properties);
    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertEquals(2, result.refs().size());
  }

  // ========== Helper Methods ==========

  /**
   * Adds snapshots to metadata and sets main branch to the last snapshot.
   *
   * @param metadata Base metadata
   * @param snapshots Snapshots to add
   * @return Updated metadata
   */
  private TableMetadata addSnapshotsToMetadata(TableMetadata metadata, List<Snapshot> snapshots) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
    for (Snapshot snapshot : snapshots) {
      builder.addSnapshot(snapshot);
    }
    if (!snapshots.isEmpty()) {
      Snapshot lastSnapshot = snapshots.get(snapshots.size() - 1);
      SnapshotRef ref = SnapshotRef.branchBuilder(lastSnapshot.snapshotId()).build();
      builder.setRef(SnapshotRef.MAIN_BRANCH, ref);
    }
    return builder.build();
  }
}
