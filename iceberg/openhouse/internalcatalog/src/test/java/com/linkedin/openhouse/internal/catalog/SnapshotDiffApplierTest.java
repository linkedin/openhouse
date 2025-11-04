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
 * Unit tests for {@link SnapshotDiffApplier}. Tests the refactored snapshot logic that was
 * extracted from OpenHouseInternalTableOperations.
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

  // ========== Helper Methods ==========

  /**
   * Creates metadata with snapshots and refs properties for testing.
   *
   * @param base Base metadata to start from
   * @param snapshots Snapshots to include
   * @param refs Snapshot refs to include (nullable)
   * @return Metadata with properties set
   */
  private TableMetadata createMetadataWithSnapshots(
      TableMetadata base, List<Snapshot> snapshots, Map<String, String> refs) {
    Map<String, String> properties = new HashMap<>(base.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    if (refs != null) {
      properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(refs));
    }
    return base.replaceProperties(properties);
  }

  /**
   * Creates metadata with snapshots pointing to the last snapshot as main branch.
   *
   * @param base Base metadata to start from
   * @param snapshots Snapshots to include
   * @return Metadata with snapshots and main branch ref
   */
  private TableMetadata createMetadataWithSnapshotsAndMainRef(
      TableMetadata base, List<Snapshot> snapshots) {
    Map<String, String> refs =
        IcebergTestUtil.obtainSnapshotRefsFromSnapshot(snapshots.get(snapshots.size() - 1));
    return createMetadataWithSnapshots(base, snapshots, refs);
  }

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

  // ========== Edge Case Tests ==========

  /** Verifies that when no snapshot JSON is provided, metadata is returned unmodified. */
  @Test
  void testApplySnapshots_noSnapshotsJson_returnsUnmodified() {
    TableMetadata result = snapshotDiffApplier.applySnapshots(null, baseMetadata);

    assertEquals(baseMetadata, result);
    verifyNoInteractions(mockMetricsReporter);
  }

  /** Verifies that table creation (null base) is handled correctly. */
  @Test
  void testApplySnapshots_nullBase_handlesTableCreation() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata newMetadata = createMetadataWithSnapshotsAndMainRef(baseMetadata, snapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(null, newMetadata);

    assertNotNull(result);
    assertEquals(snapshots.size(), result.snapshots().size());
  }

  // ========== Basic Functionality Tests ==========

  /** Verifies that new snapshots are added correctly. */
  @Test
  void testApplySnapshots_addNewSnapshots_success() throws IOException {
    List<Snapshot> initialSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, initialSnapshots);

    List<Snapshot> allSnapshots = new ArrayList<>(initialSnapshots);
    allSnapshots.addAll(IcebergTestUtil.getExtraSnapshots());
    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, allSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertTrue(result.snapshots().size() > baseWithSnapshots.snapshots().size());
    verify(mockMetricsReporter, atLeastOnce()).count(anyString(), anyDouble());
  }

  /** Verifies that deleting snapshots works correctly and updates main branch. */
  @Test
  void testApplySnapshots_deleteSnapshots_success() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());
    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, remainingSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertEquals(remainingSnapshots.size(), result.snapshots().size());
  }

  /** Verifies that updating branch references works correctly. */
  @Test
  void testApplySnapshots_branchUpdates_success() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    Snapshot newBranchTarget = snapshots.get(1);
    Map<String, String> refs = IcebergTestUtil.obtainSnapshotRefsFromSnapshot(newBranchTarget);
    TableMetadata newMetadata = createMetadataWithSnapshots(baseWithSnapshots, snapshots, refs);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertNotNull(result.currentSnapshot());
    assertEquals(newBranchTarget.snapshotId(), result.currentSnapshot().snapshotId());
  }

  // ========== Validation Tests ==========

  /** Verifies that deleting the current snapshot without replacements throws an exception. */
  @Test
  void testValidation_deletingCurrentSnapshotWithoutReplacement_throwsException()
      throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    TableMetadata newMetadata =
        createMetadataWithSnapshots(baseWithSnapshots, Collections.emptyList(), new HashMap<>());

    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(exception.getMessage().contains("Cannot delete the current snapshot"));
  }
  // ========== Metrics Tests ==========

  /** Verifies that WAP (staged) snapshots trigger the correct metrics. */
  @Test
  void testMetrics_wapSnapshots_recordsStagedCounter() throws IOException {
    List<Snapshot> baseSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, baseSnapshots);

    List<Snapshot> wapSnapshots = IcebergTestUtil.getWapSnapshots();
    List<Snapshot> allSnapshots = new ArrayList<>(baseSnapshots);
    allSnapshots.addAll(wapSnapshots);

    Map<String, String> refs =
        IcebergTestUtil.obtainSnapshotRefsFromSnapshot(baseSnapshots.get(baseSnapshots.size() - 1));
    TableMetadata newMetadata = createMetadataWithSnapshots(baseWithSnapshots, allSnapshots, refs);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    verify(mockMetricsReporter)
        .count(eq(InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR), anyDouble());
  }

  /** Verifies that deleting snapshots triggers the correct metrics. */
  @Test
  void testMetrics_deleteSnapshots_recordsDeletedCounter() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());
    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, remainingSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertEquals(remainingSnapshots.size(), result.snapshots().size());
    verify(mockMetricsReporter)
        .count(eq(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR), eq(1.0));
  }

  // ========== Property Management Tests ==========

  /** Verifies that appended snapshot IDs are recorded in properties. */
  @Test
  void testProperties_appendedSnapshots_recordedCorrectly() throws IOException {
    List<Snapshot> baseSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, baseSnapshots);

    List<Snapshot> newSnapshotsList = IcebergTestUtil.getExtraSnapshots();
    List<Snapshot> allSnapshots = new ArrayList<>(baseSnapshots);
    allSnapshots.addAll(newSnapshotsList);
    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, allSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    String appendedSnapshots =
        result.properties().get(getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS));
    assertNotNull(appendedSnapshots, "Appended snapshots should be recorded in properties");

    // Verify actual snapshot IDs are present
    for (Snapshot newSnapshot : newSnapshotsList) {
      assertTrue(
          appendedSnapshots.contains(String.valueOf(newSnapshot.snapshotId())),
          "Snapshot ID " + newSnapshot.snapshotId() + " should be in appended_snapshots");
    }
  }

  /** Verifies that temporary snapshot processing keys are removed from final properties. */
  @Test
  void testProperties_tempKeysRemoved_success() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata newMetadata = createMetadataWithSnapshotsAndMainRef(baseMetadata, snapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(null, newMetadata);

    assertNotNull(result);
    assertFalse(
        result.properties().containsKey(CatalogConstants.SNAPSHOTS_JSON_KEY),
        "Temp snapshots JSON key should be removed");
    assertFalse(
        result.properties().containsKey(CatalogConstants.SNAPSHOTS_REFS_KEY),
        "Temp snapshots refs key should be removed");
  }
}
