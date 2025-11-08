package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.ValidationException;
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

  /** Verifies that when no snapshot JSON is provided, metadata is returned unmodified. */
  @Test
  void testApplySnapshots_noSnapshotsJson_returnsUnmodified() {
    TableMetadata result = snapshotDiffApplier.applySnapshots(null, baseMetadata);

    assertEquals(baseMetadata, result);
    verifyNoInteractions(mockMetricsReporter);
  }

  /** Verifies that table creation (null base) with main branch is handled correctly. */
  @Test
  void testApplySnapshots_nullBase_handlesTableCreationWithMainBranch() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata newMetadata = createMetadataWithSnapshotsAndMainRef(baseMetadata, snapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(null, newMetadata);

    assertNotNull(result);
    assertEquals(snapshots.size(), result.snapshots().size());
  }

  // ========== Basic Functionality Tests ==========

  /** Verifies that new snapshots are added correctly to the main branch. */
  @Test
  void testApplySnapshots_addNewSnapshotsToMainBranch_success() throws IOException {
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

  /** Verifies that deleting snapshots from main branch works correctly. */
  @Test
  void testApplySnapshots_deleteSnapshotsFromMainBranch_success() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());
    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, remainingSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    assertEquals(remainingSnapshots.size(), result.snapshots().size());
  }

  /** Verifies that updating main branch references works correctly. */
  @Test
  void testApplySnapshots_mainBranchUpdates_success() throws IOException {
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

  /** Verifies that snapshots are added in timestamp order to the main branch. */
  @Test
  void testApplySnapshots_snapshotsOrderedByTimestamp_success() throws IOException {
    List<Snapshot> initialSnapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, initialSnapshots);

    // Add extra snapshots which may have different timestamps
    List<Snapshot> extraSnapshots = IcebergTestUtil.getExtraSnapshots();
    List<Snapshot> allSnapshots = new ArrayList<>(initialSnapshots);
    allSnapshots.addAll(extraSnapshots);

    // Deliberately shuffle to ensure ordering is not dependent on input order
    List<Snapshot> shuffledSnapshots = new ArrayList<>(allSnapshots);
    Collections.shuffle(shuffledSnapshots);

    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, shuffledSnapshots);

    TableMetadata result = snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata);

    assertNotNull(result);
    // Verify snapshots are ordered by timestamp
    List<Snapshot> resultSnapshots = result.snapshots();
    assertTrue(resultSnapshots.size() > 0, "Should have snapshots");

    // Verify each snapshot timestamp is <= the next one
    for (int i = 1; i < resultSnapshots.size(); i++) {
      Snapshot prev = resultSnapshots.get(i - 1);
      Snapshot current = resultSnapshots.get(i);
      assertTrue(
          prev.timestampMillis() <= current.timestampMillis(),
          String.format(
              "Snapshots should be ordered by timestamp: snapshot[%d].timestamp=%d "
                  + "should be <= snapshot[%d].timestamp=%d",
              i - 1, prev.timestampMillis(), i, current.timestampMillis()));
    }
  }

  // ========== Validation Tests ==========

  /**
   * Verifies that deleting the current snapshot from main branch without replacements throws an
   * exception.
   */
  @Test
  void testApplySnapshots_deletingCurrentSnapshotFromMainBranchWithoutReplacement_throwsException()
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

  /** Verifies that duplicate snapshot IDs in provided snapshots throw an exception. */
  @Test
  void testApplySnapshots_duplicateSnapshotIds_throwsException() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    // Create a list with duplicate snapshots (same snapshot ID appears twice)
    List<Snapshot> duplicateSnapshots = new ArrayList<>();
    duplicateSnapshots.add(snapshots.get(0));
    duplicateSnapshots.add(snapshots.get(0)); // Duplicate

    TableMetadata newMetadata =
        createMetadataWithSnapshotsAndMainRef(baseWithSnapshots, duplicateSnapshots);

    // Should throw IllegalStateException due to duplicate keys in toMap collector
    assertThrows(
        IllegalStateException.class,
        () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));
  }

  // ========== Metrics Tests ==========

  /** Verifies that staged snapshots (not on main branch) trigger the correct metrics. */
  @Test
  void testMetrics_addStagedSnapshots_recordsStagedCounter() throws IOException {
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

  /** Verifies that deleting snapshots from main branch triggers the correct metrics. */
  @Test
  void testMetrics_deleteSnapshotsFromMainBranch_recordsDeletedCounter() throws IOException {
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

  /** Verifies that appended snapshot IDs to main branch are recorded in properties. */
  @Test
  void testProperties_appendedSnapshotsToMainBranch_recordedCorrectly() throws IOException {
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

  /**
   * Verifies that temporary snapshot processing keys are removed from final properties when adding
   * to main branch.
   */
  @Test
  void testProperties_tempKeysRemovedForMainBranch_success() throws IOException {
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

  /** Verifies that providing a non-MAIN branch reference throws UnsupportedOperationException. */
  @Test
  void testApplySnapshots_nonMainBranchReference_throwsUnsupportedOperationException()
      throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    Snapshot lastSnapshot = snapshots.get(snapshots.size() - 1);

    // Create refs with a feature branch instead of MAIN
    Map<String, String> refs = new HashMap<>();
    SnapshotRef featureBranchRef = SnapshotRef.branchBuilder(lastSnapshot.snapshotId()).build();
    refs.put("feature-branch", SnapshotRefParser.toJson(featureBranchRef));

    TableMetadata newMetadata = createMetadataWithSnapshots(baseMetadata, snapshots, refs);

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> snapshotDiffApplier.applySnapshots(null, newMetadata));

    assertTrue(exception.getMessage().contains("OpenHouse supports only MAIN branch"));
  }

  /**
   * Verifies that providing a branch ref pointing to a non-existent snapshot ID causes an
   * exception. This tests a critical bug where no validation exists before calling
   * setBranchSnapshot.
   */
  @Test
  void testApplySnapshots_refPointingToNonExistentSnapshot_throwsException() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();

    // Create a ref pointing to a snapshot ID that doesn't exist in the snapshot list
    long nonExistentSnapshotId = 999999999L;
    Map<String, String> refs = new HashMap<>();
    SnapshotRef invalidRef = SnapshotRef.branchBuilder(nonExistentSnapshotId).build();
    refs.put(SnapshotRef.MAIN_BRANCH, SnapshotRefParser.toJson(invalidRef));

    TableMetadata newMetadata = createMetadataWithSnapshots(baseMetadata, snapshots, refs);

    // Iceberg's setBranchSnapshot should throw ValidationException when snapshot doesn't exist
    assertThrows(
        ValidationException.class, () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /**
   * Verifies that attempting to set a ref to a snapshot being deleted throws an exception. The
   * validation correctly catches this case where a commit attempts to both delete a snapshot and
   * set the main branch to point to that deleted snapshot. This prevents leaving the table in an
   * invalid state.
   */
  @Test
  void testApplySnapshots_settingRefToDeletedSnapshot_throwsException() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    // Try to delete the first snapshot, then point main branch to the first (deleted) one
    Snapshot snapshotToDelete = snapshots.get(0);
    List<Snapshot> remainingSnapshots = snapshots.subList(1, snapshots.size());

    // Create refs pointing to the snapshot we're trying to delete
    Map<String, String> refs = new HashMap<>();
    SnapshotRef mainRef = SnapshotRef.branchBuilder(snapshotToDelete.snapshotId()).build();
    refs.put(SnapshotRef.MAIN_BRANCH, SnapshotRefParser.toJson(mainRef));

    TableMetadata newMetadata =
        createMetadataWithSnapshots(baseWithSnapshots, remainingSnapshots, refs);

    // This should throw an exception because we're trying to delete a snapshot
    // while setting a branch reference to it
    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(
        exception
            .getMessage()
            .contains("Cannot delete snapshots that are still referenced by branches/tags"));
    assertTrue(exception.getMessage().contains("snapshot " + snapshotToDelete.snapshotId()));
    assertTrue(exception.getMessage().contains("main"));
  }

  /**
   * Verifies that a snapshot with an invalid (non-numeric) source snapshot ID in cherry-pick causes
   * JsonSyntaxException during parsing. NOTE: This fails at the JSON parsing stage due to Iceberg's
   * strict validation, not at the cherry-pick categorization stage.
   */
  @Test
  void testApplySnapshots_invalidCherryPickSourceSnapshotId_failsAtParsingStage() {
    // Create a custom snapshot JSON with invalid source-snapshot-id using Gson
    // Note: Iceberg validates snapshot structure strictly, so this fails at Gson parsing
    Gson gson = new Gson();
    JsonObject snapshotJson = new JsonObject();
    snapshotJson.addProperty("snapshot-id", 1234567890123456789L);
    snapshotJson.addProperty("timestamp-ms", 1669126937912L);
    JsonObject summary = new JsonObject();
    summary.addProperty("operation", "append");
    summary.addProperty("source-snapshot-id", "not-a-number");
    snapshotJson.add("summary", summary);
    snapshotJson.addProperty("manifest-list", "/tmp/test.avro");
    snapshotJson.addProperty("schema-id", 0);

    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(CatalogConstants.SNAPSHOTS_JSON_KEY, "[" + gson.toJson(snapshotJson) + "]");

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);

    // Should throw JsonSyntaxException when Gson tries to parse the invalid source-snapshot-id
    assertThrows(
        com.google.gson.JsonSyntaxException.class,
        () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /**
   * Verifies that a snapshot with null summary is handled correctly during WAP detection. Tests
   * lines 172, 180, 202 which check snapshot.summary(). NOTE: This currently fails at Iceberg's
   * parsing stage due to strict validation.
   */
  @Test
  void testApplySnapshots_snapshotWithNullSummary_failsAtParsingStage() {
    // Create a custom snapshot JSON with null/missing summary using Gson
    // Note: Iceberg validates snapshot structure strictly, so this fails at parsing
    Gson gson = new Gson();
    JsonObject snapshotJson = new JsonObject();
    snapshotJson.addProperty("snapshot-id", 1234567890123456789L);
    snapshotJson.addProperty("timestamp-ms", 1669126937912L);
    snapshotJson.addProperty("manifest-list", "/tmp/test.avro");
    snapshotJson.addProperty("schema-id", 0);

    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(CatalogConstants.SNAPSHOTS_JSON_KEY, "[" + gson.toJson(snapshotJson) + "]");

    // Add a main branch ref pointing to this snapshot
    Map<String, String> refs = new HashMap<>();
    SnapshotRef mainRef = SnapshotRef.branchBuilder(1234567890123456789L).build();
    refs.put(SnapshotRef.MAIN_BRANCH, SnapshotRefParser.toJson(mainRef));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(refs));

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);

    // Should throw JsonSyntaxException during Iceberg parsing due to missing required summary
    assertThrows(
        com.google.gson.JsonSyntaxException.class,
        () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /**
   * Verifies behavior when provided snapshots are empty but refs are not. Tests that a ref pointing
   * to nothing causes an exception.
   */
  @Test
  void testApplySnapshots_emptySnapshotsWithNonEmptyRefs_throwsException() {
    // Create refs pointing to a snapshot that doesn't exist
    Map<String, String> refs = new HashMap<>();
    SnapshotRef mainRef = SnapshotRef.branchBuilder(123456789L).build();
    refs.put(SnapshotRef.MAIN_BRANCH, SnapshotRefParser.toJson(mainRef));

    TableMetadata newMetadata =
        createMetadataWithSnapshots(baseMetadata, Collections.emptyList(), refs);

    // Should throw ValidationException because ref points to non-existent snapshot
    assertThrows(
        org.apache.iceberg.exceptions.ValidationException.class,
        () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /** Verifies that null providedMetadata throws NullPointerException. */
  @Test
  void testApplySnapshots_nullProvidedMetadata_throwsNullPointerException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> snapshotDiffApplier.applySnapshots(baseMetadata, null));

    assertTrue(exception.getMessage().contains("providedMetadata cannot be null"));
  }

  /** Verifies that malformed JSON in SNAPSHOTS_JSON_KEY property throws exception. */
  @Test
  void testApplySnapshots_malformedSnapshotsJson_throwsException() {
    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(CatalogConstants.SNAPSHOTS_JSON_KEY, "{ invalid json {{");

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);

    // Should throw JsonSyntaxException or similar from Gson
    assertThrows(
        com.google.gson.JsonSyntaxException.class,
        () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /** Verifies that malformed JSON in SNAPSHOTS_REFS_KEY property throws exception. */
  @Test
  void testApplySnapshots_malformedRefsJson_throwsException() throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(baseMetadata.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots));
    properties.put(CatalogConstants.SNAPSHOTS_REFS_KEY, "{ invalid json {{");

    TableMetadata newMetadata = baseMetadata.replaceProperties(properties);

    // Should throw JsonSyntaxException or similar from Gson
    assertThrows(
        com.google.gson.JsonSyntaxException.class,
        () -> snapshotDiffApplier.applySnapshots(null, newMetadata));
  }

  /**
   * Verifies behavior when attempting to delete all snapshots with no replacement. This should be
   * caught by the existing validation.
   */
  @Test
  void testApplySnapshots_deletingAllSnapshotsWithNoReplacement_throwsException()
      throws IOException {
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    TableMetadata baseWithSnapshots = addSnapshotsToMetadata(baseMetadata, snapshots);

    // Try to delete all snapshots without providing replacements
    TableMetadata newMetadata =
        createMetadataWithSnapshots(baseWithSnapshots, Collections.emptyList(), new HashMap<>());

    InvalidIcebergSnapshotException exception =
        assertThrows(
            InvalidIcebergSnapshotException.class,
            () -> snapshotDiffApplier.applySnapshots(baseWithSnapshots, newMetadata));

    assertTrue(exception.getMessage().contains("Cannot delete the current snapshot"));
  }
}
