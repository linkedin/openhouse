package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;

/**
 * Service responsible for applying snapshot changes to Iceberg table metadata.
 *
 * <p>The main entry point applySnapshots() has a clear flow: parse input → compute diff → validate
 * → apply.
 */
@AllArgsConstructor
@Slf4j
public class SnapshotDiffApplier {

  private final MetricsReporter metricsReporter;

  /**
   * Applies snapshot updates from metadata properties. Simple and clear: parse input, compute diff,
   * validate, apply, record metrics, build.
   *
   * @param existingMetadata The existing table metadata (may be null for table creation)
   * @param providedMetadata The new metadata with properties containing snapshot updates
   * @return Updated metadata with snapshots applied
   * @throws NullPointerException if providedMetadata is null
   */
  public TableMetadata applySnapshots(
      TableMetadata existingMetadata, TableMetadata providedMetadata) {
    // Validate at system boundary
    Objects.requireNonNull(providedMetadata, "providedMetadata cannot be null");

    String snapshotsJson = providedMetadata.properties().get(CatalogConstants.SNAPSHOTS_JSON_KEY);
    if (snapshotsJson == null) {
      return providedMetadata;
    }

    // Parse input
    List<Snapshot> providedSnapshots = SnapshotsUtil.parseSnapshots(null, snapshotsJson);
    Map<String, SnapshotRef> providedRefs =
        Optional.ofNullable(providedMetadata.properties().get(CatalogConstants.SNAPSHOTS_REFS_KEY))
            .map(SnapshotsUtil::parseSnapshotRefs)
            .orElse(new HashMap<>());

    List<Snapshot> existingSnapshots =
        existingMetadata != null ? existingMetadata.snapshots() : Collections.emptyList();
    Map<String, SnapshotRef> existingRefs =
        existingMetadata != null ? existingMetadata.refs() : Collections.emptyMap();

    // Compute diff (all maps created once in factory method)
    SnapshotDiff diff =
        SnapshotDiff.create(
            metricsReporter,
            existingMetadata,
            providedSnapshots,
            existingSnapshots,
            providedMetadata,
            providedRefs,
            existingRefs);

    // Validate, apply, record metrics (in correct order)
    diff.validate();
    TableMetadata result = diff.applyTo();
    diff.recordMetrics();
    return result;
  }

  /**
   * State object that computes and caches all snapshot analysis. Computes all maps once in the
   * factory method to avoid redundant operations. Provides clear methods for validation and
   * application.
   */
  private static class SnapshotDiff {
    // Injected dependency
    private final MetricsReporter metricsReporter;

    // Input state
    private final TableMetadata existingMetadata;
    private final List<Snapshot> providedSnapshots;
    private final List<Snapshot> existingSnapshots;
    private final TableMetadata providedMetadata;
    private final Map<String, SnapshotRef> providedRefs;
    private final Map<String, SnapshotRef> existingRefs;

    // Computed maps (created once)
    private final Map<Long, Snapshot> providedSnapshotByIds;
    private final Map<Long, Snapshot> existingSnapshotByIds;
    private final Set<Long> existingBranchRefIds;
    private final Set<Long> providedBranchRefIds;
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;

    // Categorized snapshots
    private final List<Snapshot> stagedSnapshots;
    private final List<Snapshot> regularSnapshots;
    private final List<Snapshot> cherryPickedSnapshots;
    private final int appendedCount;

    /**
     * Creates a SnapshotDiff by computing all snapshot analysis from the provided inputs.
     *
     * <p>Preconditions: All parameters except existingMetadata must be non-null. Collections should
     * be empty rather than null.
     *
     * @param metricsReporter Metrics reporter for recording snapshot operations
     * @param existingMetadata The existing table metadata (may be null for table creation)
     * @param providedSnapshots Snapshots provided in the update
     * @param existingSnapshots Snapshots currently in the table
     * @param providedMetadata The new metadata with properties containing snapshot updates
     * @param providedRefs Snapshot refs provided in the update
     * @param existingRefs Snapshot refs currently in the table
     * @return A new SnapshotDiff with all analysis computed
     */
    static SnapshotDiff create(
        MetricsReporter metricsReporter,
        TableMetadata existingMetadata,
        List<Snapshot> providedSnapshots,
        List<Snapshot> existingSnapshots,
        TableMetadata providedMetadata,
        Map<String, SnapshotRef> providedRefs,
        Map<String, SnapshotRef> existingRefs) {

      // Compute all index maps once
      Map<Long, Snapshot> providedSnapshotByIds =
          providedSnapshots.stream()
              .collect(
                  Collectors.toMap(
                      Snapshot::snapshotId, s -> s, (existing, replacement) -> existing));
      Map<Long, Snapshot> existingSnapshotByIds =
          existingSnapshots.stream()
              .collect(
                  Collectors.toMap(
                      Snapshot::snapshotId, s -> s, (existing, replacement) -> existing));
      Set<Long> existingBranchRefIds =
          existingRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
      Set<Long> providedBranchRefIds =
          providedRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());

      // Compute changes
      List<Snapshot> newSnapshots =
          providedSnapshots.stream()
              .filter(s -> !existingSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      List<Snapshot> deletedSnapshots =
          existingSnapshots.stream()
              .filter(s -> !providedSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());

      // Categorize snapshots
      List<Snapshot> stagedSnapshots =
          newSnapshots.stream()
              .filter(s -> s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
              .collect(Collectors.toList());

      // Compute source IDs for cherry-pick operations
      Set<Long> cherryPickSourceIds =
          providedSnapshots.stream()
              .filter(
                  s ->
                      s.summary() != null
                          && s.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))
              .map(s -> Long.parseLong(s.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)))
              .collect(Collectors.toSet());

      List<Snapshot> cherryPickedSnapshots =
          providedSnapshots.stream()
              .filter(
                  provided -> {
                    // Only consider EXISTING snapshots as cherry-picked
                    Snapshot existing = existingSnapshotByIds.get(provided.snapshotId());
                    if (existing == null) {
                      return false;
                    }

                    // Is source of cherry-pick
                    if (cherryPickSourceIds.contains(provided.snapshotId())) {
                      return true;
                    }

                    // WAP snapshot being published (staged → branch transition)
                    boolean hasWapId =
                        provided.summary() != null
                            && provided.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP);
                    boolean wasStaged = !existingBranchRefIds.contains(provided.snapshotId());
                    boolean isNowOnBranch = providedBranchRefIds.contains(provided.snapshotId());
                    return hasWapId && wasStaged && isNowOnBranch;
                  })
              .collect(Collectors.toList());

      // Regular snapshots = all new snapshots that are not staged WAP
      List<Snapshot> regularSnapshots =
          newSnapshots.stream()
              .filter(s -> !s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
              .collect(Collectors.toList());

      // Compute appended count (only regular snapshots, not cherry-picked)
      int appendedCount = regularSnapshots.size();

      return new SnapshotDiff(
          metricsReporter,
          existingMetadata,
          providedSnapshots,
          existingSnapshots,
          providedMetadata,
          providedRefs,
          existingRefs,
          providedSnapshotByIds,
          existingSnapshotByIds,
          existingBranchRefIds,
          providedBranchRefIds,
          newSnapshots,
          deletedSnapshots,
          stagedSnapshots,
          regularSnapshots,
          cherryPickedSnapshots,
          appendedCount);
    }

    /** Private constructor that accepts all pre-computed values. Use {@link #create} instead. */
    private SnapshotDiff(
        MetricsReporter metricsReporter,
        TableMetadata existingMetadata,
        List<Snapshot> providedSnapshots,
        List<Snapshot> existingSnapshots,
        TableMetadata providedMetadata,
        Map<String, SnapshotRef> providedRefs,
        Map<String, SnapshotRef> existingRefs,
        Map<Long, Snapshot> providedSnapshotByIds,
        Map<Long, Snapshot> existingSnapshotByIds,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds,
        List<Snapshot> newSnapshots,
        List<Snapshot> deletedSnapshots,
        List<Snapshot> stagedSnapshots,
        List<Snapshot> regularSnapshots,
        List<Snapshot> cherryPickedSnapshots,
        int appendedCount) {
      this.metricsReporter = metricsReporter;
      this.existingMetadata = existingMetadata;
      this.providedSnapshots = providedSnapshots;
      this.existingSnapshots = existingSnapshots;
      this.providedMetadata = providedMetadata;
      this.providedRefs = providedRefs;
      this.existingRefs = existingRefs;
      this.providedSnapshotByIds = providedSnapshotByIds;
      this.existingSnapshotByIds = existingSnapshotByIds;
      this.existingBranchRefIds = existingBranchRefIds;
      this.providedBranchRefIds = providedBranchRefIds;
      this.newSnapshots = newSnapshots;
      this.deletedSnapshots = deletedSnapshots;
      this.stagedSnapshots = stagedSnapshots;
      this.regularSnapshots = regularSnapshots;
      this.cherryPickedSnapshots = cherryPickedSnapshots;
      this.appendedCount = appendedCount;
    }

    /**
     * Validates all snapshot changes before applying them to table metadata.
     *
     * @throws InvalidIcebergSnapshotException if any validation check fails
     */
    void validate() {
      validateCurrentSnapshotNotDeleted();
    }

    /**
     * Validates that the current snapshot is not deleted without providing replacement snapshots.
     *
     * @throws InvalidIcebergSnapshotException if the current snapshot is being deleted without
     *     replacements
     */
    private void validateCurrentSnapshotNotDeleted() {
      if (this.existingMetadata == null || this.existingMetadata.currentSnapshot() == null) {
        return;
      }
      if (!this.newSnapshots.isEmpty()) {
        return;
      }
      long latestSnapshotId = this.existingMetadata.currentSnapshot().snapshotId();
      // Check if the last deleted snapshot is the current one (snapshots are ordered by time)
      if (!this.deletedSnapshots.isEmpty()
          && this.deletedSnapshots.get(this.deletedSnapshots.size() - 1).snapshotId()
              == latestSnapshotId) {
        throw new InvalidIcebergSnapshotException(
            String.format(
                "Cannot delete the current snapshot %s without adding replacement snapshots.",
                latestSnapshotId));
      }
    }

    TableMetadata applyTo() {
      TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(this.providedMetadata);

      // Validate only MAIN branch
      for (Map.Entry<String, SnapshotRef> entry : this.providedRefs.entrySet()) {
        if (!entry.getKey().equals(SnapshotRef.MAIN_BRANCH)) {
          throw new UnsupportedOperationException("OpenHouse supports only MAIN branch");
        }
      }

      /**
       * Apply categorized snapshots to metadata:
       *
       * <p>[1] Staged (WAP) snapshots - added without branch reference
       *
       * <p>[2] Cherry-picked snapshots - set as main branch snapshot
       *
       * <p>[3] Regular snapshots - set as main branch snapshot
       */
      for (Snapshot snapshot : this.stagedSnapshots) {
        metadataBuilder.addSnapshot(snapshot);
      }

      // Cherry-picked snapshots are all existing, handled by fast-forward block below
      // (No need to apply them here)

      for (Snapshot snapshot : this.regularSnapshots) {
        metadataBuilder.setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH);
      }

      // Handle fast-forward cherry-pick (ref update without new snapshot)
      if (this.newSnapshots.isEmpty() && !this.providedRefs.isEmpty()) {
        long newSnapshotId = this.providedRefs.get(SnapshotRef.MAIN_BRANCH).snapshotId();
        if (this.providedMetadata.refs().isEmpty()
            || this.providedMetadata.refs().get(SnapshotRef.MAIN_BRANCH).snapshotId()
                != newSnapshotId) {
          metadataBuilder.setBranchSnapshot(newSnapshotId, SnapshotRef.MAIN_BRANCH);
        }
      }

      // Delete snapshots
      if (!this.deletedSnapshots.isEmpty()) {
        Set<Long> snapshotIds =
            this.deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        metadataBuilder.removeSnapshots(snapshotIds);
      }

      // Record snapshot IDs in properties and cleanup input properties
      if (this.appendedCount > 0) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                formatSnapshotIds(this.regularSnapshots)));
      }
      if (!this.stagedSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                formatSnapshotIds(this.stagedSnapshots)));
      }
      if (!this.cherryPickedSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
                formatSnapshotIds(this.cherryPickedSnapshots)));
      }
      if (!this.deletedSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
                formatSnapshotIds(this.deletedSnapshots)));
      }
      metadataBuilder.removeProperties(
          new HashSet<>(
              Arrays.asList(
                  CatalogConstants.SNAPSHOTS_JSON_KEY, CatalogConstants.SNAPSHOTS_REFS_KEY)));

      return metadataBuilder.build();
    }

    void recordMetrics() {
      // Record metrics for appended snapshots (only regular snapshots)
      // Cherry-picked snapshots are all existing, not appended
      if (this.appendedCount > 0) {
        this.metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, this.appendedCount);
      }
      if (!this.stagedSnapshots.isEmpty()) {
        this.metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, this.stagedSnapshots.size());
      }
      if (!this.cherryPickedSnapshots.isEmpty()) {
        this.metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR,
            this.cherryPickedSnapshots.size());
      }
      if (!this.deletedSnapshots.isEmpty()) {
        this.metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, this.deletedSnapshots.size());
      }
    }

    /**
     * Helper method to format a list of snapshots into a comma-separated string of snapshot IDs.
     *
     * @param snapshots List of snapshots to format
     * @return Comma-separated string of snapshot IDs
     */
    private static String formatSnapshotIds(List<Snapshot> snapshots) {
      return snapshots.stream()
          .map(s -> Long.toString(s.snapshotId()))
          .collect(Collectors.joining(","));
    }
  }
}
