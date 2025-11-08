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
    Map<String, SnapshotRef> providedRefs =
        Optional.ofNullable(providedMetadata.properties().get(CatalogConstants.SNAPSHOTS_REFS_KEY))
            .map(SnapshotsUtil::parseSnapshotRefs)
            .orElse(new HashMap<>());

    // Validate MAIN-only restriction early (PR1 limitation)
    for (Map.Entry<String, SnapshotRef> entry : providedRefs.entrySet()) {
      if (!entry.getKey().equals(SnapshotRef.MAIN_BRANCH)) {
        throw new UnsupportedOperationException("OpenHouse supports only MAIN branch");
      }
    }

    if (snapshotsJson == null) {
      return providedMetadata;
    }

    // Parse input
    List<Snapshot> providedSnapshots = SnapshotsUtil.parseSnapshots(null, snapshotsJson);

    List<Snapshot> existingSnapshots =
        existingMetadata != null ? existingMetadata.snapshots() : Collections.emptyList();
    Map<String, SnapshotRef> existingRefs =
        existingMetadata != null ? existingMetadata.refs() : Collections.emptyMap();

    // Compute diff (all maps created once in factory method)
    SnapshotDiff diff =
        SnapshotDiff.create(
            metricsReporter,
            existingMetadata,
            providedMetadata,
            existingSnapshots,
            providedSnapshots,
            existingRefs,
            providedRefs);

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
    private final TableMetadata providedMetadata;
    private final String databaseId;
    private final List<Snapshot> existingSnapshots;
    private final List<Snapshot> providedSnapshots;
    private final Map<String, SnapshotRef> existingRefs;
    private final Map<String, SnapshotRef> providedRefs;

    // Computed maps (created once)
    private final Map<Long, Snapshot> providedSnapshotByIds;
    private final Map<Long, Snapshot> existingSnapshotByIds;
    private final Set<Long> existingBranchRefIds;
    private final Set<Long> providedBranchRefIds;
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;
    private final Set<Long> deletedIds;

    // Categorized snapshots
    private final List<Snapshot> newStagedSnapshots;
    private final List<Snapshot> newMainBranchSnapshots;
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
     * @param providedMetadata The new metadata with properties containing snapshot updates
     * @param existingSnapshots Snapshots currently in the table
     * @param providedSnapshots Snapshots provided in the update
     * @param existingRefs Snapshot refs currently in the table
     * @param providedRefs Snapshot refs provided in the update
     * @return A new SnapshotDiff with all analysis computed
     */
    static SnapshotDiff create(
        MetricsReporter metricsReporter,
        TableMetadata existingMetadata,
        TableMetadata providedMetadata,
        List<Snapshot> existingSnapshots,
        List<Snapshot> providedSnapshots,
        Map<String, SnapshotRef> existingRefs,
        Map<String, SnapshotRef> providedRefs) {

      // Compute all index maps once
      Map<Long, Snapshot> providedSnapshotByIds =
          providedSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      Map<Long, Snapshot> existingSnapshotByIds =
          existingSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
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
      Set<Long> deletedIds =
          deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      // Categorize snapshots
      List<Snapshot> newStagedSnapshots =
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
                    // TODO: This works for MAIN branch only,  but fails in the branch scenario and
                    // should be revisited in followup PR
                    // Snapshot exists on branch-A
                    // Cherry-pick to branch-B
                    // Would be classified as NOT wasStaged (because it's in existingBranchRefIds)
                    // Wouldn't be detected as cherry-picked
                    boolean wasStaged = !existingBranchRefIds.contains(provided.snapshotId());
                    boolean isNowOnBranch = providedBranchRefIds.contains(provided.snapshotId());
                    return hasWapId && wasStaged && isNowOnBranch;
                  })
              .collect(Collectors.toList());

      // New main branch snapshots = all new snapshots that are not staged WAP
      // (includes both regular commits and cherry-pick result snapshots)
      List<Snapshot> newMainBranchSnapshots =
          newSnapshots.stream()
              .filter(s -> !s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
              .collect(Collectors.toList());

      // Compute appended count
      int appendedCount = newMainBranchSnapshots.size();

      // Extract database ID from metadata properties
      String databaseId =
          providedMetadata.properties().get(CatalogConstants.OPENHOUSE_DATABASEID_KEY);

      return new SnapshotDiff(
          metricsReporter,
          existingMetadata,
          providedMetadata,
          databaseId,
          existingSnapshots,
          providedSnapshots,
          existingRefs,
          providedRefs,
          providedSnapshotByIds,
          existingSnapshotByIds,
          existingBranchRefIds,
          providedBranchRefIds,
          newSnapshots,
          deletedSnapshots,
          deletedIds,
          newStagedSnapshots,
          newMainBranchSnapshots,
          cherryPickedSnapshots,
          appendedCount);
    }

    /** Private constructor that accepts all pre-computed values. Use {@link #create} instead. */
    private SnapshotDiff(
        MetricsReporter metricsReporter,
        TableMetadata existingMetadata,
        TableMetadata providedMetadata,
        String databaseId,
        List<Snapshot> existingSnapshots,
        List<Snapshot> providedSnapshots,
        Map<String, SnapshotRef> existingRefs,
        Map<String, SnapshotRef> providedRefs,
        Map<Long, Snapshot> providedSnapshotByIds,
        Map<Long, Snapshot> existingSnapshotByIds,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds,
        List<Snapshot> newSnapshots,
        List<Snapshot> deletedSnapshots,
        Set<Long> deletedIds,
        List<Snapshot> newStagedSnapshots,
        List<Snapshot> newMainBranchSnapshots,
        List<Snapshot> cherryPickedSnapshots,
        int appendedCount) {
      this.metricsReporter = metricsReporter;
      this.existingMetadata = existingMetadata;
      this.providedMetadata = providedMetadata;
      this.databaseId = databaseId;
      this.existingSnapshots = existingSnapshots;
      this.providedSnapshots = providedSnapshots;
      this.existingRefs = existingRefs;
      this.providedRefs = providedRefs;
      this.providedSnapshotByIds = providedSnapshotByIds;
      this.existingSnapshotByIds = existingSnapshotByIds;
      this.existingBranchRefIds = existingBranchRefIds;
      this.providedBranchRefIds = providedBranchRefIds;
      this.newSnapshots = newSnapshots;
      this.deletedSnapshots = deletedSnapshots;
      this.deletedIds = deletedIds;
      this.newStagedSnapshots = newStagedSnapshots;
      this.newMainBranchSnapshots = newMainBranchSnapshots;
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
      validateDeletedSnapshotsNotReferenced();
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
      // TODO -- validate what are the requirements around deleting the latest snapshot on a
      // "branch".
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

    /**
     * Validates that snapshots being deleted are not still referenced by any branches or tags. This
     * prevents data loss and maintains referential integrity by ensuring that all branch and tag
     * pointers reference valid snapshots that will continue to exist after the commit.
     *
     * @throws InvalidIcebergSnapshotException if any deleted snapshot is still referenced by a
     *     branch or tag
     */
    private void validateDeletedSnapshotsNotReferenced() {
      Map<Long, List<String>> referencedIdsToRefs =
          providedRefs.entrySet().stream()
              .collect(
                  Collectors.groupingBy(
                      e -> e.getValue().snapshotId(),
                      Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

      List<String> invalidDeleteDetails =
          deletedIds.stream()
              .filter(referencedIdsToRefs::containsKey)
              .map(
                  id ->
                      String.format(
                          "snapshot %s (referenced by: %s)",
                          id, String.join(", ", referencedIdsToRefs.get(id))))
              .collect(Collectors.toList());

      if (!invalidDeleteDetails.isEmpty()) {
        throw new InvalidIcebergSnapshotException(
            String.format(
                "Cannot delete snapshots that are still referenced by branches/tags: %s",
                String.join("; ", invalidDeleteDetails)));
      }
    }

    TableMetadata applyTo() {
      TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(this.providedMetadata);

      /**
       * Apply categorized snapshots to metadata:
       *
       * <p>[1] Staged (WAP) snapshots - added without branch reference
       *
       * <p>[2] New main branch snapshots - added without branch reference (branch pointer set
       * below)
       *
       * <p>[3] Cherry-picked snapshots - existing snapshots, branch pointer set below
       */
      // Add staged snapshots in timestamp order (explicit ordering for consistency)
      this.newStagedSnapshots.stream()
          .sorted(java.util.Comparator.comparingLong(Snapshot::timestampMillis))
          .forEach(metadataBuilder::addSnapshot);

      // Add new main branch snapshots in timestamp order (explicit ordering)
      // Note: While the branch pointer (not list order) determines currentSnapshot(),
      // other code assumes snapshots are time-ordered (e.g., validation at line 308)
      this.newMainBranchSnapshots.stream()
          .sorted(java.util.Comparator.comparingLong(Snapshot::timestampMillis))
          .forEach(metadataBuilder::addSnapshot);

      // Set branch pointer once using providedRefs (covers both new snapshots and cherry-pick)
      if (!this.providedRefs.isEmpty()) {
        long newSnapshotId = this.providedRefs.get(SnapshotRef.MAIN_BRANCH).snapshotId();
        metadataBuilder.setBranchSnapshot(newSnapshotId, SnapshotRef.MAIN_BRANCH);
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
                formatSnapshotIds(this.newMainBranchSnapshots)));
      }
      if (!this.newStagedSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                formatSnapshotIds(this.newStagedSnapshots)));
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
      // Record metrics for appended snapshots (includes regular commits and cherry-pick results)
      // Note: cherryPickedSnapshots list contains existing source snapshots, not the new results
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, this.appendedCount);
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, this.newStagedSnapshots.size());
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR,
          this.cherryPickedSnapshots.size());
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, this.deletedSnapshots.size());
    }

    /**
     * Helper method to record a metric with database tag if count is greater than zero.
     *
     * @param metricName The name of the metric to record
     * @param count The count value to record
     */
    private void recordMetricWithDatabaseTag(String metricName, int count) {
      if (count > 0) {
        // Only add database tag if databaseId is present; otherwise record metric without tag
        if (this.databaseId != null) {
          this.metricsReporter.count(
              metricName, count, InternalCatalogMetricsConstant.DATABASE_TAG, this.databaseId);
        } else {
          this.metricsReporter.count(metricName, count);
        }
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
