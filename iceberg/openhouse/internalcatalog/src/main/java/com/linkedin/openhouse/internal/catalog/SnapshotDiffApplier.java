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
    TableMetadata result = diff.apply();
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
    private final Set<Long> metadataSnapshotIds;
    private final Set<Long> existingBranchRefIds;
    private final Set<Long> providedBranchRefIds;
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;
    private final Set<Long> deletedIds;

    // Categorized snapshots
    private final List<Snapshot> wapSnapshots;
    private final List<Snapshot> cherryPickedSnapshots;
    private final List<Snapshot> regularSnapshots;

    // Changes
    private final Map<String, SnapshotRef> branchUpdates;
    private final List<Snapshot> newRegularSnapshots;
    private final Set<String> staleRefs;
    private final Set<Long> existingAfterDeletionIds;
    private final List<Snapshot> unreferencedNewSnapshots;

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
      Set<Long> metadataSnapshotIds =
          providedMetadata.snapshots().stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());
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

      // Compute categorization - process in dependency order
      // 1. Cherry-picked has highest priority (includes WAP being published)
      // 2. WAP snapshots (staged, not published)
      // 3. Regular snapshots (everything else)
      List<Snapshot> cherryPickedSnapshots =
          computeCherryPickedSnapshots(
              providedSnapshots, existingSnapshotByIds, existingBranchRefIds, providedBranchRefIds);
      Set<Long> cherryPickedIds =
          cherryPickedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      List<Snapshot> wapSnapshots =
          computeWapSnapshots(
              providedSnapshots, cherryPickedIds, existingBranchRefIds, providedBranchRefIds);
      Set<Long> wapIds =
          wapSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      List<Snapshot> regularSnapshots =
          computeRegularSnapshots(providedSnapshots, cherryPickedIds, wapIds);

      // Compute branch updates
      Map<String, SnapshotRef> branchUpdates =
          providedRefs.entrySet().stream()
              .filter(
                  entry -> {
                    SnapshotRef existing = existingRefs.get(entry.getKey());
                    return existing == null
                        || existing.snapshotId() != entry.getValue().snapshotId();
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Compute other changes
      List<Snapshot> newRegularSnapshots =
          regularSnapshots.stream().filter(newSnapshots::contains).collect(Collectors.toList());
      Set<String> staleRefs = new HashSet<>(existingRefs.keySet());
      staleRefs.removeAll(providedRefs.keySet());
      Set<Long> existingAfterDeletionIds = new HashSet<>(existingSnapshotByIds.keySet());
      existingAfterDeletionIds.removeAll(deletedIds);
      List<Snapshot> unreferencedNewSnapshots =
          providedSnapshots.stream()
              .filter(
                  s ->
                      !existingAfterDeletionIds.contains(s.snapshotId())
                          && !providedBranchRefIds.contains(s.snapshotId())
                          && !metadataSnapshotIds.contains(s.snapshotId()))
              .collect(Collectors.toList());

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
          metadataSnapshotIds,
          existingBranchRefIds,
          providedBranchRefIds,
          newSnapshots,
          deletedSnapshots,
          deletedIds,
          wapSnapshots,
          cherryPickedSnapshots,
          regularSnapshots,
          branchUpdates,
          newRegularSnapshots,
          staleRefs,
          existingAfterDeletionIds,
          unreferencedNewSnapshots);
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
        Set<Long> metadataSnapshotIds,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds,
        List<Snapshot> newSnapshots,
        List<Snapshot> deletedSnapshots,
        Set<Long> deletedIds,
        List<Snapshot> wapSnapshots,
        List<Snapshot> cherryPickedSnapshots,
        List<Snapshot> regularSnapshots,
        Map<String, SnapshotRef> branchUpdates,
        List<Snapshot> newRegularSnapshots,
        Set<String> staleRefs,
        Set<Long> existingAfterDeletionIds,
        List<Snapshot> unreferencedNewSnapshots) {
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
      this.metadataSnapshotIds = metadataSnapshotIds;
      this.existingBranchRefIds = existingBranchRefIds;
      this.providedBranchRefIds = providedBranchRefIds;
      this.newSnapshots = newSnapshots;
      this.deletedSnapshots = deletedSnapshots;
      this.deletedIds = deletedIds;
      this.wapSnapshots = wapSnapshots;
      this.cherryPickedSnapshots = cherryPickedSnapshots;
      this.regularSnapshots = regularSnapshots;
      this.branchUpdates = branchUpdates;
      this.newRegularSnapshots = newRegularSnapshots;
      this.staleRefs = staleRefs;
      this.existingAfterDeletionIds = existingAfterDeletionIds;
      this.unreferencedNewSnapshots = unreferencedNewSnapshots;
    }

    private static List<Snapshot> computeWapSnapshots(
        List<Snapshot> providedSnapshots,
        Set<Long> excludeCherryPicked,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds) {
      // Depends on: cherry-picked IDs (to exclude WAP snapshots being published)
      Set<Long> allBranchRefIds =
          java.util.stream.Stream.concat(
                  existingBranchRefIds.stream(), providedBranchRefIds.stream())
              .collect(Collectors.toSet());

      return providedSnapshots.stream()
          .filter(s -> !excludeCherryPicked.contains(s.snapshotId()))
          .filter(
              s ->
                  s.summary() != null
                      && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)
                      && !allBranchRefIds.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    private static List<Snapshot> computeCherryPickedSnapshots(
        List<Snapshot> providedSnapshots,
        Map<Long, Snapshot> existingSnapshotByIds,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds) {
      Set<Long> cherryPickSourceIds =
          providedSnapshots.stream()
              .filter(
                  s ->
                      s.summary() != null
                          && s.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))
              .map(s -> Long.parseLong(s.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)))
              .collect(Collectors.toSet());

      return providedSnapshots.stream()
          .filter(
              provided -> {
                // Only consider EXISTING snapshots as cherry-picked
                Snapshot existing = existingSnapshotByIds.get(provided.snapshotId());
                if (existing == null) {
                  return false;
                }

                // Parent changed (moved to different branch)
                if (!Objects.equals(provided.parentId(), existing.parentId())) {
                  return true;
                }

                // Is source of cherry-pick
                if (cherryPickSourceIds.contains(provided.snapshotId())) {
                  return true;
                }

                // WAP snapshot being published (staged → branch)
                boolean hasWapId =
                    provided.summary() != null
                        && provided.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP);
                boolean wasStaged = !existingBranchRefIds.contains(provided.snapshotId());
                boolean isNowOnBranch = providedBranchRefIds.contains(provided.snapshotId());
                return hasWapId && wasStaged && isNowOnBranch;
              })
          .collect(Collectors.toList());
    }

    private static List<Snapshot> computeRegularSnapshots(
        List<Snapshot> providedSnapshots, Set<Long> excludeCherryPicked, Set<Long> excludeWap) {
      // Depends on: cherry-picked and WAP IDs (everything else is regular)
      return providedSnapshots.stream()
          .filter(s -> !excludeCherryPicked.contains(s.snapshotId()))
          .filter(s -> !excludeWap.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    /**
     * Validates all snapshot changes before applying them to table metadata.
     *
     * @throws InvalidIcebergSnapshotException if any validation check fails
     */
    void validate() {
      validateCurrentSnapshotNotDeleted();
      validateNoAmbiguousCommits();
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
     * Validates that no single snapshot is referenced by multiple branches in the same commit. This
     * prevents ambiguous commits where it's unclear which branch should be the primary reference
     * for a snapshot. Each snapshot can only be associated with one branch per commit to maintain
     * clear lineage and avoid conflicts.
     *
     * @throws InvalidIcebergSnapshotException if a snapshot is referenced by multiple branches
     */
    private void validateNoAmbiguousCommits() {
      Map<Long, List<String>> snapshotToBranches =
          branchUpdates.entrySet().stream()
              .collect(
                  Collectors.groupingBy(
                      e -> e.getValue().snapshotId(),
                      Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

      snapshotToBranches.forEach(
          (snapshotId, branches) -> {
            if (branches.size() > 1) {
              throw new InvalidIcebergSnapshotException(
                  String.format(
                      "Ambiguous commit: snapshot %s is referenced by multiple branches [%s] in a single commit. "
                          + "Each snapshot can only be referenced by one branch per commit.",
                      snapshotId, String.join(", ", branches)));
            }
          });
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

    TableMetadata apply() {
      TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(this.providedMetadata);

      // Remove deleted snapshots
      if (!this.deletedSnapshots.isEmpty()) {
        metadataBuilder.removeSnapshots(this.deletedIds);
      }

      // Remove stale branch references
      this.staleRefs.forEach(metadataBuilder::removeRef);

      // Add unreferenced new snapshots
      this.unreferencedNewSnapshots.forEach(metadataBuilder::addSnapshot);

      // Set branch pointers
      this.providedRefs.forEach(
          (branchName, ref) -> {
            Snapshot snapshot = this.providedSnapshotByIds.get(ref.snapshotId());
            if (snapshot == null) {
              throw new InvalidIcebergSnapshotException(
                  String.format(
                      "Branch %s references non-existent snapshot %s",
                      branchName, ref.snapshotId()));
            }

            // Check if snapshot is already in metadata (after deletions)
            boolean snapshotExistsInMetadata =
                this.metadataSnapshotIds.contains(snapshot.snapshotId())
                    && !this.deletedIds.contains(snapshot.snapshotId());

            if (snapshotExistsInMetadata) {
              SnapshotRef existingRef = this.providedMetadata.refs().get(branchName);
              if (existingRef == null || existingRef.snapshotId() != ref.snapshotId()) {
                metadataBuilder.setRef(branchName, ref);
              }
            } else {
              metadataBuilder.setBranchSnapshot(snapshot, branchName);
            }
          });

      // Record snapshot IDs in properties and cleanup input properties
      if (!this.newRegularSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                formatSnapshotIds(this.newRegularSnapshots)));
      }
      if (!this.wapSnapshots.isEmpty()) {
        metadataBuilder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                formatSnapshotIds(this.wapSnapshots)));
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
      int appendedCount =
          (int)
              this.regularSnapshots.stream()
                  .filter(s -> !this.existingSnapshotByIds.containsKey(s.snapshotId()))
                  .count();
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedCount);
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, this.wapSnapshots.size());
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
