package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.util.ArrayList;
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
    private final Map<String, SnapshotRef> providedRefs;

    // Computed maps (created once)
    private final Map<Long, Snapshot> providedSnapshotByIds;
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;

    // Categorized snapshots
    private final List<Snapshot> newStagedSnapshots;
    private final List<Snapshot> cherryPickedSnapshots;

    // Changes
    private final Map<String, SnapshotRef> branchUpdates;
    private final Map<String, List<Snapshot>> newSnapshotsByBranch;
    private final Set<String> staleRefs;
    private final List<Snapshot> unreferencedNewSnapshots;

    // Application state (pre-computed in create)
    private final List<Snapshot> autoAppendedToMainSnapshots;
    private final List<Snapshot> mainBranchSnapshotsForMetrics;
    private final Set<Long> snapshotIdsToAdd;

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

      // Compute changes
      List<Snapshot> newSnapshots =
          providedSnapshots.stream()
              .filter(s -> !existingSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      List<Snapshot> deletedSnapshots =
          existingSnapshots.stream()
              .filter(s -> !providedSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());

      // Categorize snapshots - process in dependency order
      // 1. Cherry-picked has highest priority (includes WAP being published)
      // 2. WAP snapshots (staged, not published)
      // 3. Regular snapshots (everything else)
      Set<Long> existingBranchRefIds =
          existingRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
      Set<Long> providedBranchRefIds =
          providedRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());

      List<Snapshot> cherryPickedSnapshots =
          computeCherryPickedSnapshots(
              providedSnapshots, existingSnapshotByIds, existingBranchRefIds, providedBranchRefIds);
      Set<Long> cherryPickedIds =
          cherryPickedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      List<Snapshot> newStagedSnapshots =
          computeWapSnapshots(
              providedSnapshots, cherryPickedIds, existingBranchRefIds, providedBranchRefIds);
      Set<Long> wapIds =
          newStagedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      List<Snapshot> regularSnapshots =
          computeRegularSnapshots(providedSnapshots, cherryPickedIds, wapIds);

      // Compute branch updates (refs that have changed)
      Map<String, SnapshotRef> branchUpdates =
          providedRefs.entrySet().stream()
              .filter(
                  entry -> {
                    SnapshotRef existing = existingRefs.get(entry.getKey());
                    return existing == null
                        || existing.snapshotId() != entry.getValue().snapshotId();
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Compute new snapshots by branch (only regular snapshots)
      Set<Long> regularSnapshotIds =
          regularSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      Map<String, List<Snapshot>> newSnapshotsByBranch =
          computeNewSnapshotsByBranch(
              branchUpdates, providedSnapshotByIds, existingSnapshotByIds, regularSnapshotIds);

      // Compute derived changes for multi-branch support
      Set<String> staleRefs = new HashSet<>(existingRefs.keySet());
      staleRefs.removeAll(providedRefs.keySet());

      // Collect all snapshot IDs that are in branch lineages (will be added via setBranchSnapshot)
      Set<Long> snapshotsInBranchLineages =
          newSnapshotsByBranch.values().stream()
              .flatMap(List::stream)
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());

      // Compute existing snapshot IDs after deletion
      Set<Long> deletedIds =
          deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      Set<Long> existingAfterDeletionIds = new HashSet<>(existingSnapshotByIds.keySet());
      existingAfterDeletionIds.removeAll(deletedIds);

      // Compute metadata snapshot IDs
      Set<Long> metadataSnapshotIds =
          providedMetadata.snapshots().stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());

      List<Snapshot> unreferencedNewSnapshots =
          providedSnapshots.stream()
              .filter(
                  s ->
                      !existingAfterDeletionIds.contains(s.snapshotId())
                          && !providedBranchRefIds.contains(s.snapshotId())
                          && !metadataSnapshotIds.contains(s.snapshotId())
                          && !snapshotsInBranchLineages.contains(s.snapshotId()))
              .collect(Collectors.toList());

      // Compute auto-appended snapshots for MAIN branch (backward compatibility)
      final List<Snapshot> autoAppendedToMainSnapshots;
      if (!providedRefs.containsKey(SnapshotRef.MAIN_BRANCH)
          && !unreferencedNewSnapshots.isEmpty()) {
        autoAppendedToMainSnapshots =
            unreferencedNewSnapshots.stream()
                .filter(s -> !wapIds.contains(s.snapshotId()))
                .collect(Collectors.toList());
      } else {
        autoAppendedToMainSnapshots = Collections.emptyList();
      }

      // Main branch snapshots for metrics (includes auto-appended)
      List<Snapshot> mainForMetrics =
          new ArrayList<>(
              newSnapshotsByBranch.getOrDefault(SnapshotRef.MAIN_BRANCH, Collections.emptyList()));
      mainForMetrics.addAll(autoAppendedToMainSnapshots);

      // Global set of snapshot IDs to add (deduplicated upfront)
      Set<Long> snapshotIdsToAdd = new HashSet<>();
      autoAppendedToMainSnapshots.stream().map(Snapshot::snapshotId).forEach(snapshotIdsToAdd::add);
      newSnapshotsByBranch.values().stream()
          .flatMap(List::stream)
          .map(Snapshot::snapshotId)
          .forEach(snapshotIdsToAdd::add);
      // Remove any that already exist and aren't deleted
      snapshotIdsToAdd.removeIf(id -> metadataSnapshotIds.contains(id) && !deletedIds.contains(id));

      // Extract database ID from metadata properties
      String databaseId =
          providedMetadata.properties().get(CatalogConstants.OPENHOUSE_DATABASEID_KEY);

      return new SnapshotDiff(
          metricsReporter,
          existingMetadata,
          providedMetadata,
          databaseId,
          providedRefs,
          providedSnapshotByIds,
          newSnapshots,
          deletedSnapshots,
          newStagedSnapshots,
          cherryPickedSnapshots,
          branchUpdates,
          newSnapshotsByBranch,
          staleRefs,
          unreferencedNewSnapshots,
          autoAppendedToMainSnapshots,
          mainForMetrics,
          snapshotIdsToAdd);
    }

    /** Private constructor that accepts all pre-computed values. Use {@link #create} instead. */
    private SnapshotDiff(
        MetricsReporter metricsReporter,
        TableMetadata existingMetadata,
        TableMetadata providedMetadata,
        String databaseId,
        Map<String, SnapshotRef> providedRefs,
        Map<Long, Snapshot> providedSnapshotByIds,
        List<Snapshot> newSnapshots,
        List<Snapshot> deletedSnapshots,
        List<Snapshot> newStagedSnapshots,
        List<Snapshot> cherryPickedSnapshots,
        Map<String, SnapshotRef> branchUpdates,
        Map<String, List<Snapshot>> newSnapshotsByBranch,
        Set<String> staleRefs,
        List<Snapshot> unreferencedNewSnapshots,
        List<Snapshot> autoAppendedToMainSnapshots,
        List<Snapshot> mainBranchSnapshotsForMetrics,
        Set<Long> snapshotIdsToAdd) {
      this.metricsReporter = metricsReporter;
      this.existingMetadata = existingMetadata;
      this.providedMetadata = providedMetadata;
      this.databaseId = databaseId;
      this.providedRefs = providedRefs;
      this.providedSnapshotByIds = providedSnapshotByIds;
      this.newSnapshots = newSnapshots;
      this.deletedSnapshots = deletedSnapshots;
      this.newStagedSnapshots = newStagedSnapshots;
      this.cherryPickedSnapshots = cherryPickedSnapshots;
      this.branchUpdates = branchUpdates;
      this.newSnapshotsByBranch = newSnapshotsByBranch;
      this.staleRefs = staleRefs;
      this.unreferencedNewSnapshots = unreferencedNewSnapshots;
      this.autoAppendedToMainSnapshots = autoAppendedToMainSnapshots;
      this.mainBranchSnapshotsForMetrics = mainBranchSnapshotsForMetrics;
      this.snapshotIdsToAdd = snapshotIdsToAdd;
    }

    /**
     * Computes staged WAP snapshots that are not yet published to any branch.
     *
     * <p>Depends on: cherry-picked IDs (to exclude WAP snapshots being published)
     *
     * @param providedSnapshots All snapshots provided in the update
     * @param excludeCherryPicked Set of cherry-picked snapshot IDs to exclude
     * @param existingBranchRefIds Set of snapshot IDs referenced by existing branches
     * @param providedBranchRefIds Set of snapshot IDs referenced by provided branches
     * @return List of staged WAP snapshots
     */
    private static List<Snapshot> computeWapSnapshots(
        List<Snapshot> providedSnapshots,
        Set<Long> excludeCherryPicked,
        Set<Long> existingBranchRefIds,
        Set<Long> providedBranchRefIds) {
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

    /**
     * Computes regular snapshots that are neither cherry-picked nor staged WAP.
     *
     * <p>Depends on: cherry-picked and WAP IDs (everything else is regular)
     *
     * @param providedSnapshots All snapshots provided in the update
     * @param excludeCherryPicked Set of cherry-picked snapshot IDs to exclude
     * @param excludeWap Set of WAP snapshot IDs to exclude
     * @return List of regular snapshots
     */
    private static List<Snapshot> computeRegularSnapshots(
        List<Snapshot> providedSnapshots, Set<Long> excludeCherryPicked, Set<Long> excludeWap) {
      return providedSnapshots.stream()
          .filter(s -> !excludeCherryPicked.contains(s.snapshotId()))
          .filter(s -> !excludeWap.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    /**
     * Computes new regular snapshots grouped by branch.
     *
     * <p>For each branch that has been updated, walks backward from the new branch head following
     * parent pointers until reaching an existing snapshot (merge base). Collects only new regular
     * snapshots along the way.
     *
     * @param branchUpdates Map of branch names to their new refs (only branches that changed)
     * @param providedSnapshotByIds Index of all provided snapshots by ID
     * @param existingSnapshotByIds Index of all existing snapshots by ID
     * @param regularSnapshotIds Set of IDs for regular (non-WAP, non-cherry-picked) snapshots
     * @return Map of branch names to lists of new regular snapshots added to each branch
     */
    private static Map<String, List<Snapshot>> computeNewSnapshotsByBranch(
        Map<String, SnapshotRef> branchUpdates,
        Map<Long, Snapshot> providedSnapshotByIds,
        Map<Long, Snapshot> existingSnapshotByIds,
        Set<Long> regularSnapshotIds) {

      Map<String, List<Snapshot>> result = new HashMap<>();

      branchUpdates.forEach(
          (branchName, newRef) -> {
            java.util.Deque<Snapshot> branchNewSnapshots = new java.util.ArrayDeque<>();
            long currentId = newRef.snapshotId();

            // Walk backwards from new branch head until we hit an existing snapshot (merge base)
            while (currentId != -1) {
              Snapshot snapshot = providedSnapshotByIds.get(currentId);
              if (snapshot == null) {
                break;
              }

              // Stop at existing snapshot (merge base or old branch head)
              if (existingSnapshotByIds.containsKey(currentId)) {
                break;
              }

              // Collect new regular snapshots in chronological order (add to front)
              if (regularSnapshotIds.contains(currentId)) {
                branchNewSnapshots.addFirst(snapshot);
              }

              currentId = snapshot.parentId() != null ? snapshot.parentId() : -1;
            }

            if (!branchNewSnapshots.isEmpty()) {
              result.put(branchName, new ArrayList<>(branchNewSnapshots));
            }
          });

      return result;
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

      Set<Long> deletedIds =
          deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

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

    /**
     * Applies snapshot changes to the table metadata.
     *
     * <p>Application strategy:
     *
     * <p>[1] Remove deleted snapshots to clean up old data
     *
     * <p>[2] Remove stale branch references that are no longer needed
     *
     * <p>[3] Add unreferenced snapshots (auto-append to MAIN for backward compat, then staged WAP)
     *
     * <p>[4] Set branch pointers for all provided refs. Uses pre-computed deduplication set to
     * determine whether to call setBranchSnapshot (for new snapshots) or setRef (for existing
     * snapshots being fast-forwarded or cherry-picked).
     *
     * <p>[5] Set properties using pre-computed snapshot lists for tracking
     *
     * <p>This order ensures referential integrity is maintained throughout the operation.
     */
    TableMetadata apply() {
      TableMetadata.Builder builder = TableMetadata.buildFrom(this.providedMetadata);

      // [1] Remove deletions
      if (!this.deletedSnapshots.isEmpty()) {
        Set<Long> deletedIds =
            this.deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        builder.removeSnapshots(deletedIds);
      }
      this.staleRefs.forEach(builder::removeRef);

      // [2] Add unreferenced snapshots (auto-append to MAIN for backward compat, then WAP)
      this.autoAppendedToMainSnapshots.forEach(
          s -> builder.setBranchSnapshot(s, SnapshotRef.MAIN_BRANCH));
      this.unreferencedNewSnapshots.stream()
          .filter(
              s ->
                  s.summary() != null
                      && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
          .forEach(builder::addSnapshot);

      // [3] Apply branch operations (using pre-computed deduplication set)
      // First, apply new snapshots for branches that have them
      this.newSnapshotsByBranch.forEach(
          (branchName, branchSnapshots) -> {
            // Add each new snapshot in lineage order
            for (Snapshot snapshot : branchSnapshots) {
              if (this.snapshotIdsToAdd.contains(snapshot.snapshotId())) {
                builder.setBranchSnapshot(snapshot, branchName);
              }
            }
          });

      // Then, set refs for all provided refs (branches without new snapshots, and tags)
      this.providedRefs.forEach(
          (refName, ref) -> {
            // Skip if we already added snapshots for this branch
            if (this.newSnapshotsByBranch.containsKey(refName)
                && !this.newSnapshotsByBranch.get(refName).isEmpty()) {
              return;
            }

            // For refs without new snapshots (fast-forward, cherry-pick, or tags)
            Snapshot refSnapshot = this.providedSnapshotByIds.get(ref.snapshotId());
            if (refSnapshot == null) {
              throw new InvalidIcebergSnapshotException(
                  String.format(
                      "Ref %s references non-existent snapshot %s", refName, ref.snapshotId()));
            }
            builder.setRef(refName, ref);
          });

      // [4] Set properties
      setSnapshotProperties(builder);

      return builder.build();
    }

    /**
     * Sets snapshot-related properties on the metadata builder.
     *
     * <p>Records snapshot IDs in table properties for tracking and cleanup of temporary input
     * properties used for snapshot transfer.
     *
     * @param builder The metadata builder to set properties on
     */
    private void setSnapshotProperties(TableMetadata.Builder builder) {
      // First, remove the temporary transfer properties
      builder.removeProperties(
          new HashSet<>(
              java.util.Arrays.asList(
                  CatalogConstants.SNAPSHOTS_JSON_KEY, CatalogConstants.SNAPSHOTS_REFS_KEY)));

      // Then set the tracking properties
      if (!this.mainBranchSnapshotsForMetrics.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                formatSnapshotIds(this.mainBranchSnapshotsForMetrics)));
      }
      if (!this.newStagedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                formatSnapshotIds(this.newStagedSnapshots)));
      }
      if (!this.cherryPickedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
                formatSnapshotIds(this.cherryPickedSnapshots)));
      }
      if (!this.deletedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
                formatSnapshotIds(this.deletedSnapshots)));
      }
    }

    /**
     * Records metrics for snapshot operations.
     *
     * <p>Tracks counts of: - Regular snapshots added (new commits and cherry-pick results) - Staged
     * snapshots (WAP) - Cherry-picked source snapshots - Deleted snapshots
     */
    void recordMetrics() {
      // Count only MAIN branch snapshots for backward compatibility
      int appendedCount =
          this.newSnapshotsByBranch
              .getOrDefault(SnapshotRef.MAIN_BRANCH, Collections.emptyList())
              .size();
      recordMetricWithDatabaseTag(
          InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedCount);
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
