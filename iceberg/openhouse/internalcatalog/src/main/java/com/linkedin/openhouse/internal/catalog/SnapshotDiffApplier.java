package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.google.common.collect.Sets;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

/**
 * Service responsible for applying snapshot changes to Iceberg table metadata.
 *
 * <p>This class handles the complex logic of computing snapshot diffs, validating changes, and
 * applying them to table metadata. It supports various snapshot operations including:
 *
 * <ul>
 *   <li>Adding new snapshots (regular commits)
 *   <li>Staging snapshots (WAP - Write-Audit-Publish)
 *   <li>Cherry-picking snapshots across branches
 *   <li>Deleting snapshots
 *   <li>Updating branch references
 * </ul>
 *
 * <p>The service performs comprehensive validation to ensure data integrity and prevent invalid
 * operations such as deleting referenced snapshots or creating ambiguous branch references.
 */
@AllArgsConstructor
@Slf4j
public class SnapshotDiffApplier {

  private final MetricsReporter metricsReporter;

  /**
   * Applies snapshot updates from metadata properties. Simple and clear: parse input, compute diff,
   * validate, apply, record metrics, build.
   *
   * @param base The base table metadata (may be null for table creation)
   * @param metadata The new metadata with properties containing snapshot updates
   * @return Updated metadata with snapshots applied
   */
  public TableMetadata applySnapshots(TableMetadata base, TableMetadata metadata) {
    String snapshotsJson = metadata.properties().get(CatalogConstants.SNAPSHOTS_JSON_KEY);
    if (snapshotsJson == null) {
      return metadata;
    }

    // Parse input
    List<Snapshot> providedSnapshots = SnapshotsUtil.parseSnapshots(null, snapshotsJson);
    Map<String, SnapshotRef> providedRefs =
        Optional.ofNullable(metadata.properties().get(CatalogConstants.SNAPSHOTS_REFS_KEY))
            .map(SnapshotsUtil::parseSnapshotRefs)
            .orElse(Collections.emptyMap());

    List<Snapshot> existingSnapshots = base != null ? base.snapshots() : Collections.emptyList();
    Map<String, SnapshotRef> existingRefs = base != null ? base.refs() : Collections.emptyMap();

    // Compute diff (all maps created once in constructor)
    SnapshotDiff diff =
        new SnapshotDiff(providedSnapshots, providedRefs, existingSnapshots, existingRefs);

    // Validate, apply, record metrics, build
    diff.validate(base);
    TableMetadata.Builder builder = diff.applyTo(metadata);
    diff.recordMetrics(builder);
    return builder.build();
  }

  /**
   * State object that computes and caches all snapshot analysis. Computes all maps once in the
   * constructor to avoid redundant operations. Provides clear methods for validation and
   * application.
   */
  private class SnapshotDiff {
    // Input state
    private final List<Snapshot> providedSnapshots;
    private final Map<String, SnapshotRef> providedRefs;
    private final List<Snapshot> existingSnapshots;
    private final Map<String, SnapshotRef> existingRefs;

    // Computed maps (created once)
    private final Map<Long, Snapshot> providedById;
    private final Map<Long, Snapshot> existingById;
    private final Set<Long> existingBranchIds;
    private final Set<Long> providedBranchIds;

    // Categorized snapshots
    private final List<Snapshot> wapSnapshots;
    private final List<Snapshot> cherryPickedSnapshots;
    private final List<Snapshot> regularSnapshots;

    // Changes
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;
    private final Map<String, SnapshotRef> branchUpdates;

    SnapshotDiff(
        List<Snapshot> providedSnapshots,
        Map<String, SnapshotRef> providedRefs,
        List<Snapshot> existingSnapshots,
        Map<String, SnapshotRef> existingRefs) {
      this.providedSnapshots = providedSnapshots;
      this.providedRefs = providedRefs;
      this.existingSnapshots = existingSnapshots;
      this.existingRefs = existingRefs;

      // Compute all maps once
      this.providedById =
          providedSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      this.existingById =
          existingSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      this.existingBranchIds =
          existingRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
      this.providedBranchIds =
          providedRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());

      // Compute categorization (order matters: cherry-picked filters WAP)
      List<Snapshot> initialWapSnapshots = computeWapSnapshots();
      this.cherryPickedSnapshots = computeCherryPickedSnapshots();
      this.wapSnapshots = filterWapFromCherryPicked(initialWapSnapshots);
      this.regularSnapshots = computeRegularSnapshots();

      // Compute changes
      this.newSnapshots =
          providedSnapshots.stream()
              .filter(s -> !existingById.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      this.deletedSnapshots =
          existingSnapshots.stream()
              .filter(s -> !providedById.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      this.branchUpdates = computeBranchUpdates();
    }

    private List<Snapshot> computeWapSnapshots() {
      Set<Long> allBranchIds =
          java.util.stream.Stream.concat(existingBranchIds.stream(), providedBranchIds.stream())
              .collect(Collectors.toSet());

      return providedSnapshots.stream()
          .filter(
              s ->
                  s.summary() != null
                      && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)
                      && !allBranchIds.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    private List<Snapshot> computeCherryPickedSnapshots() {
      Set<Long> cherryPickSourceIds =
          providedSnapshots.stream()
              .filter(s -> s.summary() != null && s.summary().containsKey("source-snapshot-id"))
              .map(s -> Long.parseLong(s.summary().get("source-snapshot-id")))
              .collect(Collectors.toSet());

      return providedSnapshots.stream()
          .filter(
              provided -> {
                Snapshot existing = existingById.get(provided.snapshotId());
                if (existing == null) {
                  return false;
                }

                // Parent changed (moved to different branch)
                if (!Objects.equal(provided.parentId(), existing.parentId())) {
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
                boolean wasStaged = !existingBranchIds.contains(provided.snapshotId());
                boolean isNowOnBranch = providedBranchIds.contains(provided.snapshotId());
                return hasWapId && wasStaged && isNowOnBranch;
              })
          .collect(Collectors.toList());
    }

    private List<Snapshot> filterWapFromCherryPicked(List<Snapshot> initialWapSnapshots) {
      Set<Long> cherryPickedIds =
          cherryPickedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      return initialWapSnapshots.stream()
          .filter(s -> !cherryPickedIds.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    private List<Snapshot> computeRegularSnapshots() {
      Set<Long> excludedIds =
          java.util.stream.Stream.concat(
                  wapSnapshots.stream().map(Snapshot::snapshotId),
                  cherryPickedSnapshots.stream().map(Snapshot::snapshotId))
              .collect(Collectors.toSet());

      return providedSnapshots.stream()
          .filter(s -> !excludedIds.contains(s.snapshotId()))
          .collect(Collectors.toList());
    }

    private Map<String, SnapshotRef> computeBranchUpdates() {
      return providedRefs.entrySet().stream()
          .filter(
              entry -> {
                SnapshotRef existing = existingRefs.get(entry.getKey());
                return existing == null || existing.snapshotId() != entry.getValue().snapshotId();
              })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Validates all snapshot changes before applying them to table metadata. Runs multiple
     * validation checks to ensure snapshot operations are safe and consistent.
     *
     * @param base The base table metadata to validate against (may be null for table creation)
     * @throws InvalidIcebergSnapshotException if any validation check fails
     */
    void validate(TableMetadata base) {
      validateCurrentSnapshotNotDeleted(base);
      validateNoAmbiguousCommits();
      validateDeletedSnapshotsNotReferenced();
    }

    /**
     * Validates that the current snapshot is not deleted without providing replacement snapshots.
     * This prevents leaving the table in an inconsistent state where the current snapshot pointer
     * would reference a non-existent snapshot.
     *
     * @param base The base table metadata containing the current snapshot (may be null for table
     *     creation)
     * @throws InvalidIcebergSnapshotException if the current snapshot is being deleted without
     *     replacements
     */
    private void validateCurrentSnapshotNotDeleted(TableMetadata base) {
      if (base == null || base.currentSnapshot() == null) {
        return;
      }

      long currentSnapshotId = base.currentSnapshot().snapshotId();
      boolean currentDeleted =
          deletedSnapshots.stream().anyMatch(s -> s.snapshotId() == currentSnapshotId);

      if (currentDeleted && newSnapshots.isEmpty()) {
        throw new InvalidIcebergSnapshotException(
            String.format(
                "Cannot delete the current snapshot %s without adding replacement snapshots. "
                    + "Deleted: [%s], New: [%s]",
                currentSnapshotId,
                deletedSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(", ")),
                newSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(", "))));
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
      Set<Long> deletedIds =
          deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

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

    TableMetadata.Builder applyTo(TableMetadata metadata) {
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);

      // Remove deleted snapshots
      if (!deletedSnapshots.isEmpty()) {
        Set<Long> deletedIds =
            deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        builder.removeSnapshots(deletedIds);
      }

      // Remove stale branch references
      metadata.refs().keySet().stream()
          .filter(refName -> !providedRefs.containsKey(refName))
          .forEach(builder::removeRef);

      // Track existing snapshot IDs after deletions
      Set<Long> existingAfterDeletion =
          metadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      deletedSnapshots.forEach(s -> existingAfterDeletion.remove(s.snapshotId()));

      // Add unreferenced new snapshots
      providedSnapshots.stream()
          .filter(
              s ->
                  !existingAfterDeletion.contains(s.snapshotId())
                      && !providedBranchIds.contains(s.snapshotId()))
          .forEach(builder::addSnapshot);

      // Set branch pointers
      providedRefs.forEach(
          (branchName, ref) -> {
            Snapshot snapshot = providedById.get(ref.snapshotId());
            if (snapshot == null) {
              throw new InvalidIcebergSnapshotException(
                  String.format(
                      "Branch %s references non-existent snapshot %s",
                      branchName, ref.snapshotId()));
            }

            if (existingAfterDeletion.contains(snapshot.snapshotId())) {
              SnapshotRef existingRef = metadata.refs().get(branchName);
              if (existingRef == null || existingRef.snapshotId() != ref.snapshotId()) {
                builder.setRef(branchName, ref);
              }
            } else {
              builder.setBranchSnapshot(snapshot, branchName);
            }
          });

      return builder;
    }

    void recordMetrics(TableMetadata.Builder builder) {
      int appendedCount =
          (int)
              regularSnapshots.stream()
                  .filter(s -> !existingById.containsKey(s.snapshotId()))
                  .count();
      int stagedCount = wapSnapshots.size();
      int cherryPickedCount = cherryPickedSnapshots.size();
      int deletedCount = deletedSnapshots.size();

      if (appendedCount > 0) {
        metricsReporter.count(InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedCount);
      }
      if (stagedCount > 0) {
        metricsReporter.count(InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, stagedCount);
      }
      if (cherryPickedCount > 0) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR, cherryPickedCount);
      }
      if (deletedCount > 0) {
        metricsReporter.count(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, deletedCount);
      }

      // Record snapshot IDs in properties
      List<Snapshot> newRegularSnapshots =
          regularSnapshots.stream().filter(newSnapshots::contains).collect(Collectors.toList());
      if (!newRegularSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                formatSnapshotIds(newRegularSnapshots)));
      }
      if (!wapSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                formatSnapshotIds(wapSnapshots)));
      }
      if (!cherryPickedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
                formatSnapshotIds(cherryPickedSnapshots)));
      }
      if (!deletedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
                formatSnapshotIds(deletedSnapshots)));
      }

      builder.removeProperties(
          Sets.newHashSet(
              CatalogConstants.SNAPSHOTS_JSON_KEY, CatalogConstants.SNAPSHOTS_REFS_KEY));
    }
  }

  /**
   * Formats a list of snapshots as a comma-separated string of snapshot IDs. Optimized
   * implementation using StringBuilder for better performance with large lists.
   *
   * @param snapshots List of snapshots to format
   * @return Comma-separated string of snapshot IDs, or empty string if list is empty
   */
  private String formatSnapshotIds(List<Snapshot> snapshots) {
    if (snapshots.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < snapshots.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(snapshots.get(i).snapshotId());
    }
    return sb.toString();
  }
}
