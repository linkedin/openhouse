package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

/**
 * Service responsible for applying snapshot changes to Iceberg table metadata.
 *
 * <p>This class extracts snapshot logic from OpenHouseInternalTableOperations while maintaining the
 * same behavior. The main entry point applySnapshots() has a clear flow: parse input → compute diff
 * → validate → apply.
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
            .orElse(new HashMap<>());

    List<Snapshot> existingSnapshots = base != null ? base.snapshots() : Collections.emptyList();
    Map<String, SnapshotRef> existingRefs = base != null ? base.refs() : Collections.emptyMap();

    // Compute diff (all maps created once in constructor)
    SnapshotDiff diff =
        new SnapshotDiff(
            providedSnapshots, existingSnapshots, metadata, providedRefs, existingRefs);

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
    private final List<Snapshot> existingSnapshots;
    private final TableMetadata metadata;
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

    SnapshotDiff(
        List<Snapshot> providedSnapshots,
        List<Snapshot> existingSnapshots,
        TableMetadata metadata,
        Map<String, SnapshotRef> providedRefs,
        Map<String, SnapshotRef> existingRefs) {
      this.providedSnapshots = providedSnapshots;
      this.existingSnapshots = existingSnapshots;
      this.metadata = metadata;
      this.providedRefs = providedRefs;
      this.existingRefs = existingRefs;

      // Compute all maps once
      this.providedSnapshotByIds =
          providedSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      this.existingSnapshotByIds =
          existingSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      this.existingBranchRefIds =
          existingRefs.values().stream()
              .map(SnapshotRef::snapshotId)
              .collect(Collectors.toSet());
      this.providedBranchRefIds =
          providedRefs.values().stream()
              .map(SnapshotRef::snapshotId)
              .collect(Collectors.toSet());

      // Compute changes
      this.newSnapshots =
          providedSnapshots.stream()
              .filter(s -> !existingSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      this.deletedSnapshots =
          existingSnapshots.stream()
              .filter(s -> !providedSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());

      // Categorize snapshots (simple logic for PR1 - just check summary properties)
      this.stagedSnapshots =
          newSnapshots.stream()
              .filter(s -> s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
              .collect(Collectors.toList());
      this.cherryPickedSnapshots =
          providedSnapshots.stream()
              .filter(
                  s -> {
                    // New snapshot with SOURCE_SNAPSHOT_ID_PROP (actual cherry-pick)
                    if (!existingSnapshotByIds.containsKey(s.snapshotId())
                        && s.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)) {
                      return true;
                    }
                    // WAP snapshot being published (staged → branch transition)
                    // For new snapshots: WAP created and immediately published
                    // For existing snapshots: existing WAP being published (fast-forward)
                    boolean hasWapId =
                        s.summary() != null
                            && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP);
                    boolean wasStaged = !existingBranchRefIds.contains(s.snapshotId());
                    boolean isNowOnBranch = providedBranchRefIds.contains(s.snapshotId());
                    return hasWapId && wasStaged && isNowOnBranch;
                  })
              .collect(Collectors.toList());
      this.regularSnapshots =
          newSnapshots.stream()
              .filter(
                  s ->
                      !s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)
                          && !s.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))
              .collect(Collectors.toList());
    }

    /**
     * Validates all snapshot changes before applying them to table metadata.
     *
     * @param base The base table metadata to validate against (may be null for table creation)
     * @throws InvalidIcebergSnapshotException if any validation check fails
     */
    void validate(TableMetadata base) {
      validateCurrentSnapshotNotDeleted(base);
    }

    /**
     * Validates that the current snapshot is not deleted without providing replacement snapshots.
     * This is the same validation logic from SnapshotInspector.validateSnapshotsUpdate().
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
      if (!newSnapshots.isEmpty()) {
        return;
      }
      long latestSnapshotId = base.currentSnapshot().snapshotId();
      if (!deletedSnapshots.isEmpty()
          && deletedSnapshots.get(deletedSnapshots.size() - 1).snapshotId() == latestSnapshotId) {
        throw new InvalidIcebergSnapshotException(
            String.format(
                "Cannot delete the current snapshot %s without adding replacement snapshots.",
                latestSnapshotId));
      }
    }

    TableMetadata.Builder applyTo(TableMetadata metadata) {
      TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(metadata);

      // Validate only MAIN branch
      for (Map.Entry<String, SnapshotRef> entry : providedRefs.entrySet()) {
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
      for (Snapshot snapshot : stagedSnapshots) {
        metadataBuilder.addSnapshot(snapshot);
      }

      // Only apply NEW cherry-picked snapshots
      // Existing cherry-picked snapshots are handled by fast-forward block below
      for (Snapshot snapshot : cherryPickedSnapshots) {
        if (newSnapshots.contains(snapshot)) {
          metadataBuilder.setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH);
        }
      }

      for (Snapshot snapshot : regularSnapshots) {
        metadataBuilder.setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH);
      }

      // Handle fast-forward cherry-pick (ref update without new snapshot)
      if (newSnapshots.isEmpty() && !providedRefs.isEmpty()) {
        long newSnapshotId = providedRefs.get(SnapshotRef.MAIN_BRANCH).snapshotId();
        if (metadata.refs().isEmpty()
            || metadata.refs().get(SnapshotRef.MAIN_BRANCH).snapshotId() != newSnapshotId) {
          metadataBuilder.setBranchSnapshot(newSnapshotId, SnapshotRef.MAIN_BRANCH);
        }
      }

      // Delete snapshots
      if (!deletedSnapshots.isEmpty()) {
        Set<Long> snapshotIds =
            deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        metadataBuilder.removeSnapshots(snapshotIds);
      }

      return metadataBuilder;
    }

    void recordMetrics(TableMetadata.Builder builder) {
      // Compute appended snapshots (regular + NEW cherry-picked only)
      // Existing cherry-picked snapshots (fast-forward) are not appended
      List<Snapshot> newCherryPicked =
          cherryPickedSnapshots.stream().filter(newSnapshots::contains).collect(Collectors.toList());
      int appendedCount = regularSnapshots.size() + newCherryPicked.size();

      if (appendedCount > 0) {
        metricsReporter.count(InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedCount);
      }
      if (!stagedSnapshots.isEmpty()) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, stagedSnapshots.size());
      }
      if (!cherryPickedSnapshots.isEmpty()) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR,
            cherryPickedSnapshots.size());
      }
      if (!deletedSnapshots.isEmpty()) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, deletedSnapshots.size());
      }

      // Record snapshot IDs in properties
      if (appendedCount > 0) {
        List<Snapshot> appendedSnapshots = new ArrayList<>(regularSnapshots);
        appendedSnapshots.addAll(newCherryPicked);
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                appendedSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(","))));
      }
      if (!stagedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                stagedSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(","))));
      }
      if (!cherryPickedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
                cherryPickedSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(","))));
      }
      if (!deletedSnapshots.isEmpty()) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
                deletedSnapshots.stream()
                    .map(s -> Long.toString(s.snapshotId()))
                    .collect(Collectors.joining(","))));
      }

      builder.removeProperties(
          new HashSet<>(
              Arrays.asList(
                  CatalogConstants.SNAPSHOTS_JSON_KEY, CatalogConstants.SNAPSHOTS_REFS_KEY)));
    }
  }
}
