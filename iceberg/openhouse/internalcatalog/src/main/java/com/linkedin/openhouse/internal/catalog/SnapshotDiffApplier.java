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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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

    // Compute diff (all maps created once in constructor)
    SnapshotDiff diff =
        new SnapshotDiff(providedSnapshots, existingSnapshots, metadata, providedRefs);

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

    // Computed maps (created once)
    private final Map<Long, Snapshot> providedSnapshotByIds;
    private final Map<Long, Snapshot> existingSnapshotByIds;
    private final List<Snapshot> newSnapshots;
    private final List<Snapshot> deletedSnapshots;

    // Categorized snapshots (computed during applyTo)
    private List<String> appendedSnapshots;
    private List<String> stagedSnapshots;
    private List<String> cherryPickedSnapshots;

    SnapshotDiff(
        List<Snapshot> providedSnapshots,
        List<Snapshot> existingSnapshots,
        TableMetadata metadata,
        Map<String, SnapshotRef> providedRefs) {
      this.providedSnapshots = providedSnapshots;
      this.existingSnapshots = existingSnapshots;
      this.metadata = metadata;
      this.providedRefs = providedRefs;

      // Compute all maps once
      this.providedSnapshotByIds =
          providedSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
      this.existingSnapshotByIds =
          existingSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));

      // Compute changes
      this.newSnapshots =
          providedSnapshots.stream()
              .filter(s -> !existingSnapshotByIds.containsKey(s.snapshotId()))
              .collect(Collectors.toList());
      this.deletedSnapshots =
          existingSnapshots.stream()
              .filter(s -> !providedSnapshotByIds.containsKey(s.snapshotId()))
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
      this.appendedSnapshots = new ArrayList<>();
      this.stagedSnapshots = new ArrayList<>();
      this.cherryPickedSnapshots = new ArrayList<>();

      // Validate only MAIN branch
      for (Map.Entry<String, SnapshotRef> entry : providedRefs.entrySet()) {
        if (!entry.getKey().equals(SnapshotRef.MAIN_BRANCH)) {
          throw new UnsupportedOperationException("OpenHouse supports only MAIN branch");
        }
      }

      /**
       * First check if there are new snapshots to be appended to current TableMetadata. If yes,
       * following are the cases to be handled:
       *
       * <p>[1] A regular (non-wap) snapshot is being added to the MAIN branch.
       *
       * <p>[2] A staged (wap) snapshot is being created on top of current snapshot as its base.
       * Recognized by STAGED_WAP_ID_PROP.
       *
       * <p>[3] A staged (wap) snapshot is being cherry picked to the MAIN branch wherein current
       * snapshot in the MAIN branch is not the same as the base snapshot the staged (wap) snapshot
       * was created on. Recognized by SOURCE_SNAPSHOT_ID_PROP. This case is called non-fast forward
       * cherry pick.
       *
       * <p>In case no new snapshots are to be appended to current TableMetadata, there could be a
       * cherrypick of a staged (wap) snapshot on top of the current snapshot in the MAIN branch
       * which is the same as the base snapshot the staged (wap) snapshot was created on. This case
       * is called fast forward cherry pick.
       */
      if (CollectionUtils.isNotEmpty(newSnapshots)) {
        for (Snapshot snapshot : newSnapshots) {
          if (snapshot.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)) {
            // a stage only snapshot using wap.id
            metadataBuilder.addSnapshot(snapshot);
            stagedSnapshots.add(String.valueOf(snapshot.snapshotId()));
          } else if (snapshot.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)) {
            // a snapshot created on a non fast-forward cherry-pick snapshot
            metadataBuilder.setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH);
            appendedSnapshots.add(String.valueOf(snapshot.snapshotId()));
            cherryPickedSnapshots.add(
                String.valueOf(snapshot.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)));
          } else {
            // a regular snapshot
            metadataBuilder.setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH);
            appendedSnapshots.add(String.valueOf(snapshot.snapshotId()));
          }
        }
      } else if (MapUtils.isNotEmpty(providedRefs)) {
        // Updated ref in the main branch with no new snapshot means this is a
        // fast-forward cherry-pick or rollback operation.
        long newSnapshotId = providedRefs.get(SnapshotRef.MAIN_BRANCH).snapshotId();
        // Either the current snapshot is null or the current snapshot is not equal
        // to the new snapshot indicates an update. The first case happens when the
        // stage/wap snapshot being cherry-picked is the first snapshot.
        if (MapUtils.isEmpty(metadata.refs())
            || metadata.refs().get(SnapshotRef.MAIN_BRANCH).snapshotId() != newSnapshotId) {
          metadataBuilder.setBranchSnapshot(newSnapshotId, SnapshotRef.MAIN_BRANCH);
          cherryPickedSnapshots.add(String.valueOf(newSnapshotId));
        }
      }

      // Delete snapshots
      if (CollectionUtils.isNotEmpty(deletedSnapshots)) {
        Set<Long> snapshotIds =
            deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
        metadataBuilder.removeSnapshots(snapshotIds);
      }

      return metadataBuilder;
    }

    void recordMetrics(TableMetadata.Builder builder) {
      if (CollectionUtils.isNotEmpty(appendedSnapshots)) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedSnapshots.size());
      }
      if (CollectionUtils.isNotEmpty(stagedSnapshots)) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, stagedSnapshots.size());
      }
      if (CollectionUtils.isNotEmpty(cherryPickedSnapshots)) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR,
            cherryPickedSnapshots.size());
      }
      if (CollectionUtils.isNotEmpty(deletedSnapshots)) {
        metricsReporter.count(
            InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, deletedSnapshots.size());
      }

      // Record snapshot IDs in properties
      if (CollectionUtils.isNotEmpty(appendedSnapshots)) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
                String.join(",", appendedSnapshots)));
      }
      if (CollectionUtils.isNotEmpty(stagedSnapshots)) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
                String.join(",", stagedSnapshots)));
      }
      if (CollectionUtils.isNotEmpty(cherryPickedSnapshots)) {
        builder.setProperties(
            Collections.singletonMap(
                getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
                String.join(",", cherryPickedSnapshots)));
      }
      if (CollectionUtils.isNotEmpty(deletedSnapshots)) {
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
