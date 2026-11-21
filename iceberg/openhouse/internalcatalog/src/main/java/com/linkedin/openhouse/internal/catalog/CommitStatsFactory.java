package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Builds a neutral {@link CommitStats} from the committed {@link TableMetadata}.
 *
 * <p>All the fields the optimizer needs are available in-process at commit time: point-in-time
 * totals and per-commit deltas come from the current snapshot's {@link SnapshotSummary}; identity
 * and location come from OpenHouse canonical table properties. No extra metadata read or storage
 * call is required. Whether stats should be collected at all is decided separately by the calling
 * post-commit operation (per-table property gate).
 */
@Slf4j
public final class CommitStatsFactory {

  private CommitStatsFactory() {
    // no-op for util class constructor
  }

  /**
   * Extract {@link CommitStats} from a successful commit. Returns empty when the table has no
   * stable UUID (nothing to key on). Current-state totals and the per-commit {@link
   * CommitStats.Delta} are populated from the current snapshot summary when present; for a commit
   * with no new snapshot (metadata-only) they are left null, so the publish is properties-only and
   * reports the same current state as the prior commit.
   *
   * <p>The database/table name is read from the committed {@code openhouse.databaseId} / {@code
   * openhouse.tableId} properties rather than {@code tableIdentifier}, which on a rename still
   * holds the <i>old</i> name — the committed properties always carry the new one. The identifier
   * is only a fallback for the rare commit that omits those properties.
   *
   * @param commitType the kind of commit; stamped onto the returned stats
   */
  public static Optional<CommitStats> extract(
      TableIdentifier tableIdentifier, TableMetadata committedMetadata, CommitType commitType) {
    if (committedMetadata == null) {
      return Optional.empty();
    }
    Map<String, String> properties = committedMetadata.properties();
    String tableUuid = properties.get(getCanonicalFieldName("tableUUID"));
    if (tableUuid == null || tableUuid.isEmpty()) {
      log.debug(
          "Skipping stats extraction for {}: no {} property present",
          tableIdentifier,
          getCanonicalFieldName("tableUUID"));
      return Optional.empty();
    }

    CommitStats.CommitStatsBuilder builder =
        CommitStats.builder()
            .tableUuid(tableUuid)
            .commitType(commitType)
            .databaseName(
                firstNonBlank(
                    properties.get(getCanonicalFieldName("databaseId")),
                    tableIdentifier.namespace().toString()))
            .tableName(
                firstNonBlank(
                    properties.get(getCanonicalFieldName("tableId")), tableIdentifier.name()))
            .tableLocation(committedMetadata.location())
            .tableVersion(properties.get(getCanonicalFieldName("tableVersion")))
            .tableProperties(Collections.unmodifiableMap(properties));

    Snapshot currentSnapshot = committedMetadata.currentSnapshot();
    if (currentSnapshot != null) {
      Map<String, String> summary =
          currentSnapshot.summary() == null ? Collections.emptyMap() : currentSnapshot.summary();
      builder
          .numCurrentFiles(parseLong(summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP)))
          .tableSizeBytes(parseLong(summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP)))
          .delta(
              CommitStats.Delta.builder()
                  .numFilesAdded(parseLong(summary.get(SnapshotSummary.ADDED_FILES_PROP)))
                  .numFilesDeleted(parseLong(summary.get(SnapshotSummary.DELETED_FILES_PROP)))
                  .addedSizeBytes(parseLong(summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP)))
                  .deletedSizeBytes(parseLong(summary.get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)))
                  .build());
    }
    return Optional.of(builder.build());
  }

  /** Parse a summary value to Long, tolerating null/malformed values (returns null). */
  private static Long parseLong(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Return {@code value} if it is non-null and non-blank, otherwise {@code fallback}. */
  private static String firstNonBlank(String value, String fallback) {
    return (value == null || value.trim().isEmpty()) ? fallback : value;
  }
}
