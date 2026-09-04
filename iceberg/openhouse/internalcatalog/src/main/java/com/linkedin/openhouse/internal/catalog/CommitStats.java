package com.linkedin.openhouse.internal.catalog;

import java.util.Map;
import lombok.Builder;
import lombok.Value;

/**
 * Neutral, transport-agnostic snapshot of the stats derived from a single successful commit.
 *
 * <p>Produced by {@link CommitStatsFactory} from the committed {@link
 * org.apache.iceberg.TableMetadata} and handed to a {@link TableStatsPublisher}. Kept free of any
 * optimizer-client types so the internal catalog does not depend on the optimizer client; the
 * concrete publisher maps this to the optimizer stats API request.
 *
 * <p>Snapshot/delta metrics are {@link Long} and may be {@code null} when the commit produced no
 * new snapshot (e.g. stage-create/replace, metadata-only, or replicated-table create), in which
 * case the publish is effectively properties-only.
 */
@Value
@Builder
public class CommitStats {

  /** Stable Iceberg table UUID (from {@code openhouse.tableUUID}). Never null. */
  String tableUuid;

  /** Database (namespace) name. */
  String databaseName;

  /** Table name. */
  String tableName;

  /** Storage root location of the table. */
  String tableLocation;

  /** OpenHouse table-version pointer at commit time. */
  String tableVersion;

  /** Total data files as of the committed snapshot; null when no current snapshot. */
  Long numCurrentFiles;

  /** Total on-disk size (bytes) as of the committed snapshot; null when no current snapshot. */
  Long tableSizeBytes;

  /** Data files added by this commit; null when no current snapshot. */
  Long numFilesAdded;

  /** Data files removed by this commit; null when no current snapshot. */
  Long numFilesDeleted;

  /** Bytes added by this commit; null when no current snapshot. */
  Long addedSizeBytes;

  /** Bytes removed by this commit; null when no current snapshot. */
  Long deletedSizeBytes;

  /** Table properties at commit time (includes maintenance opt-in flags). */
  Map<String, String> tableProperties;
}
