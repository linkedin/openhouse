package com.linkedin.openhouse.internal.catalog;

import java.util.Map;
import lombok.Builder;
import lombok.Value;

/**
 * Neutral, transport-agnostic snapshot of the stats derived from a single successful commit.
 *
 * <p>Produced by {@link CommitStatsFactory} from the committed {@link
 * org.apache.iceberg.TableMetadata} and handed to a commit-stats post-commit operation. Kept free
 * of any optimizer-client types so the internal catalog does not depend on the optimizer client;
 * the concrete publisher maps this to the optimizer stats API request.
 *
 * <p>The stats are split into two kinds:
 *
 * <ul>
 *   <li><b>Current state</b> ({@link #numCurrentFiles}, {@link #tableSizeBytes}) — point-in-time
 *       totals as of the committed snapshot.
 *   <li><b>Per-commit {@link Delta}</b> — what this specific commit added/removed.
 * </ul>
 *
 * <p>Both are populated only when the commit produced a new snapshot. For a commit that produced no
 * new snapshot (e.g. stage-create/replace, metadata-only, or replicated-table create), the current
 * totals are {@code null} and {@link #delta} is {@code null} (a properties-only publish). Note that
 * a metadata-only commit therefore reports the <i>same</i> current state as the prior commit;
 * consumers must treat a {@code null} delta as "unknown", not as zero — a naive counter that sums
 * deltas would be inaccurate across such commits.
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

  /**
   * What this commit changed. Null when the commit produced no new snapshot (metadata-only); a null
   * delta means "unknown", not zero.
   */
  Delta delta;

  /** Table properties at commit time (includes maintenance opt-in flags). */
  Map<String, String> tableProperties;

  /** Per-commit delta metrics, derived from the committed snapshot's summary. */
  @Value
  @Builder
  public static class Delta {

    /** Data files added by this commit. */
    Long numFilesAdded;

    /** Data files removed by this commit. */
    Long numFilesDeleted;

    /** Bytes added by this commit. */
    Long addedSizeBytes;

    /** Bytes removed by this commit. */
    Long deletedSizeBytes;
  }
}
