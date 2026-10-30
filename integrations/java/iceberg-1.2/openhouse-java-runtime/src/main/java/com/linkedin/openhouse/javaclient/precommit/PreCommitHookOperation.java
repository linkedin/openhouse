package com.linkedin.openhouse.javaclient.precommit;

import org.apache.iceberg.TableMetadata;

/**
 * What the commit is about to do, mirroring the branch {@code
 * com.linkedin.openhouse.javaclient.OpenHouseTableOperations#doCommit} takes.
 *
 * <p>There is no {@code CREATE} value because {@code doCommit} has no create branch -- creating a
 * table lands in {@link #SNAPSHOT} or {@link #METADATA} like any other commit. To spot a create,
 * check {@link PreCommitHookContext#getBase()} for an empty value.
 */
public enum PreCommitHookOperation {

  /** The table is being replaced wholesale, as in {@code REPLACE TABLE}. */
  REPLACE,

  /** Snapshots or refs changed. This is the usual write. */
  SNAPSHOT,

  /** Only table metadata changed, such as schema, partitioning, sort order or properties. */
  METADATA,

  /** Nothing changed, so nothing is sent. */
  NOOP;

  /**
   * Classifies a commit from the values {@code doCommit} has already worked out.
   *
   * <p>Takes the flags rather than recomputing them: {@code replaceTransaction} is not visible in
   * the metadata at all, and {@code metadataUpdated} comes from a private method. Recomputing here
   * would mean a second copy of that logic, free to drift from the real one.
   */
  static PreCommitHookOperation of(
      TableMetadata base,
      boolean replaceTransaction,
      boolean metadataUpdated,
      boolean snapshotsUpdated) {
    if (replaceTransaction && base != null) {
      return REPLACE;
    } else if (snapshotsUpdated) {
      return SNAPSHOT;
    } else if (metadataUpdated) {
      return METADATA;
    } else {
      return NOOP;
    }
  }
}
