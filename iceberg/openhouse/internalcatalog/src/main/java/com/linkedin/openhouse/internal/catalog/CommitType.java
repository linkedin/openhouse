package com.linkedin.openhouse.internal.catalog;

/**
 * The kind of catalog change a successful commit represents, as seen by the post-commit path.
 *
 * <p>Derived in {@code OpenHouseInternalTableOperations.doCommit} from signals available in-process
 * at commit time (whether a base metadata existed, and whether the committed properties changed the
 * table's identity) and carried through {@link PostCommitContext} and {@link CommitStats} so
 * post-commit operations can react differently per kind without re-deriving it.
 *
 * <p>Note that table drops do NOT flow through this enum: a drop never produces a committed {@link
 * org.apache.iceberg.TableMetadata} and is dispatched on the separate {@link PostDropOperation}
 * seam instead.
 */
public enum CommitType {
  /** First commit for a table: the base metadata was {@code null} (a create). */
  CREATE,

  /**
   * A commit to an already-existing table that kept its identity — either a new snapshot (a data
   * change) or a metadata-only property update. The two are not distinguished here because they
   * share the same stats-publish handling.
   */
  UPDATE,

  /**
   * A commit that changed the table's database and/or table name (a rename). No data changed; the
   * stats row is re-keyed to the new identity by its stable table UUID.
   */
  RENAME
}
