package com.linkedin.openhouse.internal.catalog;

/**
 * The kind of catalog change a successful commit represents, as seen by the post-commit path.
 *
 * <p>Derived in {@code OpenHouseInternalTableOperations.doCommit} from signals available in-process
 * at commit time — whether a base metadata existed, whether the committed properties changed the
 * table's identity, and whether the commit produced a new snapshot — and carried through {@link
 * PostCommitContext} and {@link CommitStats} so post-commit operations can react differently per
 * kind without re-deriving it. The chief intended consumer is the optimizer, which uses it to
 * decide what (if anything) a commit should trigger.
 *
 * <p>Note that table drops do NOT flow through this enum: a drop never produces a committed {@link
 * org.apache.iceberg.TableMetadata} and is dispatched on the separate {@link PostDropOperation}
 * seam instead.
 */
public enum CommitType {
  /**
   * First commit for a table: the base metadata was {@code null} (a create). May or may not carry
   * data (an empty create vs a CTAS); the {@link CommitStats} delta/snapshot distinguishes the two.
   */
  CREATE,

  /**
   * A commit to an existing table that produced a <b>new snapshot</b> — i.e. data changed (append,
   * overwrite, delete, replace). This is the signal that should trigger the full set of data-driven
   * analysis (orphan-file deletion, stats collection, future compaction).
   */
  DATA_UPDATE,

  /**
   * A commit to an existing table that produced <b>no new snapshot</b> — a metadata-only change
   * (table properties, policies, schema evolution) with the same current snapshot as before. Useful
   * to trigger only the operations that react to metadata/policy changes rather than data-driven
   * analysis — e.g. (re)setting up replication when the replication policy changes, or metadata
   * compaction — without running the data analyzers.
   */
  METADATA_UPDATE,

  /**
   * A commit that changed the table's database and/or table name (a rename). A special
   * metadata-only commit: no data changed; the stats row is re-keyed to the new identity by its
   * stable table UUID.
   */
  RENAME
}
