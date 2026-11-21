package com.linkedin.openhouse.internal.catalog;

/**
 * The kind of durable catalog change a post-commit operation is reacting to.
 *
 * <p>For Iceberg commits it is derived in {@code OpenHouseInternalTableOperations.doCommit} from
 * signals available in-process at commit time — whether a base metadata existed, whether the
 * committed properties changed the table's identity, and whether the commit produced a new
 * snapshot. A table drop is not an Iceberg commit, but it is a durable catalog change, so {@code
 * OpenHouseInternalCatalog.dropTable} dispatches it through the same post-commit path as {@link
 * #DROP}. The type is carried through {@link PostCommitContext} and {@link CommitStats} so
 * post-commit operations can react differently per kind without re-deriving it. The chief intended
 * consumer is the optimizer, which uses it to decide what (if anything) a change should trigger.
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
   * A commit that changed only the table's catalog entry, not its data or Iceberg metadata — today,
   * a rename of the database and/or table name. The stats row is re-keyed to the new identity by
   * its stable table UUID.
   */
  CATALOG_UPDATE,

  /**
   * The table was dropped: its catalog entry was removed (and, with purge, its files deleted). Not
   * an Iceberg commit — there is no committed metadata, so {@link
   * PostCommitContext#getCommittedMetadata()} is {@code null} and the table is identified by {@link
   * PostCommitContext#getTableUuid()}. Lets operations clean up any state they hold for the table;
   * it never warrants analysis.
   */
  DROP
}
