package com.linkedin.openhouse.internal.catalog;

import lombok.Value;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Immutable context handed to each {@link PostCommitOperation} after a durable catalog change.
 *
 * <p>Usually an Iceberg commit: carries the just-committed table identity and metadata (which
 * includes the current snapshot and its summary). A table drop is also dispatched through this
 * context, as {@link CommitType#DROP}: a drop is a durable catalog change (the catalog entry is
 * removed) but not an Iceberg commit, so it carries the dropped table's identity and UUID and no
 * metadata. Operations extract whatever they need; the framework stays agnostic to the business
 * logic.
 */
@Value
public class PostCommitContext {

  /** The committed (or dropped) table. Its namespace is the database. */
  TableIdentifier tableIdentifier;

  /**
   * The table metadata as committed. {@code null} only for {@link CommitType#DROP}, which produces
   * no metadata; non-null for every other commit type.
   */
  TableMetadata committedMetadata;

  /**
   * What kind of catalog change this was. Derived by the committer (or by the drop path), which has
   * the signals in-process; operations use it instead of re-deriving. Never null.
   */
  CommitType commitType;

  /**
   * Stable table UUID ({@code openhouse.tableUUID}). Taken from the committed properties for a
   * commit, and from the catalog entry for a drop, where there is no metadata to read it from. May
   * be null if the table carries no UUID.
   */
  String tableUuid;
}
