package com.linkedin.openhouse.internal.catalog;

import lombok.Value;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Immutable context handed to each {@link PostCommitOperation} after a successful commit.
 *
 * <p>Carries the just-committed table identity and metadata (which includes the current snapshot
 * and its summary). Operations extract whatever they need; the framework stays agnostic to the
 * business logic.
 */
@Value
public class PostCommitContext {

  /** The committed table. Its namespace is the database. */
  TableIdentifier tableIdentifier;

  /** The table metadata as committed. */
  TableMetadata committedMetadata;

  /**
   * What kind of commit this was ({@link CommitType#CREATE}, {@link CommitType#UPDATE}, or {@link
   * CommitType#RENAME}). Derived by the committer, which has the signals (prior metadata, identity
   * changes) in-process; operations use it instead of re-deriving. Never null.
   */
  CommitType commitType;
}
