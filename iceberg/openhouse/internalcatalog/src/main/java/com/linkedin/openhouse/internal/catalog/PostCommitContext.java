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
   * True when the table was temporary before this commit, so operations can act on the commit that
   * ends temporary status. That commit's metadata no longer carries the temporary flag.
   */
  boolean previouslyTemporary;
}
