package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
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

  /** The canonical catalog record written by the same commit. */
  HouseTable houseTable;

  public PostCommitContext(TableIdentifier tableIdentifier, TableMetadata committedMetadata) {
    this(tableIdentifier, committedMetadata, null);
  }

  public PostCommitContext(
      TableIdentifier tableIdentifier, TableMetadata committedMetadata, HouseTable houseTable) {
    this.tableIdentifier = tableIdentifier;
    this.committedMetadata = committedMetadata;
    this.houseTable = houseTable;
  }
}
