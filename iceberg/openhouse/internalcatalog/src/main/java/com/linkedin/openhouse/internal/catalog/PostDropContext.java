package com.linkedin.openhouse.internal.catalog;

import lombok.Value;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Immutable context handed to each {@link PostDropOperation} after a table is successfully dropped.
 *
 * <p>A drop deletes the HouseTable row directly and never produces a committed {@link
 * org.apache.iceberg.TableMetadata}, so (unlike {@link PostCommitContext}) there is no metadata to
 * carry — only the dropped table's identity and its stable UUID. The UUID is the key downstream
 * systems (e.g. the optimizer) use to purge any per-table state they hold for the table.
 */
@Value
public class PostDropContext {

  /** The dropped table. Its namespace is the database. */
  TableIdentifier tableIdentifier;

  /**
   * Stable Iceberg table UUID of the dropped table (from {@code openhouse.tableUUID}). May be null
   * if the dropped HouseTable row carried no UUID; operations must tolerate that.
   */
  String tableUUID;
}
