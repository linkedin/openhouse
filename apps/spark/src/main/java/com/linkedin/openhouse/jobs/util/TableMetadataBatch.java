package com.linkedin.openhouse.jobs.util;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * A group of {@link TableMetadata} that share a database and will be processed in a single batched
 * Spark job (e.g. {@code BatchedOrphanFilesDeletionSparkApp}).
 *
 * <p>By design the scheduler never crosses database boundaries when bin-packing — every table in
 * {@link #tables} has the same {@link #dbName}. The {@link
 * com.linkedin.openhouse.optimizer.binpack.FirstFitDecreasingBinPacker} is invoked per-database;
 * each emitted bin becomes one {@code TableMetadataBatch}.
 */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TableMetadataBatch extends Metadata {
  @NonNull protected String dbName;
  @NonNull protected List<TableMetadata> tables;

  /**
   * Identifier used in metrics and the Jobs Service {@code jobName} — combines the database with
   * the bin size so logs/dashboards distinguish bins of different fan-outs without exposing every
   * fqtn.
   */
  @Override
  public String getEntityName() {
    return String.format("%s[%d]", dbName, tables.size());
  }

  /** Unmodifiable view of the underlying tables list. */
  public List<TableMetadata> getTables() {
    return Collections.unmodifiableList(tables);
  }
}
