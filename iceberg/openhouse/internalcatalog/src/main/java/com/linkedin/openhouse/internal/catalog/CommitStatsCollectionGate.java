package com.linkedin.openhouse.internal.catalog;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Policy seam deciding whether commit-time stats collection is enabled for a table.
 *
 * <p>Evaluated by {@link OpenHouseInternalTableOperations} right after a successful commit, before
 * any stats are extracted or published. The default {@link ConfigurableCommitStatsCollectionGate}
 * enables collection at the database level via a configured regex (mirroring the maintenance cron's
 * {@code databaseFilter}), with an optional per-table property override. LinkedIn inter can provide
 * a {@code @Primary} implementation for per-database onboarding config.
 */
public interface CommitStatsCollectionGate {

  /**
   * @param tableIdentifier the committed table (its namespace is the database)
   * @param committedMetadata the committed table metadata (carries table properties)
   * @return true if stats should be collected and published for this commit
   */
  boolean isEnabled(TableIdentifier tableIdentifier, TableMetadata committedMetadata);
}
