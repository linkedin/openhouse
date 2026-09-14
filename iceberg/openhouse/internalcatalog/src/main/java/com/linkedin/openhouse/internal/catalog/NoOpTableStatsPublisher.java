package com.linkedin.openhouse.internal.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.stereotype.Component;

/**
 * Default {@link TableStatsPublisher} that does nothing.
 *
 * <p>Keeps the commit-time stats hook inert until a concrete publisher (e.g. the LinkedIn optimizer
 * stats client) is configured as the primary bean. This ensures OSS and dev/docker environments are
 * unaffected by the hook.
 */
@Slf4j
@Component
public class NoOpTableStatsPublisher implements TableStatsPublisher {

  @Override
  public void publishOnCommit(TableIdentifier tableIdentifier, TableMetadata committedMetadata) {
    // Intentionally a no-op. A concrete @Primary publisher replaces this in production.
  }
}
