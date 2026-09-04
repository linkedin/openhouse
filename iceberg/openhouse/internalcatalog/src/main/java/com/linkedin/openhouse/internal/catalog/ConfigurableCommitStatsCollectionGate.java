package com.linkedin.openhouse.internal.catalog;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Default {@link CommitStatsCollectionGate}: database-level enablement with a per-table override.
 *
 * <p>Enablement is the logical OR of:
 *
 * <ul>
 *   <li><b>Database filter</b> — the configured value {@code
 *       cluster.tables.optimizer.commit-stats-collection.database-filter} matched against the
 *       table's database name. The special values {@code *} or {@code all} (case-insensitive)
 *       enable every database; any other value is treated as a full-string regex (like the
 *       maintenance cron's {@code databaseFilter}). Blank/absent (the default) matches nothing, so
 *       collection is off until a database is onboarded.
 *   <li><b>Per-table override</b> — the table property {@value
 *       #COMMIT_STATS_COLLECTION_ENABLED_PROP} set to {@code "true"}, for enabling individual
 *       tables independent of the database filter.
 * </ul>
 *
 * <p>Enable all databases with {@code *} (or {@code all}); enable specific databases with a regex,
 * e.g. {@code (u_openhouse|db_foo)} — a single config change, matching the DB-by-DB onboarding
 * model.
 */
@Slf4j
@Component
public class ConfigurableCommitStatsCollectionGate implements CommitStatsCollectionGate {

  /** Per-table override property. */
  public static final String COMMIT_STATS_COLLECTION_ENABLED_PROP =
      "openhouse.optimizer.commitStatsCollectionEnabled";

  /** Whether the configured filter enables every database. */
  private final boolean matchAllDatabases;

  private final Pattern databasePattern;

  public ConfigurableCommitStatsCollectionGate(
      @Value("${cluster.tables.optimizer.commit-stats-collection.database-filter:}")
          String databaseFilter) {
    String normalized = databaseFilter == null ? "" : databaseFilter.trim();
    this.matchAllDatabases = normalized.equals("*") || normalized.equalsIgnoreCase("all");
    this.databasePattern = matchAllDatabases ? null : compileOrNull(normalized);
  }

  @Override
  public boolean isEnabled(TableIdentifier tableIdentifier, TableMetadata committedMetadata) {
    if (committedMetadata == null || tableIdentifier == null) {
      return false;
    }
    if (matchAllDatabases) {
      return true;
    }
    String databaseName = tableIdentifier.namespace().toString();
    if (databasePattern != null && databasePattern.matcher(databaseName).matches()) {
      return true;
    }
    return Boolean.parseBoolean(
        committedMetadata.properties().get(COMMIT_STATS_COLLECTION_ENABLED_PROP));
  }

  /** Returns a compiled pattern, or null when the filter is blank/invalid (i.e. no DB matches). */
  private static Pattern compileOrNull(String databaseFilter) {
    if (databaseFilter == null || databaseFilter.trim().isEmpty()) {
      return null;
    }
    try {
      return Pattern.compile(databaseFilter.trim());
    } catch (PatternSyntaxException e) {
      log.error(
          "Invalid commit-stats-collection database-filter regex '{}'; disabling database-level gating",
          databaseFilter,
          e);
      return null;
    }
  }
}
