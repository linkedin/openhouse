package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.internal.catalog.CommitPolicy;
import com.linkedin.openhouse.internal.catalog.TableCommit;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;

/**
 * The internal catalog's {@link CommitPolicy}: the column-default rules every table commit must
 * pass. The catalog checks each commit after its stale-base check and before anything is written.
 *
 * <p>A committed {@code read-bridge.column-default.enabled=true} cannot be changed or removed. A
 * request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}) skips this check.
 */
public class ColumnDefaultCommitPolicy implements CommitPolicy {

  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;

  private final CatalogRequestContext requestContext;

  public ColumnDefaultCommitPolicy(CatalogRequestContext requestContext) {
    this.requestContext = requestContext;
  }

  /**
   * @throws ColumnDefaultOptInLockedException if the commit changes or removes a committed opt-in
   */
  @Override
  public void check(TableCommit commit) {
    if (requestContext.columnDefaultPolicyBypass()) {
      return;
    }
    keepOptIn(commit);
  }

  /**
   * Keeps {@link ReadBridgeConfigResolver#COLUMN_DEFAULT_ENABLED_PROP} once committed as {@code
   * true}: it cannot be changed or removed. {@code false}, an invalid value and an absent property
   * remain mutable so a table can still opt in later. Comparing with the commit's base keeps
   * transaction retries from overwriting a value committed concurrently.
   */
  private static void keepOptIn(TableCommit commit) {
    String committed = commit.getBase().map(base -> base.properties().get(ENABLED)).orElse(null);
    if (committed != null
        && "true".equalsIgnoreCase(committed.trim())
        && !committed.equals(commit.getProperties().get(ENABLED))) {
      throw new ColumnDefaultOptInLockedException(
          String.format(
              "Table property %s is immutable once set to true on table %s; cannot change or"
                  + " remove it without the column-default policy override.",
              ENABLED, commit.getTableIdentifier()));
    }
  }
}
