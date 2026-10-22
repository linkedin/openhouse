package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CommitRejectedException;
import com.linkedin.openhouse.internal.catalog.PreCommitCheck;
import com.linkedin.openhouse.internal.catalog.PreCommitContext;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;

/**
 * The column-default rules every table commit must pass, as a {@link PreCommitCheck}: the catalog
 * runs it after its stale-base check and before anything is written.
 *
 * <p>A committed {@code required-capability.column-defaults=true} cannot be changed or removed. A
 * request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}) skips this check.
 */
public class ColumnDefaultCommitPolicy implements PreCommitCheck {

  private static final String PROPERTY = ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP;

  private final CatalogRequestContext requestContext;

  public ColumnDefaultCommitPolicy(CatalogRequestContext requestContext) {
    this.requestContext = requestContext;
  }

  @Override
  public String getName() {
    return "column-default";
  }

  /** @throws CommitRejectedException if the commit changes or removes a committed opt-in */
  @Override
  public void check(PreCommitContext commit) {
    if (requestContext.columnDefaultPolicyBypass()) {
      return;
    }
    keepOptIn(commit);
  }

  /**
   * Keeps {@link ReadBridgeConfigResolver#COLUMN_DEFAULTS_CAPABILITY_PROP} once committed as {@code
   * true}: it cannot be changed or removed. {@code false}, an invalid value and an absent property
   * remain mutable so a table can still opt in later. Comparing with the commit's base keeps
   * transaction retries from overwriting a value committed concurrently.
   */
  private static void keepOptIn(PreCommitContext commit) {
    String committed = commit.getBase().map(base -> base.properties().get(PROPERTY)).orElse(null);
    if (committed != null
        && "true".equalsIgnoreCase(committed.trim())
        && !committed.equals(commit.getProperties().get(PROPERTY))) {
      throw new CommitRejectedException(
          String.format(
              "Table property %s is immutable once set to true on table %s; cannot change or"
                  + " remove it without the column-default policy override.",
              PROPERTY, commit.getTableIdentifier()));
    }
  }
}
