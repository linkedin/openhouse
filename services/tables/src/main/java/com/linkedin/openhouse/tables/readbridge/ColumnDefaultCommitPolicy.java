package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.internal.catalog.PreCommitCheck;
import com.linkedin.openhouse.internal.catalog.PreCommitContext;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;

/**
 * The column-default rules every table commit must pass, as a {@link PreCommitCheck}: the catalog
 * runs it after its stale-base check and before anything is written. The rules judge the commit's
 * schema and snapshots against the table's current version.
 *
 * <ol>
 *   <li>A committed {@code read-bridge.column-default.enabled=true} cannot be changed or removed.
 *   <li>Every commit, renames included, must pass {@link ReadBridgeStripProtection#enforce}. Type 2
 *       counts the commit's client as default-aware when {@link ColumnDefaultClientGate} admits the
 *       request, so a snapshot commit outside a request cannot touch a table with column defaults.
 * </ol>
 *
 * <p>A request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}), read from its {@link
 * CatalogRequestContext}, skips both, so an operator can repair a table whose stored default cannot
 * be applied.
 */
public class ColumnDefaultCommitPolicy implements PreCommitCheck {

  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;

  private final CatalogRequestContext requestContext;

  private final ColumnDefaultClientGate clientGate;

  private final ReadBridgeStripProtection columnDefaultRules;

  public ColumnDefaultCommitPolicy(
      CatalogRequestContext requestContext,
      ColumnDefaultClientGate clientGate,
      ReadBridgeStripProtection columnDefaultRules) {
    this.requestContext = requestContext;
    this.clientGate = clientGate;
    this.columnDefaultRules = columnDefaultRules;
  }

  @Override
  public String getName() {
    return "column-default";
  }

  /**
   * @throws ColumnDefaultOptInLockedException if the commit changes or removes a committed opt-in
   * @throws ColumnDefaultRemovedException if the commit breaks Type 1
   * @throws ColumnDefaultUnawareCommitException if the commit breaks Type 2
   * @throws ColumnDefaultUnusableException if a default the commit declares cannot be applied
   * @throws TableConfigUnavailableException if a default the table declares cannot be applied
   */
  @Override
  public void check(PreCommitContext commit) {
    if (requestContext.columnDefaultPolicyBypass()) {
      return;
    }
    keepOptIn(commit);
    columnDefaultRules.enforce(commit, clientGate.admitsClient());
  }

  /**
   * Keeps {@link ReadBridgeConfigResolver#COLUMN_DEFAULT_ENABLED_PROP} once committed as {@code
   * true}: it cannot be changed or removed. {@code false}, an invalid value and an absent property
   * remain mutable so a table can still opt in later. Comparing with the commit's base keeps
   * transaction retries from overwriting a value committed concurrently.
   */
  private static void keepOptIn(PreCommitContext commit) {
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
