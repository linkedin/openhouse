package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.internal.catalog.CommitPolicy;
import com.linkedin.openhouse.internal.catalog.TableCommit;
import com.linkedin.openhouse.internal.catalog.exception.CommitRejectedException;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import java.util.Objects;
import java.util.Optional;

/**
 * The internal catalog's {@link CommitPolicy}: the column-default rules every table commit must
 * pass. The catalog checks each commit after its stale-base check and before anything is written.
 * The request's {@link CatalogRequestContext} supplies the commit context: the bypass and the
 * handshake. The rules judge the commit's schema and snapshots against the table's current version.
 *
 * <ol>
 *   <li>A committed {@code read-bridge.column-default.enabled=true} cannot be changed or removed.
 *   <li>A commit whose request recorded the column-default handshake ({@link
 *       ReadBridgeStripProtection#prepare}) must pass {@link ReadBridgeStripProtection#enforce}.
 *       Commits without one, such as renames and internal writes, are not checked.
 * </ol>
 *
 * <p>A request with the column-default policy bypass ({@link
 * TablesMvcConstants#HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS}) skips both, so an operator can
 * repair a table whose stored default cannot be applied.
 */
public class ColumnDefaultCommitPolicy implements CommitPolicy {

  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;

  private final CatalogRequestContext requestContext;

  private final ReadBridgeStripProtection columnDefaultRules;

  public ColumnDefaultCommitPolicy(
      CatalogRequestContext requestContext, ReadBridgeStripProtection columnDefaultRules) {
    this.requestContext = Objects.requireNonNull(requestContext, "requestContext");
    this.columnDefaultRules = Objects.requireNonNull(columnDefaultRules, "columnDefaultRules");
  }

  /**
   * @throws CommitRejectedException with an {@link UnsupportedClientOperationException} if the
   *     commit changes or removes a committed opt-in, breaks Type 1 or Type 2, or declares a
   *     default that cannot be applied, or with a {@link TableConfigUnavailableException} if a
   *     default the table declares cannot be applied
   */
  @Override
  public void check(TableCommit commit) throws CommitRejectedException {
    if (requestContext.columnDefaultPolicyBypass()) {
      return;
    }
    keepOptIn(commit);
    Optional<String> handshake = requestContext.columnDefaultHandshake();
    if (!handshake.isPresent()) {
      return;
    }
    try {
      columnDefaultRules.enforce(commit, handshake.get());
    } catch (ColumnDefaultException e) {
      throw new CommitRejectedException(
          new UnsupportedClientOperationException(
              UnsupportedClientOperationException.Operation.COLUMN_DEFAULT, e.getMessage(), e));
    } catch (TableConfigUnavailableException e) {
      throw new CommitRejectedException(e);
    }
  }

  /**
   * Keeps {@link ReadBridgeConfigResolver#COLUMN_DEFAULT_ENABLED_PROP} once committed as {@code
   * true}: it cannot be changed or removed. {@code false}, an invalid value and an absent property
   * remain mutable so a table can still opt in later. Comparing with the commit's base keeps
   * transaction retries from overwriting a value committed concurrently.
   */
  private static void keepOptIn(TableCommit commit) throws CommitRejectedException {
    String committed = commit.getBase().map(base -> base.properties().get(ENABLED)).orElse(null);
    if (committed != null
        && "true".equalsIgnoreCase(committed.trim())
        && !committed.equals(commit.getProperties().get(ENABLED))) {
      throw new CommitRejectedException(
          new UnsupportedClientOperationException(
              UnsupportedClientOperationException.Operation.ALTER_RESERVED_TBLPROPS,
              String.format(
                  "Table property %s is immutable once set to true on table %s; cannot change or"
                      + " remove it without the column-default policy override.",
                  ENABLED, commit.getTableIdentifier())));
    }
  }
}
