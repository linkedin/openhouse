package com.linkedin.openhouse.internal.catalog;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Best-effort {@link PostCommitOperation} that extracts {@link CommitStats} from a successful
 * commit and publishes them, gated per table by a user-settable table property.
 *
 * <p>OSS provides only this abstract base (it is not a Spring bean), so nothing is registered or
 * runs in OSS/dev. A deployment supplies a concrete subclass annotated {@code @Component} that
 * implements {@link #publish(CommitStats)} with its transport; {@link PostCommitOperationRunner}
 * then auto-collects it and runs it within the framework's bounded, async, timeout-guarded
 * envelope.
 *
 * <p><b>Two-level enablement.</b> A publish happens only when both are true:
 *
 * <ul>
 *   <li><b>Global runner switch</b> {@code cluster.tables.postcommit.enabled} (owns the seam), and
 *   <li><b>Per-table opt-in</b> table property {@value #COMMIT_STATS_COLLECTION_ENABLED_PROP}{@code
 *       =true}. This is a plain user property (no reserved {@code openhouse.} prefix) so table
 *       owners can set it via {@code ALTER TABLE ... SET TBLPROPERTIES}; reserved {@code
 *       openhouse.} keys cannot be modified by users and would make the gate un-settable.
 * </ul>
 *
 * <p><b>Drops.</b> A {@link CommitType#DROP} carries no metadata, so there are no stats and the
 * per-table property can't be read. It is routed to {@link #publishDrop} instead so the publisher
 * can purge whatever it holds for the table. That purge is not gated by the per-table property: it
 * must also clean up tables that opted out after publishing, and it is idempotent, so it is a no-op
 * for tables that never published.
 */
@Slf4j
public abstract class AbstractCommitStatsPublishOperation implements PostCommitOperation {

  /**
   * User-settable table property that opts a table into per-commit stats collection. Deliberately
   * un-prefixed (not {@code openhouse.}) so it is not treated as a reserved/preserved key.
   */
  public static final String COMMIT_STATS_COLLECTION_ENABLED_PROP =
      "optimizer.commitStatsCollectionEnabled";

  private static final String OPERATION_NAME = "commit-stats-publish";

  @Override
  public String getName() {
    return OPERATION_NAME;
  }

  @Override
  public void execute(PostCommitContext context) throws Exception {
    if (context.getCommitType() == CommitType.DROP) {
      if (context.getTableUuid() != null && !context.getTableUuid().isEmpty()) {
        publishDrop(context.getTableUuid(), context.getTableIdentifier());
      }
      return;
    }
    TableMetadata committedMetadata = context.getCommittedMetadata();
    if (committedMetadata == null || !isCollectionEnabled(committedMetadata)) {
      return;
    }
    Optional<CommitStats> stats =
        CommitStatsFactory.extract(
            context.getTableIdentifier(), committedMetadata, context.getCommitType());
    if (stats.isPresent()) {
      publish(stats.get());
    }
  }

  private boolean isCollectionEnabled(TableMetadata committedMetadata) {
    return Boolean.parseBoolean(
        committedMetadata.properties().getOrDefault(COMMIT_STATS_COLLECTION_ENABLED_PROP, "false"));
  }

  /**
   * Publish the extracted stats. Implementations should be non-blocking/async and must tolerate
   * interruption, since the runner enforces a hard per-operation timeout.
   *
   * @param stats stats extracted from the successful commit
   * @throws Exception any failure; recorded by the runner as a failed operation, never propagated
   *     to the committer
   */
  protected abstract void publish(CommitStats stats) throws Exception;

  /**
   * Called when the table is dropped, so the publisher can purge any state it published for it.
   * Default is a no-op; a deployment overrides it with its transport. Same contract as {@link
   * #publish}: must tolerate interruption, and failures are recorded by the runner, never
   * propagated to the caller that dropped the table.
   *
   * @param tableUuid stable UUID of the dropped table (non-empty)
   * @param tableIdentifier the dropped table
   * @throws Exception any failure
   */
  protected void publishDrop(String tableUuid, TableIdentifier tableIdentifier) throws Exception {
    // no-op by default
  }
}
