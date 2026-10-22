package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.exception.CommitRejectedException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Runs {@link PreCommitCheck}s before a commit writes anything; the pre-commit counterpart of
 * {@link PostCommitOperationRunner}. It knows nothing about what any check enforces, and owns what
 * every check needs:
 *
 * <ul>
 *   <li><b>Order.</b> Checks run in {@code @Order}, on the committing thread. The first rejection
 *       stops the rest.
 *   <li><b>Rejections stay rejections.</b> A {@link CommitRejectedException} reaches the committer
 *       unchanged. A check that throws {@link CommitFailedException} or {@link
 *       CommitStateUnknownException} is a bug: Iceberg would retry a rejection that fails the same
 *       way every time, or report that a commit which wrote nothing may have succeeded. The runner
 *       reports it as an {@link IllegalStateException} instead.
 *   <li><b>Uniform observability.</b> Passed / rejected / error counters, tagged by check name.
 * </ul>
 *
 * <p>In OSS/dev deployments no {@link PreCommitCheck} beans exist, so {@link #checkAll} does
 * nothing, not even build the context.
 */
@Slf4j
@Component
public class PreCommitCheckRunner {

  static final String METRIC_PREFIX = "openhouse_precommit_check";
  static final String TAG_NAME = "name";
  static final String TAG_RESULT = "result";

  private final List<PreCommitCheck> checks;
  private final MeterRegistry meterRegistry;

  /** @param checks every {@link PreCommitCheck} bean, in {@code @Order}; empty when none */
  @Autowired
  public PreCommitCheckRunner(List<PreCommitCheck> checks, MeterRegistry meterRegistry) {
    this.checks =
        checks == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(checks));
    this.meterRegistry = meterRegistry;
    log.info(
        "PreCommitCheckRunner initialized with {} check(s): {}",
        this.checks.size(),
        this.checks.stream().map(PreCommitCheck::getName).collect(Collectors.joining(", ")));
  }

  /**
   * Runs every check against the commit doCommit is about to write.
   *
   * @param base the table's current metadata; {@code null} when the commit creates the table
   * @param metadata the metadata the commit proposes
   * @param properties the properties the commit leaves, still carrying doCommit's commit-only keys
   * @throws CommitRejectedException the first check's rejection, unchanged
   * @throws IllegalStateException if a check throws {@link CommitFailedException} or {@link
   *     CommitStateUnknownException}
   */
  public void checkAll(
      TableIdentifier tableIdentifier,
      TableMetadata base,
      TableMetadata metadata,
      Map<String, String> properties) {
    if (checks.isEmpty()) {
      return;
    }
    PendingCommit commit = PendingCommit.of(tableIdentifier, base, metadata, properties);
    for (PreCommitCheck check : checks) {
      run(check, commit);
    }
  }

  private void run(PreCommitCheck check, PendingCommit commit) {
    String name = check.getName();
    try {
      check.check(commit);
    } catch (CommitRejectedException rejection) {
      count(name, "rejected");
      throw rejection;
    } catch (CommitFailedException | CommitStateUnknownException misuse) {
      count(name, "error");
      throw new IllegalStateException(
          "Pre-commit check '"
              + name
              + "' threw "
              + misuse.getClass().getSimpleName()
              + "; checks reject with a CommitRejectedException",
          misuse);
    } catch (RuntimeException error) {
      count(name, "error");
      throw error;
    }
    count(name, "passed");
  }

  private void count(String name, String result) {
    try {
      meterRegistry.counter(METRIC_PREFIX, TAG_NAME, name, TAG_RESULT, result).increment();
    } catch (Exception e) {
      // Metrics must never decide a commit.
      log.warn("Failed to count pre-commit check {} as {}", name, result, e);
    }
  }
}
