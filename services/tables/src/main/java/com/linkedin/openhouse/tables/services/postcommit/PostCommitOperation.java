package com.linkedin.openhouse.tables.services.postcommit;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Optional;
import reactor.core.publisher.Mono;

/**
 * A best-effort, bounded, asynchronous action run by the Tables Service after a successful Iceberg
 * commit. Operations are invoked by {@link PostCommitDispatcher} and are subject to a single per-op
 * wall-clock timeout owned by the dispatcher. Operations never affect commit correctness: any error
 * they signal is recorded as a metric and a log line, then swallowed.
 *
 * <p>Implementations describe <em>what</em> to do; the dispatcher owns <em>how</em> it runs
 * (subscription, timeout, error handling, metric emission). Each operation contributes:
 *
 * <ul>
 *   <li>{@link #name()} — short identifier used as a metric tag.
 *   <li>{@link #prepare(TableDto)} — returns the work to perform, or {@link Optional#empty()} when
 *       the operation does not apply to this commit (e.g. table not opted in, no committed
 *       snapshot). Returning empty is a normal outcome; the dispatcher emits a {@code skipped}
 *       metric and moves on.
 * </ul>
 *
 * <p>The returned {@link Mono} must not subscribe itself, apply its own outer timeout, or swallow
 * errors — those concerns belong to the dispatcher. Implementations may apply internal retries with
 * bounded budgets.
 */
public interface PostCommitOperation {

  /** Short, stable identifier for this operation. Used as the {@code op} metric tag. */
  String name();

  /**
   * Build the work to run for {@code savedDto}, or return {@link Optional#empty()} if this
   * operation does not apply. The dispatcher only invokes the returned {@link Mono} after applying
   * its own timeout / error-swallow plumbing.
   */
  Optional<Mono<Void>> prepare(TableDto savedDto);
}
