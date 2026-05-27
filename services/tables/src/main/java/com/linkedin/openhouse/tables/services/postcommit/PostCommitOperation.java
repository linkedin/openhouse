package com.linkedin.openhouse.tables.services.postcommit;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Optional;
import reactor.core.publisher.Mono;

/**
 * A best-effort, bounded, asynchronous action run by the Tables Service after a successful Iceberg
 * commit. Operations are invoked by {@link PostCommitDispatcher}, which owns the per-op timeout,
 * subscription, error swallowing, and metric emission. An error from prepare() is recorded and
 * dropped — it never affects commit correctness. Implementations may apply internal retries with
 * bounded budgets but must not subscribe themselves or apply an outer timeout.
 */
public interface PostCommitOperation {

  /** Short, stable identifier for this operation. Used as the {@code op} metric tag. */
  String name();

  /**
   * Build the work to run for {@code savedDto}, or {@link Optional#empty()} when the operation does
   * not apply (e.g. table not opted in, no committed snapshot). The dispatcher records empty as a
   * {@code skipped} metric.
   */
  Optional<Mono<Void>> prepare(TableDto savedDto);
}
