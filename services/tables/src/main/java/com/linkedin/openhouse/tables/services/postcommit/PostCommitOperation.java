package com.linkedin.openhouse.tables.services.postcommit;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Optional;
import reactor.core.publisher.Mono;

// A best-effort, bounded, asynchronous action that the Tables Service runs after a successful
// Iceberg commit.
//
// Operations are invoked by PostCommitDispatcher, which owns the per-op timeout, the
// subscription, error swallowing, and metric emission. An error signaled from prepare() is
// recorded and dropped, so it never affects commit correctness.
//
// Implementations describe what to push and where. They may apply internal retries within a
// bounded budget. They must not subscribe themselves or apply an outer timeout, because the
// dispatcher already owns both concerns.
public interface PostCommitOperation {

  // Returns a short, stable identifier for this operation. Used as the "op" metric tag.
  String name();

  // Builds the work to run for savedDto, or returns Optional.empty() when the operation does not
  // apply to this commit (for example, when the table is not opted in or has no committed
  // snapshot). The dispatcher records an empty return as a "skipped" metric.
  Optional<Mono<Void>> prepare(TableDto savedDto);
}
