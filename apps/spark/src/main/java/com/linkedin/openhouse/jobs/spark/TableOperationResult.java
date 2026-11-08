package com.linkedin.openhouse.jobs.spark;

import java.util.Optional;

/**
 * Outcome of processing a single table in a batched optimizer job: either a success, or a failure
 * carrying the failure reason ({@code ExceptionClass: message}). Modeling it as a success/failure
 * value keeps a failure reason from existing on a successful result — there is no nullable "reason"
 * field to misread — and gives both batched apps one place to derive and bound the reason.
 *
 * <p>The exception class gives a groupable failure type; the message adds detail. The reason is
 * truncated to {@link #MAX_FAILURE_REASON_LENGTH} to fit the optimizer {@code failure_reason}
 * column; the full stack trace still belongs in the executor logs.
 */
public final class TableOperationResult {

  static final int MAX_FAILURE_REASON_LENGTH = 1024;

  private final String failureReason;

  private TableOperationResult(String failureReason) {
    this.failureReason = failureReason;
  }

  public static TableOperationResult success() {
    return new TableOperationResult(null);
  }

  /**
   * A failure whose reason is {@code ExceptionClass: message} (e.g. {@code
   * java.lang.OutOfMemoryError: Java heap space}), bounded to {@link #MAX_FAILURE_REASON_LENGTH}.
   */
  public static TableOperationResult failure(Throwable t) {
    String message = t.getMessage();
    String reason = t.getClass().getName() + (message == null ? "" : ": " + message);
    if (reason.length() > MAX_FAILURE_REASON_LENGTH) {
      reason = reason.substring(0, MAX_FAILURE_REASON_LENGTH);
    }
    return new TableOperationResult(reason);
  }

  public boolean isSuccess() {
    return failureReason == null;
  }

  /** The failure reason ({@code ExceptionClass: message}), or empty for a success. */
  public Optional<String> failureReason() {
    return Optional.ofNullable(failureReason);
  }
}
