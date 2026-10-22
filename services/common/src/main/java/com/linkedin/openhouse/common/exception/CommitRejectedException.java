package com.linkedin.openhouse.common.exception;

/**
 * A commit broke a rule the internal catalog checks before writing anything, such as a {@code
 * PreCommitCheck}. Nothing has been written. Mapped to 400.
 *
 * <p>Every rule rejects with this one type and a message that names the rule and what broke it;
 * nothing handles one rule's rejection differently from another's.
 *
 * <p>It is not a {@code CommitFailedException}: Iceberg retries that, and a rejected commit fails
 * the same way every time.
 */
public class CommitRejectedException extends RuntimeException {

  public CommitRejectedException(String message) {
    super(message);
  }

  public CommitRejectedException(String message, Throwable cause) {
    super(message, cause);
  }
}
