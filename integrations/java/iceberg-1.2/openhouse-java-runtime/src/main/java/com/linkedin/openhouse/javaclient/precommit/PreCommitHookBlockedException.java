package com.linkedin.openhouse.javaclient.precommit;

import java.util.Objects;

/**
 * Thrown by a {@link PreCommitHook} to stop a commit.
 *
 * <p>Checked on purpose, so a hook cannot block a commit by accident. The only way to stop one is
 * to say so in the signature.
 *
 * <p>Use {@link #validation} when the commit itself is the problem and retrying would fail the same
 * way. Use {@link #operational} when the check could not reach a verdict, which is worth retrying.
 * The caller uses this to decide whether to retry; see {@link
 * com.linkedin.openhouse.javaclient.OpenHouseTableOperations}.
 */
public class PreCommitHookBlockedException extends Exception {

  /** Whether retrying the commit could produce a different answer. */
  public enum FailureType {
    /** The commit is the problem. Retrying will fail again. */
    VALIDATION,
    /** The check could not run. Retrying may succeed. */
    OPERATIONAL
  }

  private final String reasonCode;
  private final FailureType failureType;

  /** The commit is the problem. */
  public static PreCommitHookBlockedException validation(String reasonCode, String message) {
    return new PreCommitHookBlockedException(FailureType.VALIDATION, reasonCode, message, null);
  }

  /** The check could not reach a verdict. */
  public static PreCommitHookBlockedException operational(
      String reasonCode, String message, Throwable cause) {
    return new PreCommitHookBlockedException(FailureType.OPERATIONAL, reasonCode, message, cause);
  }

  private PreCommitHookBlockedException(
      FailureType failureType, String reasonCode, String message, Throwable cause) {
    super(message, cause);
    this.failureType = Objects.requireNonNull(failureType, "failureType");
    this.reasonCode = Objects.requireNonNull(reasonCode, "reasonCode");
  }

  /**
   * A short, stable code naming why the commit was stopped, used in logs and error messages.
   *
   * <p>Keep it low cardinality: a constant like {@code "row-count-check"}, never a value built from
   * the table or the commit, so it can safely be used as a metric tag. Same contract as {@link
   * PreCommitHook#getName()}.
   */
  public String getReasonCode() {
    return reasonCode;
  }

  public FailureType getFailureType() {
    return failureType;
  }
}
