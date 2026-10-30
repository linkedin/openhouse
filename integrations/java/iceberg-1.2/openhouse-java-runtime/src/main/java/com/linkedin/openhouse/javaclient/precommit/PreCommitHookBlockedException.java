package com.linkedin.openhouse.javaclient.precommit;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Thrown by a {@link PreCommitHook} to stop a commit.
 *
 * <p>Checked on purpose, so a hook cannot block a commit by accident -- the only way to stop one is
 * to say so in the signature.
 *
 * <p>Use {@link #validation} when the commit itself is the problem and retrying would fail the same
 * way. Use {@link #operational} when the check could not reach a verdict, which is worth retrying.
 * The caller uses this to decide whether to retry; see {@link
 * com.linkedin.openhouse.javaclient.OpenHouseTableOperations}.
 */
public class PreCommitHookBlockedException extends Exception {

  /** Reason codes are used as metric tags, so keep them short and bounded. */
  private static final Pattern REASON_CODE_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,64}");

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
    this.reasonCode = requireValidReasonCode(reasonCode);
  }

  /** A short, stable code naming why the commit was stopped. Safe to use as a metric tag. */
  public String getReasonCode() {
    return reasonCode;
  }

  public FailureType getFailureType() {
    return failureType;
  }

  private static String requireValidReasonCode(String reasonCode) {
    Objects.requireNonNull(reasonCode, "reasonCode");
    if (!REASON_CODE_PATTERN.matcher(reasonCode).matches()) {
      throw new IllegalArgumentException(
          "reasonCode must match " + REASON_CODE_PATTERN.pattern() + " but was: " + reasonCode);
    }
    return reasonCode;
  }
}
