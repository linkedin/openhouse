package com.linkedin.openhouse.common.exception;

/**
 * An exception indicating a conflict in Job state, e.g. job cancel request applied to a completed
 * job.
 */
public class JobStateConflictException extends RuntimeException {
  public JobStateConflictException(String currentState, String newState) {
    super(String.format("Cannot change state from %s to %s", currentState, newState));
  }
}
