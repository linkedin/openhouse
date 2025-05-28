package com.linkedin.openhouse.common;

/**
 * Class to represent a job state. This is decoupled from engine specific state, and there is a
 * mapping being done for each engine.
 */
public enum JobState {
  SUBMITTED,
  QUEUED,
  RUNNING,
  CANCELLED,
  FAILED,
  SUCCEEDED,
  SKIPPED;

  public boolean isTerminal() {
    return this.equals(SUCCEEDED) || this.equals(FAILED) || this.equals(CANCELLED);
  }
}
