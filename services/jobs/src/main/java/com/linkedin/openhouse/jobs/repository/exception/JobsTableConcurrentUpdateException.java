package com.linkedin.openhouse.jobs.repository.exception;

public class JobsTableConcurrentUpdateException extends RuntimeException {
  public JobsTableConcurrentUpdateException(String message, Throwable cause) {
    super(message, cause);
  }
}
