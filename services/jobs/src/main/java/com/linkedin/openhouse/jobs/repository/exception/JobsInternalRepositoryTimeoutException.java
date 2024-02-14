package com.linkedin.openhouse.jobs.repository.exception;

/** Exception thrown when contacting HTS timed out. */
public class JobsInternalRepositoryTimeoutException extends RuntimeException {
  public JobsInternalRepositoryTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
