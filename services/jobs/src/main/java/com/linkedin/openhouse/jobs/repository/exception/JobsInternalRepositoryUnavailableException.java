package com.linkedin.openhouse.jobs.repository.exception;

public class JobsInternalRepositoryUnavailableException extends RuntimeException {
  public JobsInternalRepositoryUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
